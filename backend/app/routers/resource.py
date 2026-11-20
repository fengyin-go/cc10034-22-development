"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.pipeline import ChangeEventRecord, pipeline_bus
from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


class GrantPayload(BaseModel):
    target_id: str
    database: str
    table: str
    grant_id: str | None = None
    channels: list[str] = Field(default_factory=list)


class RevokePayload(BaseModel):
    target_id: str
    database: str
    table: str
    reason: str = ""


class ReplayRow(BaseModel):
    event_id: str
    source_db: str
    source_table: str
    operation: str = "INSERT"
    before_image: dict[str, Any] | None = None
    after_image: dict[str, Any] | None = None
    tx_id: str = ""
    binlog_file: str = "mysql-bin.000001"
    binlog_pos: int = 0
    gtid: str = ""
    event_time: str = ""


def _to_event_record(row: ReplayRow) -> ChangeEventRecord:
    try:
        event_time = datetime.fromisoformat(row.event_time) if row.event_time else datetime.now()
    except ValueError:
        event_time = datetime.now()
    return ChangeEventRecord(
        event_id=row.event_id,
        source_db=row.source_db,
        source_table=row.source_table,
        operation=row.operation,
        before_image=row.before_image,
        after_image=row.after_image,
        tx_id=row.tx_id,
        binlog_file=row.binlog_file,
        binlog_pos=row.binlog_pos,
        gtid=row.gtid,
        event_time=event_time,
    )


def _require_route_service():
    service = require_service("route_dispatcher")
    return service


class RevokeOnReplay(BaseModel):
    target_id: str
    database: str
    table: str


class ReplayPayload(BaseModel):
    replay_id: str | None = None
    targets: list[str] = Field(default_factory=list)
    primary_target: str = ""
    primary_timed_out: bool = False
    rows: list[ReplayRow] = Field(default_factory=list)
    revoke_before_send: list[RevokeOnReplay] = Field(default_factory=list)
    revoke_before_dispatch: list[RevokeOnReplay] = Field(default_factory=list)


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/route-grants", response_model=dict)
def list_route_grants(module: str, target: str = Query(default="")) -> dict[str, Any]:
    """路由目标授权记录清单。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块没有路由目标授权")
    service = _require_route_service()
    grants = service.list_target_grants(target)
    return {"items": grants, "total": len(grants)}


@router.post("/{module}/route-grants", response_model=dict)
def create_route_grant(module: str, payload: GrantPayload) -> dict[str, Any]:
    """登记一条按接收方核对的表归属授权。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块没有路由目标授权")
    service = _require_route_service()
    return service.register_target_grant(
        payload.target_id, payload.database, payload.table,
        grant_id=payload.grant_id or "", channels=payload.channels,
    )


@router.post("/{module}/route-grants/revoke", response_model=dict)
def revoke_route_grant(module: str, payload: RevokePayload) -> dict[str, Any]:
    """撤回目标对某张表的投递资格，同目标其他表不受影响。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块没有路由目标授权")
    service = _require_route_service()
    return service.revoke_target_grant(
        payload.target_id, payload.database, payload.table, payload.reason)


@router.get("/{module}/delivery", response_model=dict)
def delivery_status(module: str) -> dict[str, Any]:
    """可投递目标数据、被拒绝来源行与投递回执的当前状态。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块没有投递状态")
    return _require_route_service().delivery_status()


@router.post("/{module}/replay", response_model=dict)
def replay_batch(module: str, payload: ReplayPayload) -> dict[str, Any]:
    """批量回放：过滤放行 → 路由核对授权 → 分发入口投递并回写拒绝行。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块不支持回放")
    if not payload.targets:
        raise HTTPException(status_code=400, detail="回放需要至少一个目标")
    service = _require_route_service()
    events = [_to_event_record(row) for row in payload.rows]
    return service.replay_batch(
        events,
        payload.targets,
        replay_id=payload.replay_id or "",
        primary_target=payload.primary_target,
        primary_timed_out=payload.primary_timed_out,
        revoke_before_send=[
            (r.target_id, r.database, r.table) for r in payload.revoke_before_send
        ],
        revoke_before_dispatch=[
            (r.target_id, r.database, r.table) for r in payload.revoke_before_dispatch
        ],
    )


@router.get("/{module}/replays/{replay_id}", response_model=dict)
def replay_report(module: str, replay_id: str) -> dict[str, Any]:
    """整轮回放报告：逐表说明哪部分数据实际抵达目标。"""
    if module != "route_dispatcher":
        raise HTTPException(status_code=404, detail="该模块不支持回放")
    report = _require_route_service().replay_report(replay_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"回放 {replay_id} 不存在")
    return report


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
