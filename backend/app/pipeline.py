"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str


@dataclass
class RouteTargetGrant:
    """路由目标授权记录：某个下游目标对一组库表的投递资格。

    一条授权只覆盖单张表 (target_id + database + table)，
    因此同一目标可以只对部分表保持资格，其余表独立失效。
    """
    grant_id: str
    target_id: str
    database: str
    table: str
    enabled: bool = True
    revoked_at: datetime | None = None
    reason: str = ""
    # 该授权覆盖的通道，空表示目标本身即可投递
    channels: list[str] = field(default_factory=list)

    @property
    def qualified_table(self) -> str:
        return f"{self.database}.{self.table}"


@dataclass
class DeliveryReceipt:
    """投递回执：记录源行实际抵达的目标，重发时凭回执跳过已交付行。"""
    receipt_key: str
    event_id: str
    source_db: str
    source_table: str
    target_id: str
    grant_id: str
    delivered_at: datetime
    payload_digest: str
    replayed: bool = False


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 路由目标授权 — 按 (目标, 库, 表) 粒度保存投递资格
        self.route_grants: dict[str, RouteTargetGrant] = {}
        # 投递回执 — 已确认送达的源行凭回执去重
        self.delivery_receipts: dict[str, DeliveryReceipt] = {}
        # 可投递目标数据 — 路由按记录资格生成的待投递结果
        self.deliverable_target_data: list[dict[str, Any]] = []
        # 被拒绝的来源行 — 过滤/路由/授权校验不通过时回写
        self.rejected_rows: list[dict[str, Any]] = []
        # 回放运行 — key 为 replay_id，保存批次成员、已送达与待回放状态
        self.replay_runs: dict[str, dict[str, Any]] = {}
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    def register_position(self, pos: PositionSnapshot) -> None:
        """注册存储位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。"""
        return self.positions.get(instance)

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用" and not rule.get("enabled"):
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    # ------------------------------------------------------------------
    # 路由目标授权：按接收方权限核对表归属
    # ------------------------------------------------------------------
    def register_route_grant(
        self,
        target_id: str,
        database: str,
        table: str,
        *,
        grant_id: str = "",
        enabled: bool = True,
        channels: list[str] | None = None,
        reason: str = "",
    ) -> RouteTargetGrant:
        """登记一条目标授权记录，归属粒度为 目标 + 库 + 表。"""
        gid = grant_id or f"grant:{target_id}:{database}.{table}"
        grant = RouteTargetGrant(
            grant_id=gid,
            target_id=target_id,
            database=database,
            table=table,
            enabled=enabled,
            revoked_at=None if enabled else datetime.now(),
            reason=reason,
            channels=list(channels or []),
        )
        self.route_grants[gid] = grant
        return grant

    def find_route_grant(self, target_id: str, database: str, table: str) -> RouteTargetGrant | None:
        """按接收方、库、表三要素查找仍在生效的授权记录。"""
        for grant in self.route_grants.values():
            if (
                grant.target_id == target_id
                and grant.database == database
                and grant.table == table
                and grant.enabled
            ):
                return grant
        return None

    def revoke_route_grant(self, target_id: str, database: str, table: str,
                           reason: str = "") -> list[str]:
        """撤回某目标对指定表的投递资格，返回被撤回的授权ID列表。

        只撤回匹配该库表的授权，同目标下其他表的资格保持独立。
        """
        revoked: list[str] = []
        for grant in self.route_grants.values():
            if (
                grant.target_id == target_id
                and grant.database == database
                and grant.table == table
                and grant.enabled
            ):
                grant.enabled = False
                grant.revoked_at = datetime.now()
                grant.reason = reason or grant.reason
                revoked.append(grant.grant_id)
        return revoked

    def verify_delivery_authorization(
        self, target_id: str, database: str, table: str
    ) -> tuple[bool, RouteTargetGrant | None, str]:
        """发送前重新核对库表权限：返回 (是否可发, 授权, 拒绝原因)。"""
        grant = self.find_route_grant(target_id, database, table)
        if grant is not None:
            return True, grant, ""
        known = [
            g for g in self.route_grants.values()
            if g.target_id == target_id and g.database == database and g.table == table
        ]
        if known:
            return False, known[-1], f"{target_id} 对 {database}.{table} 的授权已撤回"
        return False, None, f"{target_id} 未登记 {database}.{table} 的表归属授权"

    def compute_receipt_key(self, event_id: str, target_id: str) -> str:
        """回执键：同一源行投递到同一目标只应有一条回执。"""
        return f"{target_id}::{event_id}"

    def find_delivery_receipt(self, event_id: str, target_id: str) -> DeliveryReceipt | None:
        """查询源行是否已有该目标的送达回执。"""
        return self.delivery_receipts.get(self.compute_receipt_key(event_id, target_id))

    def build_deliverable_targets(
        self,
        event: ChangeEventRecord,
        targets: list[str],
        *,
        replay_id: str = "",
    ) -> dict[str, Any]:
        """按目标授权把一次放行结果展开成可投递目标数据。

        每个目标单独核对表归属：某个目标失去某张表的资格，
        不影响其他目标或其他表；已送达且持有回执的行不会再次生成可投递项。
        """
        deliverable: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        already_delivered: list[dict[str, Any]] = []
        seen_targets: set[str] = set()
        for target_id in targets:
            if target_id in seen_targets:
                continue
            seen_targets.add(target_id)
            # 先看回执：已经送达的行保留原回执与原授权，
            # 即使之后表归属被撤权也不改判为「未授权」，只按回执跳过重发。
            receipt = self.find_delivery_receipt(event.event_id, target_id)
            if receipt is not None:
                already_delivered.append({
                    "event_id": event.event_id,
                    "target_id": target_id,
                    "receipt_key": receipt.receipt_key,
                    "receipt": self._receipt_view(receipt),
                })
                continue
            ok, grant, reason = self.verify_delivery_authorization(
                target_id, event.source_db, event.source_table
            )
            if not ok:
                rejected.append({
                    "event_id": event.event_id,
                    "source_db": event.source_db,
                    "source_table": event.source_table,
                    "target_id": target_id,
                    "grant_id": grant.grant_id if grant else "",
                    "reason": reason,
                    "stage": "route_authorization",
                    "replay_id": replay_id,
                    "recorded_at": datetime.now().isoformat(),
                })
                continue
            deliverable.append({
                "event_id": event.event_id,
                "source_db": event.source_db,
                "source_table": event.source_table,
                "source_row": f"{event.source_db}.{event.source_table}",
                "operation": event.operation,
                "target_id": target_id,
                "grant_id": grant.grant_id,
                "qualified_table": grant.qualified_table,
                "binlog_file": event.binlog_file,
                "binlog_pos": event.binlog_pos,
                "gtid": event.gtid,
                "replay_id": replay_id,
                "payload_digest": self.compute_event_hash(event),
                "status": "待投递",
            })
        return {
            "event_id": event.event_id,
            "deliverable": deliverable,
            "rejected": rejected,
            "already_delivered": already_delivered,
        }

    def commit_delivery(
        self, item: dict[str, Any], *, replayed: bool = False
    ) -> DeliveryReceipt:
        """记录一条可投递目标数据已实际交付，并固化原回执与授权。"""
        key = self.compute_receipt_key(item["event_id"], item["target_id"])
        existing = self.delivery_receipts.get(key)
        if existing is not None:
            # 已有回执时不改写送达时间和授权，重发不会重复交付同一行
            existing.replayed = existing.replayed or replayed
            return existing
        receipt = DeliveryReceipt(
            receipt_key=key,
            event_id=item["event_id"],
            source_db=item["source_db"],
            source_table=item["source_table"],
            target_id=item["target_id"],
            grant_id=item["grant_id"],
            delivered_at=datetime.now(),
            payload_digest=item.get("payload_digest", ""),
            replayed=replayed,
        )
        self.delivery_receipts[key] = receipt
        for entry in self.deliverable_target_data:
            if (
                entry["event_id"] == item["event_id"]
                and entry["target_id"] == item["target_id"]
            ):
                entry["status"] = "已送达"
                entry["receipt_key"] = key
        return receipt

    def _receipt_view(self, receipt: DeliveryReceipt) -> dict[str, Any]:
        """回执的可序列化视图，保留原授权信息。"""
        grant = self.route_grants.get(receipt.grant_id)
        return {
            "receipt_key": receipt.receipt_key,
            "event_id": receipt.event_id,
            "source_db": receipt.source_db,
            "source_table": receipt.source_table,
            "target_id": receipt.target_id,
            "grant_id": receipt.grant_id,
            "grant_qualified_table": grant.qualified_table if grant else
            f"{receipt.source_db}.{receipt.source_table}",
            "grant_revoked": bool(grant and not grant.enabled),
            "delivered_at": receipt.delivered_at.isoformat(),
            "payload_digest": receipt.payload_digest,
            "replayed": receipt.replayed,
        }

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
            "route_grant_count": len(self.route_grants),
            "active_route_grant_count": sum(1 for g in self.route_grants.values() if g.enabled),
            "delivery_receipt_count": len(self.delivery_receipts),
            "deliverable_target_count": len(self.deliverable_target_data),
            "rejected_row_count": len(self.rejected_rows),
            "replay_run_count": len(self.replay_runs),
        }


pipeline_bus = PipelineBus()