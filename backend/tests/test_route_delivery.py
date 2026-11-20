"""路由目标授权与批量回放的行为测试。

直接调用领域服务，不依赖测试框架：
    cd backend && .venv/bin/python tests/test_route_delivery.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import ChangeEventRecord, PipelineBus  # noqa: E402
from app.services import route_dispatcher as route_module  # noqa: E402
from app.services.route_dispatcher import RouteDispatcherService  # noqa: E402


def fresh_service() -> RouteDispatcherService:
    bus = PipelineBus()
    route_module.pipeline_bus = bus
    return RouteDispatcherService()


def make_event(event_id: str, db: str, table: str, pos: int = 1) -> ChangeEventRecord:
    return ChangeEventRecord(
        event_id=event_id,
        source_db=db,
        source_table=table,
        operation="INSERT",
        before_image=None,
        after_image={"id": 1},
        tx_id=f"tx-{event_id}",
        binlog_file="mysql-bin.000001",
        binlog_pos=pos,
        gtid=f"gtid-{event_id}",
        event_time=datetime.now(),
    )


def seed_grants(service: RouteDispatcherService, *targets: str) -> None:
    for target in targets:
        for db in ("finance", "sales"):
            service.register_target_grant(target, db, "orders")


def test_primary_timeout_revocation_keeps_independent_table() -> None:
    """主端超时后备用端只失去 finance.orders，sales.orders 仍独立送达。"""
    service = fresh_service()
    seed_grants(service, "primary_sink", "standby_sink")
    run = service.replay_batch(
        [
            make_event("fin-1", "finance", "orders", 100),
            make_event("sales-1", "sales", "orders", 200),
        ],
        ["primary_sink", "standby_sink"],
        replay_id="r1",
        primary_target="primary_sink",
        primary_timed_out=True,
        revoke_before_send=[("standby_sink", "finance", "orders")],
    )
    by_event = {m["event_id"]: m for m in run["members"]}
    assert by_event["fin-1"]["sent_targets"] == [], by_event["fin-1"]
    assert by_event["sales-1"]["sent_targets"] == ["standby_sink"], by_event["sales-1"]

    report = service.replay_report("r1")
    assert report["by_table"]["finance.orders"]["delivered_targets"] == []
    assert report["by_table"]["sales.orders"]["delivered_targets"] == ["standby_sink"]
    assert report["primary_timed_out"] is True
    assert report["summary"]["delivered"] == 1


def test_pre_send_recheck_blocks_revoked_table() -> None:
    """备用发送前重新核对权限：路由核对后撤掉 finance.orders 也送不出去。"""
    service = fresh_service()
    seed_grants(service, "primary_sink", "standby_sink")
    run = service.replay_batch(
        [make_event("fin-1", "finance", "orders", 300)],
        ["primary_sink", "standby_sink"],
        replay_id="r2",
        revoke_before_dispatch=[("standby_sink", "finance", "orders")],
    )
    member = run["members"][0]
    assert member["prepared_targets"] == ["primary_sink", "standby_sink"]
    assert member["sent_targets"] == ["primary_sink"], member
    recheck = [r for r in member["rejected"] if r["stage"] == "pre_send_recheck"]
    assert len(recheck) == 1 and recheck[0]["target_id"] == "standby_sink"
    assert service._grant_view  # 授权记录仍在，只是失效


def test_delivered_rows_keep_receipt_and_do_not_redeliver() -> None:
    """已确认送达的源行保留原回执与授权，重发不重复交付同一行。"""
    service = fresh_service()
    seed_grants(service, "standby_sink")
    rows = [make_event("sales-1", "sales", "orders", 500)]

    first = service.replay_batch(rows, ["standby_sink"], replay_id="r3")
    first_receipt = first["members"][0]["sent"][0]["receipt"]
    assert first_receipt["replayed"] is True

    second = service.replay_batch(rows, ["standby_sink"], replay_id="r4")
    member = second["members"][0]
    assert member["sent_targets"] == []
    assert member["outcome"] == "已送达(跳过)"
    assert [i["target_id"] for i in member["already_delivered"]] == ["standby_sink"]

    receipts = service.delivery_status()["receipts"]
    assert len(receipts) == 1
    assert receipts[0]["delivered_at"] == first_receipt["delivered_at"]
    assert receipts[0]["grant_id"] == "grant:standby_sink:sales.orders"
    assert receipts[0]["replayed"] is True


def test_revoked_grant_kept_on_existing_receipt() -> None:
    """撤权只影响后续投递，已送达行的原回执和授权保留。"""
    service = fresh_service()
    seed_grants(service, "standby_sink")
    service.replay_batch([make_event("fin-1", "finance", "orders")], ["standby_sink"], replay_id="r5")
    service.revoke_target_grant("standby_sink", "finance", "orders", reason="演练撤权")

    receipts = service.delivery_status()["receipts"]
    assert len(receipts) == 1
    assert receipts[0]["grant_revoked"] is True
    assert receipts[0]["grant_id"] == "grant:standby_sink:finance.orders"

    again = service.replay_batch(
        [make_event("fin-1", "finance", "orders")], ["standby_sink"], replay_id="r6")
    member = again["members"][0]
    assert member["sent_targets"] == []
    assert member["already_delivered"], member


def test_filter_rejection_written_back_as_source_row() -> None:
    """被事件过滤拦截的来源行回写为拒绝行，不进入路由。"""
    service = fresh_service()
    seed_grants(service, "standby_sink")
    route_module.pipeline_bus.filter_rules.append({
        "name": "拦截财务",
        "type": "黑名单",
        "expression": "finance",
        "is_whitelist": False,
        "enabled": True,
    })
    run = service.replay_batch(
        [make_event("fin-1", "finance", "orders")], ["standby_sink"], replay_id="r7")
    member = run["members"][0]
    assert member["outcome"] == "已过滤"
    assert member["rejected"][0]["stage"] == "event_filter"
    assert run["sent"] == []


def test_unauthorized_target_rejected_without_grant() -> None:
    """没有表归属授权的目标不能生成可投递目标数据。"""
    service = fresh_service()
    seed_grants(service, "primary_sink")
    run = service.replay_batch(
        [make_event("sales-1", "sales", "orders")],
        ["primary_sink", "standby_sink"],
        replay_id="r8",
        revoke_before_send=[("primary_sink", "sales", "orders")],
    )
    member = run["members"][0]
    assert member["sent_targets"] == []
    reasons = {r["target_id"]: r["reason"] for r in member["rejected"]}
    assert "未登记" in reasons["standby_sink"]
    assert "已撤回" in reasons["primary_sink"]


def test_grants_are_per_table() -> None:
    """同一目标的授权按表独立，撤回一张表不影响另一张。"""
    service = fresh_service()
    seed_grants(service, "standby_sink")
    result = service.revoke_target_grant("standby_sink", "finance", "orders", "演练")
    assert result["revoked_grant_ids"] == ["grant:standby_sink:finance.orders"]
    remaining = {g["qualified_table"] for g in result["remaining_grants"] if g["enabled"]}
    assert remaining == {"sales.orders"}


def main() -> None:
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    failures = 0
    for test in tests:
        try:
            test()
        except AssertionError as exc:
            failures += 1
            print(f"FAIL {test.__name__}: {exc}")
        else:
            print(f"ok   {test.__name__}")
    if failures:
        raise SystemExit(f"{failures} 个用例失败")
    print(f"全部 {len(tests)} 个用例通过")


if __name__ == "__main__":
    main()
