"""路由分发领域服务：按规则将变更事件路由到下游消费通道。

上游：dedup_window(去重) → event_filter(过滤) → route_dispatcher(分发)
消费：下游Kafka/MQ消费端

路由不止决定「走主目标还是备用目标」，还要按接收方权限核对表归属：
每个目标对 (库, 表) 有独立的授权记录，只有核到授权的目标才进入
可投递目标数据；核不到或被撤权的目标把来源行回写到被拒绝行集合。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import ChangeEventRecord, pipeline_bus


class RouteDispatcherService(BaseModuleService):
    MODULE = "route_dispatcher"
    ENTITY = "路由规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "匹配条件", "目标通道", "分发策略", "是否广播", "权重", "备用目标"]
    REQUIRED_FIELDS = ["规则名称", "匹配条件", "目标通道"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已启用"}
    NEGATIVE_ACTIONS = []

    def route_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个事件执行路由分发。

        根据规则匹配条件和分发策略，将事件定向到目标通道。
        支持广播、加权分发和备通道降级；主目标在前，备用目标在后，
        由 prepare_delivery 统一核对每个目标的表归属授权。
        """
        condition = config.get("匹配条件", "")
        target = config.get("目标通道", "default_sink")
        strategy = config.get("分发策略", "round_robin")
        broadcast = config.get("是否广播", "否") == "是"
        weight = int(config.get("权重", "1"))
        backup = config.get("备用目标", "")

        event_str = str(event)
        if condition and condition not in event_str and condition not in event.get("来源表", ""):
            return {"routed": False, "reason": "condition_mismatch"}

        targets = [target]
        if backup:
            targets.append(backup)

        route_result: dict[str, Any] = {
            "routed": True,
            "event_id": event.get("事件ID", ""),
            "rule": config.get("规则名称"),
            "strategy": strategy,
            "broadcast": broadcast,
            "routed_at": datetime.now().isoformat(),
            "targets": targets,
        }
        if not broadcast:
            route_result["weight"] = weight
        if backup:
            route_result["backup_target"] = backup

        pipeline_bus.routing_table.append({
            "name": config.get("规则名称"),
            "condition": condition,
            "target": target,
            "backup_target": backup,
            "enabled": config.get("status") == "已启用",
        })

        pipeline_bus.pipeline_metrics["routed"] += len(targets)
        pipeline_bus.write_audit(
            "路由分发", target,
            f"事件 {event.get('事件ID','')} 路由到 {','.join(targets)}",
            "route_dispatcher", "已路由"
        )
        return route_result

    def prepare_delivery(
        self,
        event: ChangeEventRecord,
        targets: list[str],
        *,
        replay_id: str = "",
        unavailable_targets: set[str] | None = None,
    ) -> dict[str, Any]:
        """核对目标授权，把放行结果展开成可投递目标数据。

        表归属按接收方逐个核对：备用端只撤了 finance.orders 时，
        finance.orders 无法从备用端送出，sales.orders 仍独立可投递。
        不可用目标（例如主端超时）单独记为 target_unavailable。
        """
        unavailable = unavailable_targets or set()
        available = [t for t in targets if t not in unavailable]
        result = pipeline_bus.build_deliverable_targets(
            event, available, replay_id=replay_id
        )
        for target_id in targets:
            if target_id in unavailable:
                row = {
                    "event_id": event.event_id,
                    "source_db": event.source_db,
                    "source_table": event.source_table,
                    "target_id": target_id,
                    "grant_id": "",
                    "reason": f"{target_id} 未响应（主端超时），本轮不接收",
                    "stage": "target_unavailable",
                    "replay_id": replay_id,
                    "recorded_at": datetime.now().isoformat(),
                }
                result["rejected"].append(row)
                pipeline_bus.rejected_rows.append(row)
        pipeline_bus.deliverable_target_data.extend(result["deliverable"])
        pipeline_bus.rejected_rows.extend(
            row for row in result["rejected"] if row["stage"] != "target_unavailable"
        )
        if result["deliverable"]:
            pipeline_bus.write_audit(
                "可投递目标生成", result["event_id"],
                f"{event.source_db}.{event.source_table} 生成 "
                f"{len(result['deliverable'])} 条可投递目标数据",
                "route_dispatcher", "已生成"
            )
        for row in result["rejected"]:
            if row["stage"] == "target_unavailable":
                continue
            pipeline_bus.write_audit(
                "表归属核对", row["target_id"],
                f"{row['source_table']} 被拒：{row['reason']}",
                "route_dispatcher", "已拒绝", row["reason"],
            )
        return result

    def dispatch_entry(
        self,
        event: ChangeEventRecord,
        targets: list[str],
        *,
        replay_id: str = "",
        prepared: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """分发入口：实际使用目标集合并回写被拒绝的来源行。

        未传入 prepared 时会现场核对授权；传入时仍会在真正送出前
        逐个目标重新核对库表权限，备用发送前的撤权因此拦得住。
        """
        if prepared is None:
            prepared = self.prepare_delivery(event, targets, replay_id=replay_id)

        sent: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = list(prepared["rejected"])
        skipped: list[dict[str, Any]] = list(prepared["already_delivered"])
        for item in prepared["deliverable"]:
            if pipeline_bus.find_delivery_receipt(item["event_id"], item["target_id"]):
                # 已有回执：重发不重复交付同一行
                receipt = pipeline_bus.delivery_receipts[
                    pipeline_bus.compute_receipt_key(item["event_id"], item["target_id"])]
                skipped.append({
                    "event_id": item["event_id"],
                    "target_id": item["target_id"],
                    "receipt_key": receipt.receipt_key,
                    "receipt": pipeline_bus._receipt_view(receipt),
                })
                continue
            ok, grant, reason = pipeline_bus.verify_delivery_authorization(
                item["target_id"], item["source_db"], item["source_table"]
            )
            if not ok:
                # 本轮核对后、发送前又被撤权：同样回写为拒绝行
                row = {
                    "event_id": item["event_id"],
                    "source_db": item["source_db"],
                    "source_table": item["source_table"],
                    "target_id": item["target_id"],
                    "grant_id": grant.grant_id if grant else item.get("grant_id", ""),
                    "reason": reason,
                    "stage": "pre_send_recheck",
                    "replay_id": replay_id,
                    "recorded_at": datetime.now().isoformat(),
                }
                pipeline_bus.rejected_rows.append(row)
                rejected.append(row)
                pipeline_bus.write_audit(
                    "发送前复核", item["target_id"], reason,
                    "route_dispatcher", "已拒绝", reason,
                )
                continue
            receipt = pipeline_bus.commit_delivery(
                {**item, "grant_id": grant.grant_id},
                replayed=bool(replay_id),
            )
            item["status"] = "已送达"
            item["receipt"] = pipeline_bus._receipt_view(receipt)
            sent.append(item)

        return {
            "event_id": event.event_id,
            "source_row": f"{event.source_db}.{event.source_table}",
            "replay_id": replay_id,
            "sent": sent,
            "rejected": rejected,
            "already_delivered": skipped,
        }

    def register_target_grant(
        self,
        target_id: str,
        database: str,
        table: str,
        *,
        grant_id: str = "",
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """登记路由目标授权记录，形成可核对的表归属清单。"""
        grant = pipeline_bus.register_route_grant(
            target_id, database, table, grant_id=grant_id, channels=channels
        )
        pipeline_bus.write_audit(
            "授权登记", target_id,
            f"{target_id} 获得 {database}.{table} 的投递资格",
            "route_dispatcher", "已登记"
        )
        return self._grant_view(grant)

    def revoke_target_grant(
        self, target_id: str, database: str, table: str, reason: str = ""
    ) -> dict[str, Any]:
        """撤回目标对某张表的投递资格，其他表不受影响。"""
        revoked = pipeline_bus.revoke_route_grant(target_id, database, table, reason)
        pipeline_bus.write_audit(
            "授权撤回", target_id,
            f"{target_id} 撤销 {database}.{table} 的投递资格"
            + (f"：{reason}" if reason else ""),
            "route_dispatcher", "已撤回" if revoked else "无匹配授权"
        )
        return {
            "target_id": target_id,
            "database": database,
            "table": table,
            "revoked_grant_ids": revoked,
            "remaining_grants": self.list_target_grants(target_id),
        }

    def list_target_grants(self, target_id: str = "") -> list[dict[str, Any]]:
        """列出目标授权记录，可按目标过滤。"""
        return [
            self._grant_view(grant)
            for grant in pipeline_bus.route_grants.values()
            if not target_id or grant.target_id == target_id
        ]

    def delivery_status(self) -> dict[str, Any]:
        """可投递目标数据与被拒绝来源行的当前快照。"""
        return {
            "deliverable_targets": list(pipeline_bus.deliverable_target_data),
            "rejected_rows": list(pipeline_bus.rejected_rows),
            "receipts": [
                pipeline_bus._receipt_view(r)
                for r in pipeline_bus.delivery_receipts.values()
            ],
        }

    def replay_batch(
        self,
        events: list[ChangeEventRecord],
        targets: list[str],
        *,
        replay_id: str = "",
        primary_target: str = "",
        primary_timed_out: bool = False,
        revoke_before_send: list[tuple[str, str, str]] | None = None,
        revoke_before_dispatch: list[tuple[str, str, str]] | None = None,
    ) -> dict[str, Any]:
        """回放一批来源行：逐行核对授权、投递、回写拒绝与回执。

        - primary_timed_out：主目标本轮不接收，备用目标独立核对表归属。
        - revoke_before_send：在路由核对前撤回 (目标, 库, 表)，
          模拟「主端超时前撤回备用端某张表权限」。
        - revoke_before_dispatch：在生成可投递目标之后、实际送出之前撤回，
          验证备用发送前的二次核对能拦住撤权表。
        """
        rid = replay_id or f"replay-{len(pipeline_bus.replay_runs) + 1}"
        for target_id, database, table in revoke_before_send or []:
            self.revoke_target_grant(target_id, database, table, reason="回放前撤权")

        unavailable = {primary_target} if primary_timed_out and primary_target else set()
        run: dict[str, Any] = {
            "replay_id": rid,
            "targets": list(targets),
            "primary_target": primary_target,
            "primary_timed_out": bool(unavailable),
            "unavailable_targets": sorted(unavailable),
            "started_at": datetime.now().isoformat(),
            "finished_at": "",
            "members": [],
            "sent": [],
            "rejected": [],
            "already_delivered": [],
        }
        pipeline_bus.replay_runs[rid] = run

        for event in events:
            # 事件过滤把放行结果交给路由分发
            passed, matched = pipeline_bus.evaluate_filter(event)
            member = {
                "event_id": event.event_id,
                "source_row": f"{event.source_db}.{event.source_table}",
                "source_db": event.source_db,
                "source_table": event.source_table,
                "filter_passed": passed,
                "filter_rules": matched,
            }
            if not passed:
                row = {
                    "event_id": event.event_id,
                    "source_db": event.source_db,
                    "source_table": event.source_table,
                    "target_id": "",
                    "grant_id": "",
                    "reason": "被过滤规则拦截",
                    "stage": "event_filter",
                    "replay_id": rid,
                    "recorded_at": datetime.now().isoformat(),
                }
                pipeline_bus.rejected_rows.append(row)
                member["outcome"] = "已过滤"
                member["sent_targets"] = []
                member["rejected"] = [row]
                member["already_delivered"] = []
                run["members"].append(member)
                run["rejected"].append(row)
                continue

            routed = self.route_event(
                {"事件ID": event.event_id, "来源表": f"{event.source_db}.{event.source_table}"},
                {
                    "规则名称": f"回放路由-{rid}",
                    "匹配条件": "",
                    "目标通道": targets[0] if targets else "",
                    "备用目标": targets[1] if len(targets) > 1 else "",
                    "分发策略": "replay",
                    "是否广播": "否",
                    "权重": "1",
                    "status": "已启用",
                },
            )
            target_list = routed.get("targets", [])

            # 第一步：按接收方核对表归属，生成可投递目标数据
            prepared = self.prepare_delivery(
                event, target_list, replay_id=rid, unavailable_targets=unavailable
            )
            member["prepared_targets"] = [i["target_id"] for i in prepared["deliverable"]]
            member["prepared_rejected"] = [
                {"target_id": r["target_id"], "reason": r["reason"], "stage": r["stage"]}
                for r in prepared["rejected"]
            ]

            # 第二步：备用发送前重新核对库表权限
            for target_id, database, table in revoke_before_dispatch or []:
                if (database, table) == (event.source_db, event.source_table):
                    self.revoke_target_grant(
                        target_id, database, table, reason="发送前撤权")
            dispatched = self.dispatch_entry(
                event, target_list, replay_id=rid, prepared=prepared
            )
            member["already_delivered"] = dispatched["already_delivered"]
            member["sent_targets"] = [i["target_id"] for i in dispatched["sent"]]
            member["sent"] = dispatched["sent"]
            member["rejected"] = dispatched["rejected"]
            member["outcome"] = (
                "已送达" if dispatched["sent"]
                else ("已送达(跳过)" if dispatched["already_delivered"] else "已拒绝")
            )
            run["members"].append(member)
            run["sent"].extend(dispatched["sent"])
            run["rejected"].extend(dispatched["rejected"])
            run["already_delivered"].extend(dispatched["already_delivered"])

        run["finished_at"] = datetime.now().isoformat()
        run["summary"] = self._summarize_replay(run)
        pipeline_bus.write_audit(
            "批量回放", rid,
            f"回放 {len(events)} 行到 {len(targets)} 个目标，送达 {len(run['sent'])} 条",
            "route_dispatcher", "已完成"
        )
        return run

    def replay_report(self, replay_id: str) -> dict[str, Any] | None:
        """整轮回放与单表资格的冲突说明：哪部分数据实际抵达目标。"""
        run = pipeline_bus.replay_runs.get(replay_id)
        if run is None:
            return None
        by_table: dict[str, dict[str, Any]] = {}
        for member in run["members"]:
            row = member["source_row"]
            bucket = by_table.setdefault(row, {
                "source_row": row,
                "members": 0,
                "delivered_targets": [],
                "rejected_targets": [],
                "rejected_reasons": [],
                "already_delivered": [],
            })
            bucket["members"] += 1
            for target in member.get("sent_targets", []):
                if target not in bucket["delivered_targets"]:
                    bucket["delivered_targets"].append(target)
            for item in member.get("rejected", []):
                if not item["target_id"]:
                    continue
                if item["target_id"] not in bucket["rejected_targets"]:
                    bucket["rejected_targets"].append(item["target_id"])
                reason = f"{item['target_id']}:{item['reason']}"
                if reason not in bucket["rejected_reasons"]:
                    bucket["rejected_reasons"].append(reason)
            for item in member.get("already_delivered", []):
                if item["target_id"] not in bucket["already_delivered"]:
                    bucket["already_delivered"].append(item["target_id"])
        return {
            "replay_id": replay_id,
            "targets": run["targets"],
            "primary_target": run.get("primary_target", ""),
            "primary_timed_out": run.get("primary_timed_out", False),
            "unavailable_targets": run.get("unavailable_targets", []),
            "summary": run["summary"],
            "by_table": by_table,
            "members": run["members"],
            "receipts": [
                pipeline_bus._receipt_view(r)
                for r in pipeline_bus.delivery_receipts.values()
            ],
        }

    def _summarize_replay(self, run: dict[str, Any]) -> dict[str, Any]:
        sent_rows = {(i["event_id"], i["target_id"]) for i in run["sent"]}
        authorization_rejects = {
            (i["event_id"], i["target_id"], i["reason"])
            for i in run["rejected"]
            if i["stage"] in ("route_authorization", "pre_send_recheck")
        }
        unavailable = {
            (i["event_id"], i["target_id"])
            for i in run["rejected"]
            if i["stage"] == "target_unavailable"
        }
        filtered = sum(1 for m in run["members"] if m.get("outcome") == "已过滤")
        return {
            "members": len(run["members"]),
            "delivered": len(sent_rows),
            "rejected_authorization": len(authorization_rejects),
            "target_unavailable": len(unavailable),
            "filtered": filtered,
            "rejected": len({(i["event_id"], i["target_id"], i["reason"])
                             for i in run["rejected"]}),
            "skipped_already_delivered": len({
                (i["event_id"], i["target_id"]) for i in run["already_delivered"]}),
            "tables": sorted({m["source_row"] for m in run["members"]}),
            "targets": list(run["targets"]),
        }

    def _grant_view(self, grant) -> dict[str, Any]:
        return {
            "grant_id": grant.grant_id,
            "target_id": grant.target_id,
            "database": grant.database,
            "table": grant.table,
            "qualified_table": grant.qualified_table,
            "enabled": grant.enabled,
            "channels": list(grant.channels),
            "revoked_at": grant.revoked_at.isoformat() if grant.revoked_at else "",
            "reason": grant.reason,
        }

    def batch_route(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量路由事件。

        按策略对事件流执行批量分发，支持负载均衡模式。
        这里只做目标选择，实际投递前仍由 prepare_delivery/dispatch_entry
        按接收方授权核对表归属。
        """
        strategy = config.get("分发策略", "round_robin")
        targets = config.get("目标通道", "default").split(",")
        weight = int(config.get("权重", "1"))
        broadcast = config.get("是否广播", "否") == "是"

        by_target: dict[str, int] = {}
        for i, event in enumerate(events):
            if broadcast:
                selected = targets
            else:
                idx = (i // weight) % len(targets)
                selected = [targets[idx]]
            for target in selected:
                by_target[target] = by_target.get(target, 0) + 1

        pipeline_bus.write_audit(
            "批量路由", f"{len(events)}条事件",
            f"批量路由 {len(events)} 条事件到 {len(by_target)} 个目标",
            "route_dispatcher", "已路由"
        )
        return {
            "total_routed": len(events),
            "by_target": by_target,
            "strategy": strategy,
            "broadcast": broadcast,
        }

    def compute_route_distribution(self, configs: list[dict[str, Any]]) -> dict[str, Any]:
        """计算路由分发比例的预期分布。"""
        total_weight = sum(int(c.get("权重", "1")) for c in configs if c.get("status") == "已启用")
        dist: dict[str, Any] = {}
        for c in configs:
            if c.get("status") == "已启用":
                w = int(c.get("权重", "1"))
                name = c.get("规则名称", "")
                dist[name] = {
                    "target": c.get("目标通道"),
                    "weight": w,
                    "share_pct": round(w / max(total_weight, 1) * 100, 1),
                }
        return {"total_weight": total_weight, "distribution": dist}

    def find_fallback_route(self, event: dict[str, Any], primary_target: str) -> dict[str, Any]:
        """查找备选路由：备用目标必须持有该来源表的授权才可接。"""
        source_row = str(event.get("来源表", ""))
        database, _, table = source_row.partition(".")
        for rule in pipeline_bus.routing_table:
            candidate = rule.get("backup_target") or rule.get("target")
            if not candidate or candidate == primary_target:
                continue
            if not rule.get("enabled"):
                continue
            if database and table:
                ok, grant, reason = pipeline_bus.verify_delivery_authorization(
                    candidate, database, table
                )
                if not ok:
                    continue
            else:
                grant, reason = None, "缺少来源表，无法核对表归属"
            return {
                "found": True,
                "fallback": candidate,
                "condition": rule.get("condition"),
                "grant_id": grant.grant_id if grant else "",
                "reason": reason,
            }
        return {"found": False, "reason": "没有持有该表授权的备用目标"}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            target = entry.get("目标通道", "")
            pipeline_bus.routing_table.append({
                "name": entry.get("规则名称"),
                "condition": entry.get("匹配条件"),
                "target": target,
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "路由启用", target,
                f"路由规则 {entry.get('规则名称')} 已启用，指向 {target}",
                "route_dispatcher", "已启用"
            )
        return entry, msg