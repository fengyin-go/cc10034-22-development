# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 路由目标授权与批量回放

路由分发不只看「主目标还是备用目标」，还要按接收方权限核对表归属：

- 授权记录粒度为 `接收方目标 + 库 + 表`，登记/撤回互不影响其他表；
  种子数据里主备两个目标各持有 `finance.orders`、`sales.orders` 两条资格。
- 事件过滤的放行结果交给路由：路由逐目标核对授权，生成「可投递目标数据」；
  分发入口实际使用该目标集合，并把未获授权或主端不可用的来源行回写为拒绝行。
- 主目标超时只让它本轮不接收；已撤权的备用表（如 `finance.orders`）不能从备用端送出，
  同批次其他库表（如 `sales.orders`）仍独立可投递。
- 备用发送前会再核对一次库表权限，路由核对之后才撤的权同样拦得住。
- 每条实际送达都落投递回执；已送达的源行保留原回执与原授权，
  再次回放只按回执跳过，不会重复交付同一行。
- `GET /api/route_dispatcher/replays/{replay_id}` 按表汇总「实际抵达目标 / 被拒目标 / 回执跳过」，
  说明整轮回放与单表资格冲突时哪部分数据真正抵达。

相关接口（均挂在 `route_dispatcher` 模块下）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET/POST | `/api/route_dispatcher/route-grants` | 查询/登记表归属授权 |
| POST | `/api/route_dispatcher/route-grants/revoke` | 撤回某目标某张表的资格 |
| GET | `/api/route_dispatcher/delivery` | 可投递目标数据、拒绝行、回执 |
| POST | `/api/route_dispatcher/replay` | 发起批量回放 |
| GET | `/api/route_dispatcher/replays/{replay_id}` | 回放报告 |

前端在「路由分发」页下方提供授权清单、撤回按钮和回放表单。
后端行为用例：`cd backend && .venv/bin/python -I tests/test_route_delivery.py`。

领域：数据工程
子方向：数仓与数据湖
