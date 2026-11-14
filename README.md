# 机构知识库论文变更同步系统

把机构知识库（源）中论文的**新增 / 修订 / 撤回**同步到检索库（采集端），
保证撤回论文不再被检出。FastAPI + SQLite 实现，`docker compose` 一键启动，
内置一个**无需账号的本地知识库源**，可演示修订、撤回、并发提交、慢响应、断线续采、
游标过期与进程重启恢复。

## 目录结构

```
.
├── docker-compose.yml        # source + collector 两个服务，SQLite 持久化到命名卷
├── source/                   # 本地知识库源（无鉴权，分页 + 签名游标 + 故障/过期注入）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── data.py           # 不可改写的变更事件种子数据（含乱序、重复、旧版重投）
│       └── main.py           # /changes 变更流、/control/* 演示控制、/events
├── collector/                # 采集端：采集 API、暂存、原子发布、可查询论文库
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── db.py             # SQLite 表结构、暂存去重、撤回粘性、原子发布
│       ├── sync.py           # 分页拉取、每页检查点、410 重取原区间、后台 worker
│       ├── schemas.py
│       └── main.py           # 采集 / 续采 / 查询 HTTP API
├── tests/                    # pytest（httpx MockTransport，无需起服务）
├── demo/demo.sh              # 端到端演示脚本
└── requirements-dev.txt      # 本地测试依赖
```

## 快速开始

```bash
docker compose up -d --build
./demo/demo.sh                 # 全流程演示（也可只用下面的 curl 手动调用）
```

- 源服务： http://localhost:8000 （OpenAPI 文档 `/docs`）
- 采集端： http://localhost:8001 （OpenAPI 文档 `/docs`）

## 核心语义如何落地

| 需求 | 实现方式 |
| --- | --- |
| 输入源地址 + 同步截至时间 | `POST /sync/jobs`，body `{"source_url": "...", "end": "...", "start": "..."}`；`start` 可省略 |
| 首次全采，之后从上次完成时间起采 | 无 `completed_at` 时 `start` 取纪元起点（全采）；之后默认取上次区间 `end`。区间两端**闭区间**，边界事件重取并按修订号去重，不丢数据 |
| 源按闭区间分页、变更不可改写、跨页乱序重复 | 源 `/changes?start&end&cursor` 返回不可变事件；采集端按 `(paper_id)` 暂存，**仅保留最高 `rev`**，同修订号重复用 `MAX(is_retracted)` 兜底 |
| 撤回不能被旧版覆盖 | 暂存表与正式表的 UPSERT 都以 `excluded.rev > 现有 rev` 为更新条件；旧修订既改不掉字段也改不掉撤回位（**撤回粘性**） |
| 每页变更与续采游标一同持久化，断线/重启可续采 | 每拉到一页，在**同一个事务**里写 `staged_changes` 行并更新 `jobs.cursor / next_offset / pages_fetched`；重启后 `collecting/committing` 任务被标记为 `interrupted`，可 `resume` |
| 单个 API 进程内只允许一个任务推进，竞争请求返回 409 | 进程级执行槽（`sync._running`）在**创建/续采前**由请求线程 `try_claim()` 抢占，再随线程启动**移交**给 worker；worker 在 `finally` 中释放。抢不到的请求直接 409，**尚未写库**，因此不会新增任务、也不会改动任何已有进度 |
| 被受理的任务不能丢失执行机会 | 提交顺序是“抢占槽 → 写库并提交 → 把已持有的槽交给 worker”，不存在“已写入 collecting 但没人跑”的窗口；万一线程确实起不来，该任务在同一请求内被置为 `failed`（可续采）并返回 503，绝不留下占坑的 `collecting` 行 |
| 完成、失败与后台退出交错时不遗留占用 | worker 在 `finally` 中无条件释放执行槽（正常完成、源故障、未预期异常、提前 return 都一样）；未预期异常还会把任务置为 `failed` 而不是卡在 `collecting`。`start_background` 在持有槽时重新校验任务状态，已 `completed` 的任务不会被“复活” |
| 取页缓慢时状态和论文查询仍能响应 | 拉页在后台线程中执行，HTTP 请求线程从不等待它；源端 `page_delay` 在**状态锁之外** sleep，控制接口/健康检查也不被阻塞 |
| 游标过期则重取原区间，不跳到新截至时间 | 源对过期/越界游标返回 `410`；采集端收到后**清空本任务暂存、游标归零、`cursor_resets+1`**，任务回到可续采，重取的仍是该任务原始 `[start,end]` |
| 整区间取齐才一起更新论文库与完成时间 | 全部页 `done` 后，在**单事务**内把暂存发布到 `papers` 并写 `sync_state.completed_at`；任一步失败整体回滚 |
| 失败时旧库可查、暂存不可见 | 暂存表 `staged_changes` 与查询表 `papers` 物理隔离；查询 API 只读 `papers`，未发布的暂存对外完全不可见 |
| 撤回论文不被检出 | `GET /papers` 默认隐藏 `is_retracted=1`；`GET /papers/{id}` 对撤回论文返回 **HTTP 410 Gone**（加 `?include_retracted=true` 可审计查看） |

> `completed_at` 记录的是本次**区间结束时间**（下次拉取的变更时间基线），
> 而不是墙上时钟，否则回填历史区间时增量起点会晚于历史 `end` 而无法重取。

## 采集端 API

| 方法与路径 | 说明 |
| --- | --- |
| `POST /sync/jobs` | 创建同步任务。body：`end`（必填）、`start`（可选，默认上次完成时间/全采）、`source_url`（可选，默认环境变量）。已有任务推进中返回 **409 且不产生任何副作用** |
| `GET /sync/jobs` | 任务列表（含检查点：页数、偏移、游标、重置次数、暂存数、错误） |
| `GET /sync/jobs/{id}` | 单任务状态 |
| `POST /sync/jobs/{id}/resume` | 续采 `failed/interrupted` 任务，沿用原闭区间与持久化游标；槽被占用时 409 且不改变任务 |
| `GET /sync/state` | 上次同步的源、区间与完成时间 |
| `GET /papers` | 论文列表，默认隐藏撤回；`?include_retracted=true` 可包含 |
| `GET /papers/{id}` | 按编号查询；撤回论文返回 410 |
| `GET /papers-count` | 计数 |

任务状态机：`collecting → committing → completed`；异常进入 `failed`，
进程在途崩溃在重启时变为 `interrupted`，二者均可 `resume`。
单个 API 进程同一时刻只允许一个任务推进：竞争创建/续采返回 409，且被拒请求
不会新增任务、不会改动既有进度（执行槽在写库前抢占）。

### 交接与并发要点

- **先占槽，再造任务**：`POST /sync/jobs` 先 `sync.try_claim()`，失败立刻 409，
  此时还没有任何数据库写入。抢到槽后才做“是否有未完成任务”的检查与插入。
- **占槽随线程移交**：任务行提交后，已持有的槽直接交给 worker（`claim_held=True`），
  没有“先释放再抢”的空档；因此“上一个任务刚完成时提交”不会出现 409 却留孤儿任务。
- **worker 退出必释放**：`run_sync` 的 `finally` 覆盖完成、源 503、410、未预期异常与
  提前返回；未预期异常同时把任务置为 `failed`，不会把 `collecting` 卡死。
- **持久游标与整区间发布不变**：续采仍用原任务号、原 `[start,end]` 与持久化游标，
  游标过期则清空暂存从 offset 0 重取；只有整区间 `done` 后才在单事务内发布到
  `papers` 并推进 `completed_at`。

## 源服务 API（演示用，采集端不调用控制接口）

| 方法与路径 | 说明 |
| --- | --- |
| `GET /changes?start&end[&cursor]` | 闭区间分页变更流；游标带 HMAC 签名、TTL 与代次 |
| `GET /events` | 查看全部事件 |
| `POST /control/fail` `{"times":N}` | 接下来 N 次 `/changes` 返回 503；`times:0` 清除 |
| `POST /control/page-delay` `{"seconds":N}` | 每个 `/changes` 页额外延迟 N 秒（0 关闭），用于演示“取页缓慢时采集端状态/查询仍可响应”；延迟在状态锁外执行 |
| `POST /control/expire-after` `{"requests":N}` | 第 N 次成功响应后作废旧游标（**确定性**游标过期演示） |
| `POST /control/expire-cursors` | 立即作废旧游标（手动） |
| `POST /control/append-batch` | 追加预置增量批次（新论文、再修订、撤回 + 旧版重投） |
| `POST /control/append` | 追加自定义事件 |
| `POST /control/reset` / `GET /control/state` | 重置 / 查看源状态 |

## 手动演示要点

`./demo/demo.sh` 覆盖以下流程，也可以只用 curl 手动调用：

1. **首次全采**：重置源、把每页放慢到 3s，提交全采；期间状态与论文查询立即返回；
   同时并发提交 3 个任务，全部 409，且任务数不变（无孤儿 `collecting`）。
2. **断线续采**：追加增量批次并注入一次 503，任务进入 `failed`；旧库仍可查、暂存不可见；
   清除故障后按原任务号 `resume`，沿用持久游标完成。
3. **游标过期**：第一页后确定性作废旧游标，续采清空暂存并重取**原区间**。
4. **重启恢复**：慢页进行中 `docker compose restart collector`，任务在重启后变为
   `interrupted`（同 id、同 `[start,end]`），随后按原任务号续采并整区间发布。

```bash
# 1) 首次全采（撤回的 P0001/P0003 自动隐藏）
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-08T23:59:59Z"}'
curl localhost:8001/papers/P0001            # 410 Gone
curl 'localhost:8001/papers/P0001?include_retracted=true'

# 2) 断线：制造一次 503，再续采
curl -X POST localhost:8000/control/append-batch
curl -X POST localhost:8000/control/fail -H 'Content-Type: application/json' -d '{"times":1}'
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-10T23:59:59Z"}'       # 任务 failed，旧库仍可查、暂存不可见
curl -X POST localhost:8000/control/fail -H 'Content-Type: application/json' -d '{"times":0}'
curl -X POST localhost:8001/sync/jobs/2/resume

# 3) 游标过期：第一页后确定性作废，续采重取原区间
curl -X POST localhost:8000/control/expire-after -H 'Content-Type: application/json' -d '{"requests":1}'
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-11T23:59:59Z"}'       # cursor_resets=1，再 resume

# 4) 取页缓慢 + 并发提交：状态/查询即时响应，竞争请求 409 且不留任务
curl -X POST localhost:8000/control/page-delay -H 'Content-Type: application/json' -d '{"seconds":2}'
curl -X POST localhost:8001/sync/jobs -H 'Content-Type: application/json' \
  -d '{"end":"2026-11-12T23:59:59Z"}' &
curl -s -o /dev/null -w '%{http_code}\n' localhost:8001/sync/jobs         # 200，立即返回
curl -s -o /dev/null -w '%{http_code}\n' localhost:8001/papers           # 200，立即返回
for i in 1 2 3; do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8001/sync/jobs \
    -H 'Content-Type: application/json' -d '{"end":"2026-11-12T23:59:59Z"}' &
done; wait                                                               # 三个 409
curl -s localhost:8001/sync/jobs | jq length                             # 任务数没有增加

# 5) 重启恢复：慢页进行中 docker compose restart collector，随后按原任务号 resume
```

## 本地测试（不依赖 Docker）

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. python -m pytest -q
```

覆盖：分页乱序/重复去重、最高修订胜出、撤回粘性与 410、失败时旧库可查且暂存
不可见、续采、游标过期重取原区间、重启后 interrupted 翻转、源端非重入锁回归，
以及本轮新增的交接回归：竞争创建/续采 409 不改库也不改进度、worker 异常退出必释放
执行槽并置 failed、慢页期间状态/论文查询仍及时响应、上一个任务刚完成即可提交下一个、
被受理任务确实被执行、源端 `page_delay` 不阻塞控制接口与健康检查。

## 数据安全与隔离说明

- 采集端 SQLite（WAL 模式）放在命名卷 `collector_data`，容器重启不丢检查点。
- 源端事件为 append-only 不可改写；采集端不修改源数据，只做拉取与本地发布。
- 所有"对外可见"的只有 `papers` 表；`staged_changes`、`jobs`、`sync_state`
  仅服务于同步过程与运维观测。
