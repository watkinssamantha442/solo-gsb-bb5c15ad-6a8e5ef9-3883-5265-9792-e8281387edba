# Barrier Coordination API

供并行任务协调收尾的屏障（rendezvous barrier）服务。客户端创建带**参与者名单、
达标人数（quorum）和截止时间**的屏障；参与者以自身标识**幂等报到**；并发报到
首次达到人数时在 Redis 内**原子冻结**达标成员快照，后续报到不会改写结果。
等待者通过阻塞接口在达标瞬间拿到冻结快照，或在截止时拿到已报到成员与缺席名单，
任意数量的并发等待者都会被可靠唤醒。

技术栈：Python 3.12 · FastAPI · Redis 7（Lua 脚本保证原子性，Pub/Sub 负责唤醒）。

## 快速启动

一条命令构建并启动 API 与其依赖的 Redis：

```bash
docker compose up --build
```

启动后：

| 地址 | 说明 |
|---|---|
| `http://localhost:8000` | API 入口 |
| `http://localhost:8000/docs` | Swagger UI 交互文档 |
| `http://localhost:8000/openapi.json` | OpenAPI  schema |
| `http://localhost:8000/healthz` | 健康检查（含 Redis 连通性） |

Redis 同时暴露在 `localhost:6379` 便于调试。停止并清理：`docker compose down`。

## 配置

通过环境变量配置（`docker-compose.yml` 的 `api.environment` 或本地 shell）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0`（compose 中为 `redis://redis:6379/0`） | Redis 连接串 |
| `BARRIER_RETENTION_SECONDS` | `3600` | 截止后屏障数据在 Redis 中的保留时长，到期自动清理 |
| `REDIS_MAX_CONNECTIONS` | `1000` | Redis 连接池上限（每个阻塞等待者持有 1 条连接） |

## API 用法

### 1. 创建屏障 — `POST /barriers`

`deadline`（绝对 ISO-8601 时间）与 `ttl_seconds`（相对秒数）二选一。

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{
        "participants": ["worker-1", "worker-2", "worker-3"],
        "threshold": 2,
        "ttl_seconds": 300
      }'
```

响应（201）：

```json
{
  "barrier_id": "9f1c...",
  "status": "open",
  "threshold": 2,
  "deadline": "2026-09-20T12:05:00+00:00",
  "participants": ["worker-1", "worker-2", "worker-3"],
  "checked_in": [],
  "checked_in_count": 0,
  "absent": ["worker-1", "worker-2", "worker-3"],
  "remaining_seconds": 299.9,
  "met_members": null,
  "met_at": null
}
```

### 2. 幂等报到 — `POST /barriers/{id}/checkin`

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/checkin \
  -H 'Content-Type: application/json' \
  -d '{"participant_id": "worker-1"}'
```

* 同一参与者重复报到是安全 no-op（`recorded: false`），可放心重试。
* 使计数首次达到 threshold 的那次报到会在**同一原子操作**内冻结达标成员名单；
  之后的报到返回 `barrier_status: "met"`，不会改写冻结结果。
* 非名单成员 → `403`；屏障不存在 → `404`；截止后报到 → `409`。

### 3. 阻塞等待 — `GET /barriers/{id}/wait`

```bash
# 一直阻塞到达标或截止
curl -s http://localhost:8000/barriers/<id>/wait

# 可选：调用方自定义最长等待秒数（仍受截止时间约束）
curl -s 'http://localhost:8000/barriers/<id>/wait?timeout=30'
```

* **达标时**：立即返回 `{"status": "met", "members": [...], "met_at": ...}`，
  `members` 即冻结的达标快照。
* **截止时**：返回 `{"status": "closed", "checked_in": [...], "absent": [...]}`。
* **调用方超时先到期**：返回 `status: "open"` 及当前出勤情况，可再次等待。
* 任意数量的并发等待者均通过 Redis Pub/Sub 被可靠唤醒。

### 4. 查询状态 — `GET /barriers/{id}`

非阻塞返回完整状态（字段同创建响应）。

## 语义保证

* **原子冻结**：报到逻辑整体封装在单个 Redis Lua 脚本中执行（Redis 单线程
  串行化脚本），首次达标与快照冻结不可分割；冻结后状态机进入 `met`，后续
  报到在脚本内直接短路，结果不可改写。
* **幂等**：报到基于集合 `SADD`，重复报到不产生副作用；创建时名单去重。
* **可靠唤醒**：等待者先订阅 Pub/Sub 频道**再**复查状态，事件不会丢失；
  截止时间由等待者预算内超时与惰性收尾（finalize）共同保证，且收尾会广播
  唤醒其余等待者。
* **时钟一致**：脚本内使用 Redis `TIME` 而非应用服务器时钟判定截止。

## 本地开发（不用 Docker）

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
redis-server &                                   # 或指向任意 Redis
REDIS_URL=redis://localhost:6379/0 \
  uvicorn app.main:app --reload --port 8000
```

## 冒烟测试

服务运行后执行，覆盖并发冻结、幂等、多等待者唤醒、截止缺席名单等场景：

```bash
pip install -r requirements-dev.txt
python scripts/smoke_test.py        # 默认打 http://localhost:8000
```

## 项目结构

```
app/
  main.py       FastAPI 入口、生命周期、异常处理
  barriers.py   路由与阻塞等待逻辑
  lua.py        原子报到 / 惰性收尾 Lua 脚本
  models.py     Pydantic 请求/响应模型
  config.py     环境变量配置
scripts/
  smoke_test.py 端到端冒烟测试
Dockerfile
docker-compose.yml
```
