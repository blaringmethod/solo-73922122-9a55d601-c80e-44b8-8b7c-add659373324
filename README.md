# 冷库温度采集与持续监测 API（FastAPI + Modbus TCP + SQLite）

单进程冷链温度服务，包含两部分：

1. **即时采集 API（保留原接口）**：通过 **Modbus TCP 功能码 03** 读取各回路当前温度
   （unit=1，保持寄存器地址 0~7，有符号 16 位整数 ÷ 10 得摄氏温度），读数不缓存。
2. **持续监测 API（新增）**：提交 8 个回路各自的温度上限后，单进程只运行一个监测任务，
   每轮并发采样、逐回路两轮确认超温、区分温度恢复与采样中断（缺测），整轮读数/失败原因、
   判定状态与超温事件一并落 SQLite，支持按任务查询与停止收尾。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

启动后：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集 + 监测 API | http://localhost:8000 | FastAPI 服务 |
| 交互式文档 | http://localhost:8000/docs | Swagger UI，可直接 Try it out 全部启停/查询接口 |
| 演示页面 | http://localhost:8000/ | 即时采集 + 监测任务 + 故障注入可视化 |
| 模拟控制器 Modbus | `simulator:5020`（宿主 `localhost:5020`） | Modbus TCP 从站 |
| 模拟器控制 API | http://localhost:8081 | 切换延迟 / 断线 / 异常 / 寄存器值 |

停止：`docker compose down`（加 `-v` 同时清空监测数据卷）。
SQLite 数据保存在命名卷 `monitor_data`（容器内 `/data/monitoring.db`），容器重建后历史不丢。

## 二、即时采集 API（原有接口，保持不变）

```bash
curl http://localhost:8000/read/0            # 读取单个回路
curl http://localhost:8000/read/all          # 并发读取全部 8 回路
curl -X POST http://localhost:8000/read/batch \
  -H 'Content-Type: application/json' -d '{"circuits":[0,3,7]}'
```

8 笔读取在同一条 TCP 连接上**流水线并发在途**，按 TID 匹配，单项失败不影响其他回路。
错误统一返回中文「采集失败」说明与机器可读 `error` 类别（`timeout` / `connection_lost` /
`connect_failed` / `exception_response` / `protocol_error` / `illegal_address`）。

## 三、持续监测 API（新增）

### 判定规则（逐回路、逐轮）

* **两轮确认超限才开事件**：连续两轮「成功且温度 > 上限」，第一轮记 `over_limit`（待确认），
  第二轮记 `opened` 并开启超温事件。
* **成功不超限**：若事件已开则按**恢复**结束（`recovered`，记录恢复温度）；否则记 `normal`。
* **采集失败（缺测 `missing`）**：重置该回路待确认计数；已开启的事件**只记缺测、绝不解除**，
  从而把「温度恢复」与「采样中断」严格区分开。
* **停止/重启**：仍未结束的事件一律标记为**中断 `interrupted`**，不能记作恢复，且无收尾温度。

### 任务生命周期与并发约束

* 单进程**最多一个运行中的任务**；运行中或停止收尾中再次启动返回 `409`。
* 每轮对 8 回路复用共享 Modbus 客户端并发采样，**每轮结束后等待 1 秒**再开下一轮。
* 停止受理后**不再开启新轮次**，等待在途轮次完成并落库后才上报停止；
  停止中拒绝启动；对同一任务的重复停止请求**等待同一次收尾**；HTTP 请求断连不影响收尾。
* 整轮（8 条读数或失败原因、判定、事件开启/结束）在**一个 SQLite 事务**中提交，
  查询只返回已完整提交的状态。
* 进程重启时自动恢复：上次未正常停止的任务被收尾，残留开启事件标为中断；
  旧任务的运行态不会写入新任务。

### 接口一览（均在 `/docs` 可见）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/monitor/tasks` | 提交 8 个上限并启动唯一任务，返回任务与查询入口 |
| GET | `/monitor/tasks` | 任务列表 |
| GET | `/monitor/tasks/{id}` | 按任务查询状态 |
| POST | `/monitor/tasks/{id}/stop` | 停止（等在途轮落库，重复停止共享收尾） |
| GET | `/monitor/tasks/{id}/rounds?limit=&offset=` | 按任务查询采样轮次（分页） |
| GET | `/monitor/tasks/{id}/rounds/{round_no}` | 单轮整轮读数/失败原因与逐回路判定 |
| GET | `/monitor/tasks/{id}/events?status=&circuit=` | 按任务查询超温事件 |

### 典型调用流程

```bash
# 1) 提交 8 个回路温度上限（℃），下标即回路 0~7
curl -X POST http://localhost:8000/monitor/tasks \
  -H 'Content-Type: application/json' \
  -d '{"thresholds_c":[0,0,10,20,0,35,0,20]}'
# -> 201 {task:{id, status:"running", ...}, rounds_url, events_url}

TID=<上一步返回的 task.id>

# 2) 等待若干轮后查询某一轮（回路 3 当前 23.5℃ > 上限 20℃）
curl http://localhost:8000/monitor/tasks/$TID/rounds/2
# readings[].judge_status: over_limit / opened / normal / recovered / missing

# 3) 查询超温事件（open / recovered / interrupted）
curl "http://localhost:8000/monitor/tasks/$TID/events"

# 4) 停止：在途轮落库后完成，未结束事件标 interrupted
curl -X POST http://localhost:8000/monitor/tasks/$TID/stop
```

### 数据模型（SQLite）

| 表 | 内容 |
| --- | --- |
| `monitor_tasks` | 任务 ID、提交的 8 个上限（JSON）、状态、轮次计数、开启/中断事件计数 |
| `sample_rounds` | 每轮起止时间、成功/失败计数 |
| `readings` | 每回路每轮的读数或失败原因、上限、判定状态、确认计数、关联事件 |
| `events` | 超温事件的开启轮次/温度，以及恢复或中断的结束轮次/温度 |

## 四、模拟器演示：超温、缺测与恢复

容器内自带端到端演示脚本，自动跑完「两轮确认超温 → 缺测不解除 → 恢复结束 →
停止中断」全流程：

```bash
docker compose up --build -d
docker compose exec collector python scripts/demo_monitoring.py
```

也可手动通过模拟器控制 API（:8081）制造场景：

```bash
# 缺测：对回路 3 注入 5 次 Modbus 异常响应（已开事件只记 missing，不解除）
curl -X POST localhost:8081/exception -H 'Content-Type: application/json' \
  -d '{"address":3,"code":2,"count":5}'

# 缺测：回复延迟 2.5 秒，触发采集端 2 秒超时
curl -X POST localhost:8081/mode -H 'Content-Type: application/json' \
  -d '{"delay_seconds":2.5}'

# 恢复：把回路 3 寄存器降到 12.0℃（120 ÷ 10），下一轮成功不超限即按恢复结束事件
curl -X POST localhost:8081/registers -H 'Content-Type: application/json' \
  -d '{"values":[-180,-175,50,120,-200,300,-45,105]}'

# 恢复模拟器全部默认
curl -X POST localhost:8081/reset
```

即时采集的并发/故障演示脚本仍保留：

```bash
docker compose exec collector python scripts/demo_concurrent.py
```

## 五、关键设计

### 即时采集（`app/modbus_client.py`）

1. 全调用共用一条 TCP 连接，懒连接、断线自动重连。
2. 流水线发送：仅短临界区锁串行化 socket 写入，不限制在途数量，每笔带唯一 TID。
3. 后台读取循环按 MBAP `length` 重组分片/粘包，按 **TID 匹配**，乱序回复不串值。
4. 逐字段核对协议标识符、单元号、功能码、字节计数；异常响应报采集失败。
5. 2 秒超时即判失败并拆除整条连接，迟到响应因 TID 回收被丢弃。
6. 每代连接带 `generation`，旧连接残余报文无法干扰新连接。

### 持续监测（`app/monitoring/`）

* `manager.py`：单任务锁、轮询循环（轮间 1 秒可被停止立即唤醒）、逐回路判定状态机、
  停止收尾与重启恢复、代际校验（旧任务残余协程不得写入）。
* `repository.py`：同步 `sqlite3` + 线程锁 + WAL，整轮单事务提交；所有查询走 SQLite，
  保证「返回同一已提交状态」；父目录不可写时回退到用户缓存目录。
* `routes.py` / `schemas.py`：监测 REST 接口与请求/响应模型。
* 监测任务与即时采集**复用同一个共享 Modbus 客户端**。

## 六、本地开发（无 Docker）

```bash
pip install -r requirements-dev.txt

# 终端 1：启动模拟器
python -m simulator.app

# 终端 2：启动采集 + 监测 API
MONITOR_DB_PATH=./monitoring.db MODBUS_HOST=127.0.0.1 MODBUS_PORT=5020 \
  uvicorn app.main:app --reload --port 8000

# 测试（56 个用例：采集协议 27 + 监测状态机/停止 18 + 监测 HTTP 7 + 持久化/重启 4）
pytest -q
```

## 七、配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MODBUS_HOST` | `simulator` | 控制器地址 |
| `MODBUS_PORT` | `5020` | 控制器 Modbus TCP 端口 |
| `MODBUS_UNIT_ID` | `1` | 单元标识符 |
| `MODBUS_TIMEOUT_SECONDS` | `2` | 单笔读取超时（秒） |
| `REGISTER_COUNT` | `8` | 回路数量（地址 0~N-1） |
| `MONITOR_INTERVAL_SECONDS` | `1` | 每轮结束后到下一轮的等待秒数 |
| `MONITOR_DB_PATH` | `/data/monitoring.db` | SQLite 数据库文件路径 |

模拟器变量：`REGISTER_VALUES`、`REPLY_MODE`、`DELAY_SECONDS`、
`DISCONNECT_MODE`、`MODBUS_PORT`、`CONTROL_PORT`。

## 八、目录结构

```
.
├── app/
│   ├── main.py                  # FastAPI 入口、即时采集路由、生命周期/重启恢复
│   ├── modbus_client.py         # 单连接流水线 Modbus 客户端（即时采集复用）
│   ├── config.py                # 环境变量配置
│   ├── static/index.html        # 演示页面（即时采集 + 监测任务 + 故障注入）
│   └── monitoring/
│       ├── manager.py           # 单任务管理、轮询、判定状态机、停止收尾
│       ├── repository.py        # SQLite 仓储（WAL、单事务、重启恢复）
│       ├── routes.py            # /monitor 路由
│       └── schemas.py           # 请求/响应模型
├── simulator/app.py             # 本地模拟控制器（Modbus + 故障注入 API）
├── scripts/
│   ├── demo_concurrent.py       # 即时并发采集演示
│   └── demo_monitoring.py       # 持续监测全流程演示（超温/缺测/恢复/中断）
├── tests/                       # 56 个 pytest 用例
├── Dockerfile
└── docker-compose.yml           # 含 monitor_data 命名卷
```
