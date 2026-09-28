# micro_matrix

一个为了**搞懂分布式与微服务**而写的 Python 项目。
业务很简单（下单扣库存），复杂度全部放在分布式问题上。

**如果你还不了解微服务 —— 那正好，这个项目就是为此准备的。**
从 [docs/1-architecture.md](docs/1-architecture.md) 开始，它从
"微服务系统通常由哪些部分组成"讲起。

---

## 怎么学：三步，别跳步

| 步骤 | 做什么 | 预计 |
|---|---|---|
| **① 先理解它是什么** | 读 `docs/` 的前三份文档 | 约 2 小时 |
| **② 再读代码** | 按调用顺序读 6 组文件 | 约 6 小时 |
| **③ 然后亲手弄坏它** | 跑 5 个 lab，看到每个机制在防什么 | 约 1.5 小时 |

**不要跳到第 ② 步。** 代码里的注释有 100 多个知识点，
但在你建立"这个系统长什么样"的认知之前，它们只是碎片。
第 ① 步就是为了建立那个认知。

---

## 文档地图

| 文档 | 讲什么 | 什么时候读 |
|---|---|---|
| **[1-architecture.md](docs/1-architecture.md)** | 微服务系统有哪些部分 · 本项目全貌 · **服务之间怎么调用** | **第一步，从这里开始** |
| **[2-api.md](docs/2-api.md)** | 接口文档：对外 HTTP + 内部 gRPC，字段、状态码、错误语义 | 第一步 |
| **[3-data-model.md](docs/3-data-model.md)** | 7 张表的结构、为什么这么设计、一次下单会写哪些表 | 第一步 |
| [4-concepts.md](docs/4-concepts.md) | **7 条核心结论** + CAP / Saga / exactly-once / MVCC 等概念 | 第二步之后 |
| [5-interview-index.md](docs/5-interview-index.md) | 面试题 → 代码的反向索引 | ⚠️ **查字典用，不要通读** |
| [6-runbook.md](docs/6-runbook.md) | 操作手册 · Windows + Python 3.14 踩坑记录 · 出问题怎么查 | 出问题时 |
| [7-deployment.md](docs/7-deployment.md) | 服务发现（Consul / K8s DNS）· 容器化 · K8s 清单 · 怎么排查 | 想部署 / 上 K8s 时 |
| [8-ecosystem.md](docs/8-ecosystem.md) | Docker / Docker Desktop / K8s / 镜像仓库的关系 · 上 K8s 踩坑复盘 | 分不清这几个、上 K8s 卡住时 |

> 源码里每个文件顶部都有统一的四行头，`5-interview-index.md` 把其中的
> `[面试]` 行抽出来做成了反向索引：
>
> ```
> [问题]   这个文件解决什么问题
> [本文件] 怎么解决的
> [面试]   它直接回答的面试题        ← 索引的就是这一行
> [取舍]   选了 A 没选 B，为什么
> ```

---

## 跑起来

### 命令约定

**本文档所有命令都是 PowerShell 语法**（Windows 默认终端）。如果你用 Git Bash，
对应关系在这里，不用每处都找：

| 用途 | PowerShell（本文档） | Git Bash |
|---|---|---|
| 环境变量 | `$env:MM_CHAOS_PAYMENT_FAIL=1` | `MM_CHAOS_PAYMENT_FAIL=1` |
| 带 JSON 发请求 | `Invoke-RestMethod` | `curl -d '{...}'` |
| 搜日志 | `Select-String -Path logs\*.log -Pattern x` | `grep x logs/*.log` |
| 跟踪日志 | `Get-Content logs\order.log -Wait -Tail 30` | `tail -f logs/order.log` |
| 路径分隔符 | `\` | `/` |

> **为什么不用 curl 发 JSON**：PowerShell 向原生程序传参时会**剥掉内嵌的双引号**，
> `-d '{"a":"b"}'` 到 curl 手里变成 `{a:b}`，服务端直接报 JSON 解析错误。
> 这不是 bug 是 PowerShell 的传参规则。用 `Invoke-RestMethod` 最省事。

### 装依赖

```powershell
# 1. 中间件（Redis + Postgres）—— 只需要这两个
docker compose up -d

# 2. 建虚拟环境并激活
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 如果报"禁止运行脚本"，先执行一次（只对当前窗口生效）：
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 3. 装依赖
python -m pip install -r requirements.txt

# 4. 环境体检 —— 先确认技术栈在这台机器上是活的
python scripts/smoke.py

# 5. 生成 gRPC 代码
python scripts/gen_proto.py

# 6. 播种库存（首次必须做，否则下单全是"库存不足"）
python scripts/seed.py
```

### 起服务

```powershell
python scripts/run_all.py
```

会轮询就绪（不是盲等 sleep），就绪后打印所有端点。**这个窗口会被占住**，
下面操作请另开一个 PowerShell 窗口。

### 下第一单

```powershell
$body = @{
    user_id    = "u1"
    sku        = "SKU-1"
    quantity   = 2
    request_id = "r-1"
} | ConvertTo-Json

Invoke-RestMethod -Uri http://127.0.0.1:8000/orders -Method Post `
    -ContentType "application/json; charset=utf-8" -Body $body
```

接口的完整说明（字段约束、所有错误码、幂等语义）见 [docs/2-api.md](docs/2-api.md)。

---

## 架构一览

```
                   外部客户端
                       │ HTTP
                       ▼
        ┌──────────────────────────────┐
        │  gateway   :8000             │  限流 · trace 起点
        │  FastAPI                     │  HTTP → gRPC 翻译
        └──────────────┬───────────────┘
                       │ gRPC ← deadline 从这里逐跳收紧
                       ▼
        ┌──────────────────────────────┐
        │  order   :50051              │  下单事务 · 幂等
        │  grpc.aio                    │  Saga 编排 · Outbox
        └───────┬──────────────┬───────┘
                │ gRPC         │ 后台任务：relay + consumer
                ▼              ▼
   ┌────────────────────┐  ┌──────────────────────┐
   │ inventory  :50052  │  │ Redis Streams        │
   │ 原子扣减 · 补偿     │  │ 事件 → 加积分等副作用  │
   └────────────────────┘  └──────────────────────┘
                │
                ▼
   ┌────────────────────────────────────────────┐
   │ Postgres（7 张表）        Redis（限流/锁/消息） │
   └────────────────────────────────────────────┘
```

**两条路径的划分本身就是设计的一部分：**

- **同步关键路径** gateway → order → inventory
  承载：gRPC、deadline 逐跳传播、幂等、防超卖
- **异步非关键路径** order 单事务写 orders + outbox → relay → Stream → consumer
  承载：at-least-once、幂等消费者、死信队列、最终一致性

HTTP 只存在于边缘，内部一律 gRPC。

**想知道每一部分为什么存在、服务之间具体怎么调用** ——
见 [docs/1-architecture.md](docs/1-architecture.md)。

---

## 读代码：6 组文件

**前提：你已经读完 `docs/` 的前三份。** 每组都有自检问题，答得上来再往下。

### ① 一次下单的完整旅程（约 1 小时）

按调用顺序读，每读一个就搜一下它在哪被调用：

1. [services/gateway/http_api.py](services/gateway/http_api.py) —— 限流、trace 起点、错误映射
2. [common/trace.py](common/trace.py) —— ★ trace_id 怎么跨进程（含一个静默失效的坑）
3. [common/clients.py](common/clients.py) —— channel 复用、**deadline 逐跳收紧**
4. [services/order/service.py](services/order/service.py) —— ★ Saga：三步 + 逆向补偿

**自检**：给下游的超时是谁算的？支付失败后哪几步被撤销、按什么顺序？
Saga 有没有隔离性？

### ② 为什么不会超卖（约 1 小时）

[services/inventory/repo.py](services/inventory/repo.py)（三种实现对比）
→ [services/inventory/service.py](services/inventory/service.py)（幂等与事务边界）
→ [services/order/repo.py](services/order/repo.py)（唯一约束 = 最后一道防线）

**自检**：竞态窗口具体在哪两行之间？为什么 `WHERE stock >= $1` 不能省？
这个项目**哪里用了分布式锁**？

### ③ 事件怎么可靠地传出去（约 1 小时）

[services/order/repo.py](services/order/repo.py)（Outbox 与订单同事务）
→ [services/order/relay.py](services/order/relay.py)（at-least-once 的来源）
→ [services/order/consumer.py](services/order/consumer.py)（ack 时机、幂等、DLQ）

**自检**：为什么「写订单」和「发消息」不能分开写？
relay 崩溃重启后事件会丢吗、会重复吗？
消费端幂等为什么必须和副作用在**同一个事务**里？

### ④ 出故障时会怎样（约 1 小时）

[common/retry.py](common/retry.py) → [common/breaker.py](common/breaker.py)
→ [common/ratelimit.py](common/ratelimit.py) → [common/lock.py](common/lock.py)
→ [common/errors.py](common/errors.py)

**自检**：重试和熔断谁套谁？为什么限流必须用 Lua？
锁的 TTL 该设多久？

### ⑤ 服务的骨架（约 40 分钟）

[services/order/main.py](services/order/main.py)（后台任务 + 优雅停机）
→ [services/order/grpc_api.py](services/order/grpc_api.py)（传输层为什么必须薄）
→ [common/db.py](common/db.py)（表结构定义 + 为什么不用 ORM）

### ⑥ 实验的开关（约 20 分钟）

[common/chaos.py](common/chaos.py) —— 环境变量控制的故障注入。
这是第 ③ 步 labs 的基础，读懂它才知道 lab 是怎么"把系统弄坏"的。

### 可以跳过什么

**别把 6600 行都读完。** 下面这些读一眼文件头部的四行就够：

| 文件 | 为什么可以跳 |
|---|---|
| [gen/](gen/) 全部 | 机器生成的，头部明确写了「不要读」 |
| [common/settings.py](common/settings.py) · [logging.py](common/logging.py) | 配置和日志的样板 |
| [common/metrics.py](common/metrics.py) · [ids.py](common/ids.py) | 配套工具。想看 p99 怎么算、Snowflake 怎么防时钟回拨再回来 |
| [common/reset.py](common/reset.py) · [lifecycle.py](common/lifecycle.py) | 实验和停机的辅助代码 |

**判断标准**：如果读的时候想的是「哦，就是管道而已」，那就可以跳。

---

## 亲手弄坏它：5 个 lab

**这是第三步，也是最重要的一步。** 光读代码你不知道这些机制在防什么。

```powershell
python labs\lab1_oversell.py         # 超卖：await 就是竞态窗口
python labs\lab2_dup_consume.py      # 重复消费与 ack 时机
python labs\lab3_lock_fencing.py     # 锁失效与 fencing token
python labs\lab4_outbox_twinwrite.py # 双写不一致与本地消息表
python labs\lab5_retry_storm.py      # 重试风暴
```

| lab | 它证明什么 |
|---|---|
| [lab1](labs/lab1_oversell.py) | 库存 5，8 个并发请求**全部**扣减成功 |
| [lab2](labs/lab2_dup_consume.py) | 同一消息投递 3 次 → 不幂等就加 3 次积分 |
| [lab3](labs/lab3_lock_fencing.py) | 锁过期 → 两个持有者同时进临界区 |
| [lab4](labs/lab4_outbox_twinwrite.py) | 只写库不发消息 → 事件永久丢失 |
| [lab5](labs/lab5_retry_storm.py) | 无抖动重试 → 30 个调用者同一瞬间重试 |

每个 lab 都会**断言出错误行为** —— 看到 `✓ 结果符合预期` 才算跑对。
前四个只需要中间件；`lab5` 会自己起一个带故障注入的实例。

压测（注意 `--user`，限流是按用户维度的，默认 20/s）：

```powershell
python labs\loadgen.py --n 200 --concurrency 50 --user load1
```

---

## 常用操作

**查数据**（一致性交叉验证等）见 [docs/3-data-model.md](docs/3-data-model.md) 和
[docs/6-runbook.md](docs/6-runbook.md)。

**看日志**：

```powershell
Get-Content logs\order.log -Wait -Tail 30
```

> 日志文件带 UTF-8 BOM，所以中文能正确显示。看到乱码的话加 `-Encoding utf8`，
> 原因见 [RUNBOOK 第 10 条](docs/6-runbook.md)。

**按 trace_id 串起一次请求的全链路**：

```powershell
Select-String -Path logs\*.log -Pattern "d4ce5835" -SimpleMatch |
    ForEach-Object { $_.Filename + "  " + $_.Line }
```

**重置环境**：

```powershell
python scripts\seed.py --reset        # 清空数据库和 Redis，重新播种
```

**故障注入**（改环境变量后**重启**对应服务）：

```powershell
# 让扣库存变成「先读后写」→ 真实并发下会超卖
$env:MM_CHAOS_DISABLE_ATOMIC_DECR = "1"
python services\inventory\main.py
```

> ⚠️ `$env:` 设的是**当前 PowerShell 窗口**的环境变量，不是只对这一条命令生效
> （和 bash 的 `VAR=x cmd` 不同）。所以**重启服务时它还在** ——
> 这正是"我明明没开注入，怎么行为不对"的常见原因。
> 取消：`Remove-Item Env:\MM_CHAOS_DISABLE_ATOMIC_DECR`

开关清单见 [common/chaos.py](common/chaos.py)。

---

## 部署到容器 / K8s

本地开发业务服务跑宿主机（见上面「跑起来」）。要容器化或上 K8s：

- **全栈容器化**：`cd deploy && docker compose up -d --build`
- **上 K8s**：打镜像 → 推本地私有仓库 → `kubectl apply -f deploy/k8s/`

服务发现（Consul vs K8s DNS）、本地私有仓库、K8s 清单怎么读怎么排查，见
[docs/7-deployment.md](docs/7-deployment.md)；Docker / Docker Desktop / K8s / 镜像仓库
之间的关系和踩坑复盘，见 [docs/8-ecosystem.md](docs/8-ecosystem.md)。

---

## 这个项目**不做**什么

明确砍掉的，以及为什么 —— 这些同样值得在面试里讲：

| 没做 | 原因 |
|---|---|
| 手写 Raft / 注册中心内核 | 用 Consul（成熟方案）。自己只写注册/心跳/解析/注销四个 HTTP 调用，见 [common/discovery.py](common/discovery.py) |
| SQLAlchemy | 防超卖那行 SQL 必须被看见，ORM 会把它藏起来 |
| OpenTelemetry SDK | 手写 60 行让「trace_id 怎么过网络」完全暴露 |
| Temporal / Nameko | 会把机制藏起来。Nameko 基于 eventlet，2026 已实质废弃 |
| 独立的 payment 服务 | 折成 order 内一个可控失败的函数，省一个进程 |
| outbox / 幂等表的清理任务 | 真实系统必须有，这里没实现。见 [3-data-model.md](docs/3-data-model.md) |
