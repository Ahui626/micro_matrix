# 2. 接口文档

> 📍 第 2 份 / 共 6 份。前置 [1-architecture.md](1-architecture.md) · 下一步 → [3-data-model.md](3-data-model.md)

这个项目对外提供 HTTP 接口，内部服务之间用 gRPC。
这份文档把两层的接口都列清楚。

**读完你应该能**：不查代码就知道怎么调这个系统；知道每个错误码是什么意思、该不该重试。

---

## 概览

| 谁 | 监听 | 协议 | 谁调它 |
|---|---|---|---|
| **gateway** | `127.0.0.1:8000` | HTTP/JSON | 外部客户端 |
| **order** | `127.0.0.1:50051` | gRPC | gateway |
| **inventory** | `127.0.0.1:50052` | gRPC | order |

**外部只应该访问 gateway。** order 和 inventory 的 gRPC 接口虽然也能直接调
（本项目没做网络隔离），但绕过网关就没有限流、没有统一的 trace 起点。

契约的权威来源是这两个文件：
[proto/order.proto](../proto/order.proto) · [proto/inventory.proto](../proto/inventory.proto)

---

# 第一部分：对外 HTTP 接口

## 1. `POST /orders` —— 下单

这是系统最重要的接口，也是唯一会产生副作用的。

### 请求

`Content-Type: application/json`

| 字段 | 类型 | 必填 | 约束 | 说明 |
|---|---|---|---|---|
| `user_id` | string | ✅ | 1–64 字符 | 下单用户 |
| `sku` | string | ✅ | 1–64 字符 | 商品编号 |
| `quantity` | int | ✅ | 1–1000 | 购买数量 |
| `request_id` | string | ❌ | ≤64 字符 | **幂等键**。不传则网关生成一个 |

**关于 `request_id`（重要）**：
- 它是**幂等键** —— 同一个 `request_id` 重复请求，服务端只会执行一次，之后返回第一次的结果
- **调用方应该自己生成并复用**。比如你的客户端超时了要重试，**必须带同一个
  `request_id`**，否则会被当成两笔不同的订单
- 不传的话网关会生成一个新的 —— 那样重试就会变成重复下单，所以**正式客户端一定要传**

#### 到底该怎么生成

**一句话规则：在「用户按下提交」的那一刻生成一次，存起来，之后所有重试都用它。**

一个 `request_id` = 一次**逻辑操作**，不是一次 HTTP 请求。
一次逻辑操作可能对应 N 次传输尝试，这 N 次必须共用一个 id。

**用什么生成** —— UUID4 去掉横线是最通用的选择：

| 环境 | 写法 | 长度 |
|---|---|---|
| Python | `uuid.uuid4().hex` | 32 |
| PowerShell | `[guid]::NewGuid().ToString("N")` | 32 |
| JavaScript | `crypto.randomUUID().replace(/-/g,"")` | 32 |
| Java | `UUID.randomUUID().toString().replace("-","")` | 32 |

为什么是它：**不需要协调**（不像自增 ID 要中心分配，客户端本地就能生成）、
**碰撞概率可忽略**（122 位随机）、长度合适（本接口限制 ≤64 字符）。

**三种典型场景**：

```javascript
// ① 前端表单提交 —— 生成在"点击提交"里，不在 fetch 的重试逻辑里
async function submitOrder() {
  const requestId = sessionStorage.getItem("pendingOrder")
                 ?? crypto.randomUUID().replace(/-/g, "");
  sessionStorage.setItem("pendingOrder", requestId);   // 存起来
  await postOrder({ ..., request_id: requestId });
}
// 存进 sessionStorage 的意义：页面刷新后重试还是同一个 id，不会变成重复下单
```

② **后端调后端**：上游生成、往下传。本项目就是这样 ——
客户端 → gateway → order → inventory 一路透传同一个 id。

③ **定时任务 / 消息消费**：用业务上天然唯一的键，比如
`f"daily-report:{date}"`、`f"refund:{order_id}"`。

**❌ 最经典的错误 —— 在重试循环里生成**：

```python
# 错：每次重试都是新 id，幂等完全失效
for attempt in range(3):
    rid = uuid.uuid4().hex        # ← 生成在这里就错了
    requests.post(url, json={"request_id": rid, ...})

# 对：循环外生成一次
rid = uuid.uuid4().hex
for attempt in range(3):
    requests.post(url, json={"request_id": rid, ...})
```

注意：框架的重试中间件（gRPC 拦截器、HTTP 客户端的 retry 策略）**不会**帮你处理这个 ——
它们重放的是同一个请求体，前提是你已经把 id 放进去了。

**⚠️ 网关代生成只是为了调试方便，不是推荐做法**：

```python
request_id = body.request_id or uuid.uuid4().hex   # services/gateway/http_api.py
```

不传就替你生成一个 —— 方便 curl 手敲时不用现编 UUID。
但它是**每次请求都新生成**的，等于假装有幂等：重试照样会重复下单。

生产系统更该做的是**拒绝**没有 `request_id` 的写请求（返回 400），逼调用方负起责任。

**⏳ 关于过期清理**：`api_idempotency` 表只需要覆盖「重试窗口」（比如 24 小时），
过了就可以删。代价是有人手动重放一个很旧的请求会创建新订单 ——
这个取舍可接受，因为**幂等保证的是「重试安全」，不是「永久去重」**。

### 成功响应 `201 Created`

```json
{
  "id": 375706001543794688,
  "status": "CONFIRMED",
  "sku": "SKU-1",
  "quantity": 2,
  "user_id": "u1",
  "deduplicated": false
}
```

| 字段 | 说明 |
|---|---|
| `id` | 订单 ID，Snowflake 生成的 64 位整数 |
| `status` | `CONFIRMED` / `CANCELLED`（见下方"订单状态"） |
| `deduplicated` | `true` 表示这次是重复请求，返回的是**第一次**的结果 |

响应头里会带 `x-trace-id` —— 拿它去日志里查这次请求的完整经过。

### ⚠️ `201` 不代表下单成功

这是本项目一个**刻意的设计**，第一次看到会意外：

```powershell
# 库存只有 5，却要买 999 件

$body = @{ user_id="u1"; sku="SKU-1"; quantity=999; request_id="r-2" } | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:8000/orders -Method Post -Body $body -ContentType "application/json"
# → 201 Created，但 status = "CANCELLED"

```

**为什么返回 201 而不是错误**：订单记录**确实被创建了**（数据库里有这一行，
可以查到、可以对账）。如果返回 409，等于告诉客户端"什么都没发生" ——
而事实上有一个处于 CANCELLED 的订单存在。而且 Saga 的结果可能是
**异步**确定的，用异常语义根本承载不了。

**代价**：客户端必须检查响应体里的 `status`，不能只看 HTTP 状态码。

### 错误响应

| 状态码 | 含义 | 客户端该怎么做 |
|---|---|---|
| `422` | 参数不合法（pydantic 校验失败） | **不要重试**，改参数 |
| `429` | 触发限流（默认每用户 20 请求/秒） | 等 `Retry-After` 秒后重试 |
| `503` | 下游不可用（order 服务挂了） | **可以重试**（响应头有 `X-Retryable: true`） |
| `504` | 超时（整条链路超过 3 秒） | 可以重试，但**必须带同一个 `request_id`** |
| `409` | 前置条件不满足 | 不要重试 |
| `502` | 网关自身异常 | 视情况，可重试 |

> **重试前一定要看 `X-Retryable` 响应头**。网关会把"这个错误值不值得重试"
> 明确告诉你，不要自己猜。语义定义见 [common/errors.py](../common/errors.py)。

### 示例

```powershell
$body = @{
    user_id    = "u1"
    sku        = "SKU-1"
    quantity   = 2
    request_id = "my-request-001"
} | ConvertTo-Json

$resp = Invoke-WebRequest -Uri http://127.0.0.1:8000/orders -Method Post -UseBasicParsing `
        -ContentType "application/json; charset=utf-8" -Body $body
$resp.StatusCode
$resp.Headers['x-trace-id']
$resp.Content | ConvertFrom-Json
```

---

## 2. `GET /orders/{id}` —— 查订单

### 请求

路径参数 `id`：订单 ID（整数）。

### 响应 `200 OK`

```json
{
  "id": 375706001543794688,
  "status": "CONFIRMED",
  "sku": "SKU-1",
  "quantity": 2,
  "user_id": "u1",
  "deduplicated": false
}
```

注意：查单不涉及幂等，`deduplicated` 恒为 `false`。

### 错误

| 状态码 | 含义 |
|---|---|
| `404` | 订单不存在 |
| `503` / `504` | 下游不可用 / 超时 |

---

## 3. `GET /healthz` —— 健康检查

```json
{"status": "ok", "redis": "up"}
```

**它会真的检查依赖**（Redis 连不连得上），不是只返回 200。
一个"进程活着但连不上 Redis"的实例不该被放进负载均衡 ——
否则它接到的每个请求都会失败。

Redis 挂了会返回 `503` 加 `{"status": "degraded", "redis": "down"}`。

> K8s 的 **readiness 探针**就是打这个接口。注意不能拿它当 **liveness** 探针 ——
> liveness 失败会**重启**容器，如果它去检查 Redis，Redis 一抖你所有的 Pod
> 都会被重启，本来只是缓存问题，现在变成全站雪崩。

---

## 4. `GET /metrics` / `GET /metrics/json`

`/metrics` 返回 Prometheus 文本格式，可以直接接一个 Prometheus 来抓：

```
mm_counter_http_requests_total 128
mm_latency_p99_ms_http/orders 83.20
mm_latency_count_order_create 120
```

`/metrics/json` 返回同样的数据但用 JSON，方便脚本处理：

```json
{
  "counters": {"http_requests_total": 128, "order_deduplicated_total": 3},
  "latency": {
    "order.create": {"count": 120, "p50_ms": 22.5, "p95_ms": 29.8, "p99_ms": 36.4, "max_ms": 120.1}
  }
}
```

⚠️ **这是网关自己进程内的指标**。熔断器的状态在 order 进程里，这里读不到 ——
进程内状态天然无法被其它进程观测。要看全局必须把指标推送到 Prometheus 聚合。

---

# 第二部分：内部 gRPC 接口

这两个服务不对外，但理解它们的契约才能看懂代码。

## OrderService（order 服务提供）

### `CreateOrder`

```protobuf
rpc CreateOrder(CreateOrderRequest) returns (CreateOrderResponse);
```

| 请求字段 | 类型 | 说明 |
|---|---|---|
| `request_id` | string | 幂等键。网关从 HTTP 请求透传下来 |
| `user_id` | string | |
| `sku` | string | |
| `quantity` | int32 | |

| 响应字段 | 类型 | 说明 |
|---|---|---|
| `order` | Order | 订单完整信息 |
| `deduplicated` | bool | 是否命中幂等 |

**调用方（gateway）必须设置的 deadline**：3000ms。
这个值会随请求传下去，order 再切一部分给 inventory（见 [1-architecture.md](1-architecture.md) 3.5）。

### `GetOrder`

```protobuf
rpc GetOrder(GetOrderRequest) returns (Order);
```

请求：`id` (int64)。找不到时返回 `NOT_FOUND`。

### `Order` 消息

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | int64 | Snowflake 生成的订单号 |
| `request_id` | string | 幂等键 |
| `user_id` / `sku` | string | |
| `quantity` | int32 | |
| `status` | string | `PENDING` / `CONFIRMED` / `CANCELLED` |
| `created_at_ms` | int64 | 毫秒时间戳 |

---

## InventoryService（inventory 服务提供）

### `Reserve` —— 预留库存

```protobuf
rpc Reserve(ReserveRequest) returns (ReserveResponse);
```

| 请求字段 | 说明 |
|---|---|
| `request_id` | 幂等键。**同一个 key 重复调用不会重复扣减** |
| `sku` | |
| `quantity` | int32 |

| 响应字段 | 说明 |
|---|---|
| `ok` | 业务结果。`false` 是**正常返回**，不是错误 |
| `remaining` | 预留后剩余库存 |
| `reason` | 失败原因：`OUT_OF_STOCK` / `SKU_NOT_FOUND` / `""` |
| `reservation_id` | 本次预留的 ID，补偿时用得上 |

**注意 `ok=false` 不是 gRPC 错误。** 库存不足是**业务上的正常结果** ——
用错误码表达会让调用方很难区分"业务拒绝"和"网络故障"，
而这两者的处理方式完全相反：前者不该重试，后者必须重试。

### `Release` —— 释放库存（补偿）

```protobuf
rpc Release(ReleaseRequest) returns (ReleaseResponse);
```

这是 Saga 的**补偿动作**：前一步成功了、后一步失败了，就把库存放回去。

**它必须幂等** —— 补偿动作自己也会被重试。代码里靠
`UPDATE reservations SET status='RELEASED' WHERE status='RESERVED'`
保证第 2..N 次调用不会重复加库存。

注意 `Release` 用的是**当初预留的记录里的数量**，而不是请求里传的 `quantity`。
调用方传的值可能不对，不能信。

### `GetStock` —— 查库存

```protobuf
rpc GetStock(GetStockRequest) returns (Stock);
```

`Stock` 消息：`sku`、`available`（可用量）、`version`（乐观锁版本号）。

---

# 第三部分：状态与错误语义

## 订单状态机

```
              ┌──────────┐
  创建订单 ──>│ PENDING  │
              └────┬─────┘
                   │
        ┌──────────┴──────────┐
        │                     │
   全部成功               任一步失败
        │                     │
        ▼                     ▼
  ┌───────────┐        ┌────────────┐
  │ CONFIRMED │        │ CANCELLED  │
  └───────────┘        └────────────┘
```

`PENDING` 只存在于 Saga 执行过程中。**如果进程在 Saga 中途崩溃，
订单会永远停在 `PENDING`** —— 真实系统需要"对账任务"扫描这类订单并推进。
本项目没有实现对账，这是个已知的简化（见 [4-concepts.md](4-concepts.md)）。

## gRPC 状态码与可重试性

这张表是 [common/errors.py](../common/errors.py) 的权威定义：

| 状态码 | 可重试 | 什么时候出现 |
|---|---|---|
| `OK` | —— | 成功 |
| `UNAVAILABLE` | ✅ | 对端挂了 / 重启中 / 数据库不可用 |
| `DEADLINE_EXCEEDED` | ✅ | 超时 |
| `ABORTED` | ✅ | 并发冲突 |
| `RESOURCE_EXHAUSTED` | ✅ | 被限流（**必须配合退避**） |
| `INVALID_ARGUMENT` | ❌ | 参数不合法 |
| `NOT_FOUND` | ❌ | 资源不存在 |
| `ALREADY_EXISTS` | ❌ | 幂等冲突 |
| `FAILED_PRECONDITION` | ❌ | 前置条件不满足 |
| `PERMISSION_DENIED` / `UNAUTHENTICATED` | ❌ | 权限 |
| `UNIMPLEMENTED` | ❌ | 方法不存在 |
| `INTERNAL` / `UNKNOWN` | ❌ | 未知错误（保守起见不重试） |

**为什么用白名单而不是黑名单**：只有明确知道可重试的才重试。
错误地重试比不重试更危险 —— 不重试只是这一次失败，错误重试会放大故障。

---

# 第四部分：几个刻意的设计选择

这些是面试里容易被追问的点，代码和注释里都有说明。

| 选择 | 为什么不选另一种 |
|---|---|
| 库存不足返回 **201 + `status=CANCELLED`**，不是 409 | 订单确实被创建了。返回异常等于说"什么都没发生" |
| `Reserve` 用 `ok=false` 表达库存不足，不用 gRPC 错误码 | 业务拒绝和网络故障的处理方式完全相反，必须能区分 |
| 幂等键由**客户端**生成，不是服务端 | 只有调用方知道自己"重试了同一个逻辑操作" |
| `api_idempotency` 存**完整响应**，不存标记 | 只有这样重复请求才能返回和第一次**完全相同**的结果 |
| `Release` 用记录里的数量，忽略请求里的 `quantity` | 调用方可能传错，或者意在"多退一点" |
| `status` 用 `string` 而不是 protobuf `enum` | enum 加值需要双方同时升级 proto，string 对未知值天然兼容 |

---

## 下一步

接口知道了，接下来看数据：[3-data-model.md](3-data-model.md) ——
7 张表分别存什么、为什么这么设计。
