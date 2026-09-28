# 3. 数据模型

> 📍 第 3 份 / 共 6 份。前置 [2-api.md](2-api.md) · 下一步 → [4-concepts.md](4-concepts.md)

这个项目一共 **7 张表**。数量不多，但每张表的存在都有明确理由 ——
**每张表都在解决一个分布式问题**，不是为了存字段而建。

表结构的权威来源是 [common/db.py](../common/db.py) 里的 `SCHEMA` 常量，
每张表上面都有注释说明它解决什么问题。

**读完你应该能**：知道每个数据存哪、为什么这么设计、一次下单会写哪些表。

---

## 总览：7 张表分三组

| 组 | 表 | 干什么 |
|---|---|---|
| **A. 业务数据** | `inventory` | 库存（真相之源） |
| | `orders` | 订单 |
| **B. 幂等支撑** | `api_idempotency` | HTTP 入口的幂等 |
| | `reservations` | 服务间调用的幂等 + 补偿依据 |
| | `processed_events` | 消息消费的幂等 |
| **C. 异步投递** | `outbox` | 待发送的事件（本地消息表） |
| | `points` | 消费者的副作用（用来验证幂等是否生效） |

**B 组的三张表最容易被混淆**，但它们的**作用范围完全不同** ——
这是理解这个项目的关键。下面会重点讲。

---

# A. 业务数据

## `inventory` —— 库存

```sql
CREATE TABLE inventory (
    sku     TEXT PRIMARY KEY,
    stock   INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0
);
```

| 列 | 说明 |
|---|---|
| `sku` | 商品编号，主键 |
| `stock` | 当前库存量 |
| `version` | **乐观锁版本号**，每次变更 +1 |

`version` 列的作用：让"读-改-写"能检测出并发修改。
`UPDATE ... WHERE version = $expected` 如果影响行数是 0，说明期间有人改过，
需要重读重试。实现见 [services/inventory/repo.py:reserve_optimistic()](../services/inventory/repo.py)。

> **注意**：本项目的**默认路径不用乐观锁**，用的是单条原子 UPDATE。
> 乐观锁是作为对照实现保留的 —— 两种都写出来，才能看清区别。

### 防超卖的那一行 SQL

```sql
UPDATE inventory
   SET stock = stock - $1,
       version = version + 1
 WHERE sku = $2 AND stock >= $1
RETURNING stock
```

**`WHERE stock >= $1` 这半句是整个项目最重要的一行。**
它把"够不够扣"的判断和"扣减"合并成一条语句，
让数据库在同一个原子操作里完成。判断和修改之间没有缝隙。

去掉它（变成先 SELECT 再 UPDATE），并发下就会超卖 ——
[lab1](../labs/lab1_oversell.py) 能亲手复现。

---

## `orders` —— 订单

```sql
CREATE TABLE orders (
    id            BIGINT PRIMARY KEY,
    request_id    TEXT NOT NULL UNIQUE,
    user_id       TEXT NOT NULL,
    sku           TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
```

| 列 | 说明 |
|---|---|
| `id` | 主键。**Snowflake 生成，不是自增** |
| `request_id` | **UNIQUE** ← 幂等的最后一道防线 |
| `user_id` / `sku` / `quantity` | |
| `status` | `PENDING` / `CONFIRMED` / `CANCELLED` |
| `created_at_ms` | 毫秒时间戳 |

### 两个设计点

**① 为什么 `id` 不用自增**：
分库分表后自增会冲突，而且自增 ID 会暴露业务量（用户能猜到你有多少订单）。
Snowflake 生成全局唯一且趋势递增的 64 位整数。见 [common/ids.py](../common/ids.py)。

**② `request_id` 加 UNIQUE 是"最后一道防线"**：

应用层会先查幂等表来判断"这个请求是不是重复的"，但**两个并发请求可能同时
通过检查**，都发现"没处理过"，于是都往下走。

这时唯一约束救场：只有一个能插入成功，另一个拿到唯一冲突，返回已有订单。

**不要依赖"应用层先查一下"来保证唯一性 —— 并发下它一定会漏。**
数据库约束是唯一可靠的守门人。

实现见 [services/order/repo.py](../services/order/repo.py)。

---

# B. 三张幂等表（重点）

这三张表都在做"去重"，但**守的边界完全不同**。
先看一张对比表：

| 表 | 主键 | 守哪个边界 | 防的是什么 |
|---|---|---|---|
| `api_idempotency` | `request_id` | **客户端 → 网关** | 客户端超时重试导致的重复下单 |
| `reservations` | `reservation_id` | **order → inventory** | gRPC 超时重试导致的重复扣库存 |
| `processed_events` | `(event_id, consumer)` | **消息队列 → 消费者** | 消息重复投递导致的重复加积分 |

**为什么要三张而不是一张**：因为重复可能发生在三个不同的位置，
每个位置的"去重键"和"去重范围"都不一样。用一张表反而表达不了。

---

## `api_idempotency` —— HTTP 入口的幂等

```sql
CREATE TABLE api_idempotency (
    request_id    TEXT PRIMARY KEY,
    response      JSONB NOT NULL,
    created_at_ms BIGINT NOT NULL
);
```

**关键：`response` 存的是完整的响应体，不是一个"已处理"标记。**

为什么？因为幂等的语义是"**返回和第一次完全相同的结果**"，
不是"拒绝第二次"。如果只存一个标记，重复请求来的时候你只能说
"处理过了"，但给不出当时的结果。

响应体长这样：

```json
{"order_id": 375706001543794688, "status": "CONFIRMED", "message": ""}
```

重复请求时，直接把这个 JSON 还原成响应返回，`deduplicated` 标记为 `true`。

---

## `reservations` —— 预留记录

```sql
CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    request_id     TEXT NOT NULL,
    sku            TEXT NOT NULL,
    quantity       INTEGER NOT NULL,
    status         TEXT NOT NULL,      -- RESERVED / RELEASED
    created_at_ms  BIGINT NOT NULL
);
CREATE INDEX idx_reservations_request ON reservations(request_id);
```

这张表承担**两个职责**：

**① 支撑 `Reserve` 的幂等**：同一个 `request_id` 重复调用，
直接返回上次的 `reservation_id`，不再扣库存。

**② 支撑 `Release` 的幂等**：`status` 列就是干这个的。

```sql
UPDATE reservations SET status = 'RELEASED'
 WHERE reservation_id = $1 AND status = 'RESERVED'
```

只有第一次调用会命中（影响行数 = 1），后续都是 0，调用方据此跳过库存回补。

**为什么补偿也必须幂等**：补偿动作自己也会被重试。
如果它不幂等，一次失败的订单会把库存加回去**两次**，凭空多出货来。

**为什么要记录 `quantity`**：`Release` 时要用**当初预留的数量**，
而不是调用方现在传的数量 —— 后者可能传错，或者意在"多退一点"。

---

## `processed_events` —— 消费端幂等

```sql
CREATE TABLE processed_events (
    event_id        TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    processed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (event_id, consumer)
);
```

**注意主键是复合的 `(event_id, consumer)`**，不是单独的 `event_id`。

含义：**同一个事件可以被不同消费者各处理一次，但同一个消费者不能重复处理**。

比如"订单已确认"这个事件，可能同时有：
- `points-granter` 消费者 —— 加积分
- `notifier` 消费者 —— 发短信

两个消费者都要处理这个事件，所以 `event_id` 不能单独做主键。
但每个消费者自己不能重复处理，所以加上 `consumer` 组成复合主键。

### 这个去重必须和副作用在同一个事务里

```python
async with conn.transaction():
    claimed = await repo.try_claim_event(conn, event_id, consumer, now)
    if not claimed:
        return                      # 已处理过，跳过
    await repo.add_points(conn, user_id, points)   # 副作用
# 事务提交之后才 XACK

```

**顺序错了会怎样**：如果先提交"认领记录"、再单独做副作用，
中间崩溃就会导致"事件被标记为已处理，但积分从没加过" ——
消息被**永久跳过**，而且没有任何报错。

一起提交则要么都发生、要么都不发生，崩溃后消息重投即可。

---

# C. 异步投递

## `outbox` —— 本地消息表

```sql
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL UNIQUE,
    event_type    TEXT NOT NULL,
    aggregate_id  BIGINT NOT NULL,
    payload       JSONB NOT NULL,
    published     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published = FALSE;
```

| 列 | 说明 |
|---|---|
| `id` | 自增主键，relay 按它顺序投递 |
| `event_id` | 事件唯一标识。消费端幂等靠它 |
| `event_type` | `OrderCreated` / `OrderConfirmed` / `OrderCancelled` |
| `aggregate_id` | 聚合根 ID（这里是 `order_id`） |
| `payload` | 事件内容（jsonb） |
| `published` | 是否已投递到消息队列 |

### 这张表解决什么问题

**你无法把"写数据库"和"发消息"变成一个原子操作。**

```
先写库、后发消息  → 中间崩溃，事件永久丢失，下游永远不知道这笔单
先发消息、后写库  → 下游先收到事件，回头查订单却查不到（幽灵订单）
```

**解法**：不要真的"发消息"，而是把"要发的消息"当作**业务数据**
写进同一个事务。于是"订单已创建"和"事件待发送"要么一起成功、要么一起失败。

然后由 relay 进程（[services/order/relay.py](../services/order/relay.py)）
在事务之外把事件真的投递出去。

**[lab4](../labs/lab4_outbox_twinwrite.py) 能亲手复现这两种损坏和修复。**

### 那个局部索引

```sql
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published = FALSE;
```

这是**局部索引**（partial index）：只覆盖 `published = FALSE` 的行。

relay 每 200ms 轮询一次"未发送的事件"。用普通索引的话，索引会跟着表一起
膨胀 —— 累积 100 万条历史事件后，relay 的每次轮询都要扫一遍大索引。

用局部索引，索引里永远只有几十个条目（未发送的），查询始终是常数级。

### 事件类型的语义

一个成功的订单会产生 **2 条事件**：

| 事件 | 什么时候发 | 含义 |
|---|---|---|
| `OrderCreated` | T1 建单时 | "订单被创建了"（事实发生了） |
| `OrderConfirmed` | Saga 成功后 | "订单最终确认了"（结果确定了） |

失败/取消的订单会发 `OrderCreated` + `OrderCancelled`。

**消费端只订阅终态事件**（`OrderConfirmed` / `OrderCancelled`），
不订阅 `OrderCreated`。

> ⚠️ 这个区分不是可有可无的：开发过程中真实踩过这个坑 ——
> 最初消费端订阅的是 `OrderCreated`，结果**给一个最终被取消的订单加了积分**。
> 是 lab 的断言把这个 bug 抓出来的。
>
> 教训：**"事实事件"和"业务结果事件"不能混用。**

---

## `points` —— 消费者的副作用

```sql
CREATE TABLE points (
    user_id TEXT PRIMARY KEY,
    total   INTEGER NOT NULL DEFAULT 0
);
```

这张表本身没什么技术含量（就是用户积分）。它存在的意义是
**当作"副作用是否被执行了"的证据** —— [lab2](../labs/lab2_dup_consume.py)
就是靠它检测出"重复投递导致重复加分"。

真实项目里这个位置可能是发短信、发优惠券、更新报表 —— 换汤不换药。

---

# 表之间的关系：**没有外键**

你在 `\d` 的输出里会发现一张外键都没有。这是刻意的。

**为什么不用外键**：

1. **微服务原则**：不同服务的数据属于不同服务。跨服务外键在物理上
   就不可能（数据库可能都不在一起）。本项目三个服务共用一个库是简化，
   但设计上按"不共享数据"来。
2. **外键会带来耦合**：加个外键，A 表的数据就成了 B 表写入的前置条件，
   两个服务的发布节奏就被绑住了。
3. **性能**：外键约束在高并发写入时是额外的锁开销。

**那一致性靠什么保证**？

| 场景 | 靠什么 |
|---|---|
| `orders.request_id` 不重复 | UNIQUE 约束 |
| `reservations` 不重复预留 | UNIQUE 约束（`reservation_id` + 应用层查 `request_id`） |
| 库存不为负 | 原子 UPDATE 的 `WHERE stock >= $1` |
| 事件不丢 | Outbox 模式（同事务写入） |
| 事件不重复处理 | `processed_events` 复合主键 |

**核心思想**：一致性来自**约束和原子操作**，而不是来自表之间的引用关系。

---

# 一次下单会写哪些表

把 [1-architecture.md](1-architecture.md) 的时序图和表对应起来：

| Saga 步骤 | 写哪些表 | 在同一个事务里？ |
|---|---|---|
| 幂等检查 | 读 `api_idempotency` | —— |
| **T1** 建订单 | `orders` + `outbox` | ✅ 是（关键） |
| **T2** 预留库存 | `inventory` + `reservations` | ✅ 是 |
| **T3** 支付 | （本地函数，不写表） | —— |
| 终态 | `orders`（改状态）+ `outbox` + `api_idempotency` | ✅ 是 |
| 补偿（失败时） | `reservations`（改状态）+ `inventory`（加回） | ✅ 是 |
| **异步** relay | 读 `outbox` → 写 Redis Stream | —— |
| **异步** consumer | `processed_events` + `points` | ✅ 是 |

**注意到规律了吗**：每一个"必须一起成功"的操作组合，
都放在同一个事务里。这就是为什么事务边界在 `service.py` 而不是 `repo.py` ——
只有 service 知道"哪几条语句必须同生共死"。

---

# 数据生命周期（哪些表会一直长）

生产环境必须考虑这个，否则表会撑爆磁盘。

| 表 | 增长 | 清理策略（生产环境） |
|---|---|---|
| `orders` | 一直长 | 归档历史订单到冷存储 |
| `outbox` | 一直长 | **发送成功后定期删除**（保留 7 天用于排查） |
| `api_idempotency` | 一直长 | **按时间清理**（幂等只需覆盖重试窗口，比如 24 小时） |
| `processed_events` | 一直长 | 同上，按时间清理 |
| `reservations` | 一直长 | 随订单归档 |
| `inventory` | 固定 | —— |
| `points` | 每用户一行 | —— |

**本项目没有实现清理** —— 这是个明确的简化。但你至少要知道
`outbox`、`api_idempotency`、`processed_events` 这三张表在真实系统里
**必须有清理策略**，否则运行几个月就会成为问题。

---

# 常用查询

```powershell
# 表结构

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "\d orders"

# 一致性交叉验证（详见 6-runbook.md）

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "
select
  (select coalesce(sum(quantity),0) from orders where status='CONFIRMED') * 10 as points_expected,
  (select coalesce(sum(total),0) from points)                                as points_actual,
  (select coalesce(sum(quantity),0) from orders where status='CONFIRMED')     as units_sold,
  (select 100000 - stock from inventory where sku='SKU-1')                    as units_deducted;"

# outbox 积压（长期 > 0 说明 relay 有问题）

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "select published, count(*) from outbox group by published;"

# 看一条事件的完整内容

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "select event_type, payload from outbox limit 2;"
```

> **psql 提示**：每条 SQL 必须以**分号**结尾。如果提示符从 `micro#`
> 变成 `micro-#`，那个 `-` 表示 psql 在等你补完上一条语句。
> 清空缓冲区用 `\r`。

---

## 下一步

现在你知道数据存在哪了。接下来看**为什么这么设计** ——
[4-concepts.md](4-concepts.md) 里有 7 条核心结论，以及 CAP、
分布式事务、exactly-once 这些概念的完整解释。
