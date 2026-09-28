# 5. 面试题 → 代码 反向索引

> ⚠️ **这是一本字典，不要通读，也不要用它入门。**
>
> 它把 40 个源文件里的 `[面试]` 注释抽出来，按主题排好了 —— 一共 100 多条，
> 通读只会让人失去重点。
>
> **它在你学习路线里的位置**：
>
> | 阶段 | 该读什么 |
> |---|---|
> | 还不了解这个项目 | [1-architecture.md](1-architecture.md) → [2-api.md](2-api.md) → [3-data-model.md](3-data-model.md) |
> | 想知道核心结论 | [4-concepts.md](4-concepts.md) 开头的「7 件事」 |
> | 读代码时 | README 的「读代码：6 组文件」 |
> | **学完之后 / 面试前** | **本文件** —— 按主题扫一遍查漏，或按题查代码 |
>
> 如果你觉得这些条目太碎 —— 它们确实很碎，这是**有意的**：
> 它是索引，不是教材。

★ 标记的是高频且容易答错的。

---

## 一、并发与竞态

| 问题 | 答案在哪 |
|---|---|
| ★ 为什么"先 SELECT 再 UPDATE"会超卖？窗口在哪？ | [inventory/repo.py:reserve_naive](../services/inventory/repo.py) · [lab1](../labs/lab1_oversell.py) |
| 为什么不能用 Python 的锁（threading.Lock / asyncio.Lock）？ | [inventory/repo.py](../services/inventory/repo.py) · [lab1 case 4](../labs/lab1_oversell.py) |
| 为什么 SQL 里要写 `WHERE stock >= $1`？ | [inventory/repo.py:reserve_atomic](../services/inventory/repo.py) |
| 乐观锁和单条原子 UPDATE 有什么区别？该用哪个？ | [inventory/repo.py:reserve_optimistic](../services/inventory/repo.py) |
| 事务隔离级别：Postgres 默认 Read Committed 能防住什么？ | [common/db.py](../common/db.py) |
| ★ 为什么"串行循环发 100 次"不是并发测试？ | [labs/loadgen.py](../labs/loadgen.py) |
| 为什么要用 Semaphore 限制并发度？ | [labs/loadgen.py](../labs/loadgen.py) |

**一句话答案**：超卖的根因不是"并发"，是"判断和修改之间存在时间窗口"。
在 asyncio 里这个窗口就是 **await 点**。修法是让数据库保证原子性，
而不是加 Python 锁（只能锁住自己的进程）。

---

## 二、幂等

| 问题 | 答案在哪 |
|---|---|
| 幂等键为什么必须由客户端生成？ | [inventory/service.py](../services/inventory/service.py) |
| 为什么 `orders.request_id` 要加唯一约束？ | [order/repo.py](../services/order/repo.py) |
| 为什么"扣库存"和"写预留记录"必须同事务？ | [inventory/service.py](../services/inventory/service.py) |
| 补偿操作为什么也必须幂等？ | [inventory/service.py:release](../services/inventory/service.py) |
| 幂等和副作用为什么必须在同一个事务里？ | [order/repo.py:try_claim_event](../services/order/repo.py) |
| ★ 那 exactly-once 呢？ | [lab2](../labs/lab2_dup_consume.py) |

**一句话答案**：exactly-once 在跨系统投递里基本做不到
（"处理"和"确认"分属你的数据库和消息队列，无法原子完成）。
工程上的正确答案是 **at-least-once + 消费端幂等 = effectively-once**。
主动说出这句话，比被追问出来强得多。

---

## 三、分布式事务与一致性

| 问题 | 答案在哪 |
|---|---|
| ★ Saga 和 2PC 的区别？ | [order/service.py](../services/order/service.py) |
| Saga 的两种编排方式（orchestration vs choreography）？ | [order/service.py](../services/order/service.py) |
| ★ Saga 最容易被追问的局限是什么？ | [order/service.py](../services/order/service.py) |
| 补偿动作失败了怎么办？ | [order/service.py:_compensate_release](../services/order/service.py) |
| ★ "预留库存调用超时了"——库存到底扣没扣？ | [order/service.py](../services/order/service.py) |
| ★ 为什么"先写库再发消息"和"先发消息再写库"都不行？ | [lab4](../labs/lab4_outbox_twinwrite.py) |
| ★ 本地消息表（Outbox）是怎么解决的？ | [order/repo.py:insert_order_with_outbox](../services/order/repo.py) |
| 这个模式的代价是什么？ | [lab4 结论](../labs/lab4_outbox_twinwrite.py) |
| 完整分类：2PC / TCC / Saga / 本地消息表 / 最大努力通知 | [4-concepts.md](4-concepts.md) |

**必须能答出的一条**：Saga **没有隔离性**。中间状态（PENDING）对外可见，
别人可能看到"订单存在但库存还没扣"。这叫脏读，Saga 天生无法避免。
答不出这一点，说明只记住了 Saga 的形状，没理解它的代价。

---

## 四、消息与投递语义

| 问题 | 答案在哪 |
|---|---|
| ★ 为什么 at-least-once 是"必然重复"而不是"偶尔重复"？ | [lab2](../labs/lab2_dup_consume.py) |
| ★ XACK 的时机为什么决定了投递语义？ | [order/consumer.py](../services/order/consumer.py) |
| ★ 为什么中继只能做到 at-least-once？ | [order/relay.py](../services/order/relay.py) |
| XAUTOCLAIM 是干什么的？ | [order/consumer.py:_reclaim_stale](../services/order/consumer.py) |
| 为什么要死信队列？直接无限重试不行吗？ | [order/consumer.py](../services/order/consumer.py) |
| 多个 relay 实例同时跑会怎样？（SKIP LOCKED） | [order/repo.py:fetch_unpublished](../services/order/repo.py) |
| 为什么用轮询，不用数据库触发器或 CDC？ | [order/relay.py](../services/order/relay.py) |
| Redis Streams 和 Kafka 有什么区别？ | [common/redisx.py](../common/redisx.py) |
| 消息顺序性 / 消费者组 / rebalance | [4-concepts.md](4-concepts.md) |

---

## 五、服务通信（gRPC 与 deadline）

| 问题 | 答案在哪 |
|---|---|
| gRPC 的 channel 为什么必须复用？ | [common/clients.py](../common/clients.py) |
| ★ deadline 和 timeout 的本质区别？ | [common/clients.py](../common/clients.py) |
| ★ 子调用的超时该怎么算？为什么不能用固定的 3 秒？ | [common/clients.py:child_timeout](../common/clients.py) |
| ★ 服务的超时预算该谁算？ | [order/grpc_api.py](../services/order/grpc_api.py) |
| 为什么要给下游的预算小于自己剩余的预算？ | [common/clients.py](../common/clients.py) |
| 为什么这一层（grpc_api）必须"薄"？ | [inventory/grpc_api.py](../services/inventory/grpc_api.py) |
| 服务端怎么知道调用方还剩多少时间？ | [inventory/grpc_api.py](../services/inventory/grpc_api.py) |
| 服务端处理超时了该怎么办？ | [inventory/grpc_api.py](../services/inventory/grpc_api.py) |
| gRPC 状态码里哪些可重试、哪些绝对不能重试？ | [common/errors.py](../common/errors.py) |
| 为什么"库存不足"不该用错误码返回？ | [proto/inventory.proto](../proto/inventory.proto) |
| protobuf 字段编号为什么不能随便改？reserved 干什么？ | [proto/order.proto](../proto/order.proto) |

**可以在日志里看到的证据**：一次请求的预算会逐跳收紧 ——
gateway 3000ms → order 2998ms → inventory 1008ms。

---

## 六、弹性：重试 / 熔断 / 限流 / 锁

| 问题 | 答案在哪 |
|---|---|
| ★ 一次安全的重试必须满足哪三个条件？ | [common/retry.py](../common/retry.py) · [lab5](../labs/lab5_retry_storm.py) |
| 抖动为什么必须有？（用数字说话） | [lab5 C 部分](../labs/lab5_retry_storm.py) |
| 重试次数和 deadline 是什么关系？ | [common/retry.py:call_with_retry](../common/retry.py) |
| 重试和熔断该谁套谁？ | [common/breaker.py](../common/breaker.py) · [order/service.py](../services/order/service.py) |
| 多层重试叠加会怎样？ | [lab5 A 部分](../labs/lab5_retry_storm.py) |
| 熔断器的三个状态？HALF_OPEN 为什么只放一个请求？ | [common/breaker.py](../common/breaker.py) |
| 熔断和限流的区别？ | [common/breaker.py](../common/breaker.py) |
| 熔断打开时该返回什么？ | [common/breaker.py](../common/breaker.py) |
| 四种限流算法的区别？ | [common/ratelimit.py](../common/ratelimit.py) |
| 令牌桶为什么能容忍突发？ | [common/ratelimit.py](../common/ratelimit.py) |
| 为什么限流必须用 Lua？ | [common/ratelimit.py](../common/ratelimit.py) |
| 限流 key 该用什么维度？ | [common/ratelimit.py](../common/ratelimit.py) |
| ★ 锁的 TTL 设多久？设错了会怎样？ | [common/lock.py](../common/lock.py) · [lab3](../labs/lab3_lock_fencing.py) |
| SET NX PX 和 SETNX + EXPIRE 的区别？ | [common/lock.py](../common/lock.py) |
| 释放锁为什么要校验持有者？校验了就安全了吗？ | [common/lock.py:release](../common/lock.py) |
| ★★ fencing token 为什么才是真正的解法？ | [lab3 C 部分](../labs/lab3_lock_fencing.py) |
| Redlock 到底行不行？ | [4-concepts.md](4-concepts.md) |

**最有分量的一句**：锁的过期**不是**一个正确性机制。
正确性必须来自被保护的资源本身（fencing token），或者干脆不用锁
—— 本项目除 lab3 外**没有任何地方用分布式锁**，这本身就是一个可讲的点。

---

## 七、数据层

| 问题 | 答案在哪 |
|---|---|
| 为什么异步服务里不能用同步数据库驱动？ | [common/db.py](../common/db.py) |
| 连接池该设多大？ | [common/db.py](../common/db.py) |
| ★ 数据库约束为什么是最后一道防线？ | [order/repo.py](../services/order/repo.py) |
| 为什么用裸 SQL 而不是 ORM？ | [common/db.py](../common/db.py) |
| MVCC / 幻读 / 隔离级别 | [4-concepts.md](4-concepts.md) |

---

## 八、分布式 ID（Snowflake）

| 问题 | 答案在哪 |
|---|---|
| 64 位是怎么分配的？为什么时间戳放高位？ | [common/ids.py](../common/ids.py) |
| ★ 时钟回拨会怎样？怎么处理？ | [common/ids.py:next_id](../common/ids.py) |
| 为什么不用 UUID 当主键？ | [common/ids.py](../common/ids.py) |
| worker_id 冲突会发生什么？ | [common/ids.py](../common/ids.py) |

---

## 九、链路追踪与可观测性

| 问题 | 答案在哪 |
|---|---|
| ★ 为什么不能只在 `intercept_service` 里 set 一下就完事？ | [common/trace.py](../common/trace.py) |
| 为什么用 contextvars 而不是全局变量或 threading.local？ | [common/trace.py](../common/trace.py) |
| trace_id 是怎么跨进程传递的？ | [common/trace.py](../common/trace.py) |
| OpenTelemetry 帮你做了什么？ | [4-concepts.md](4-concepts.md) |
| 可观测性的三大支柱是什么？ | [common/metrics.py](../common/metrics.py) |
| ★ 为什么看 p99 而不是平均值？ | [common/metrics.py](../common/metrics.py) |
| ★ 用请求路径当指标标签会有什么问题？（高基数） | [gateway/http_api.py](../services/gateway/http_api.py) |
| trace_id 为什么要由格式化器自动注入？ | [common/logging.py](../common/logging.py) |

**同类陷阱**：`common/trace.py` 的 gRPC 拦截器坑，
和 `gateway/http_api.py` 里 BaseHTTPMiddleware 的坑，是**同一类问题**——
都是没想清楚"这段代码跑在哪个上下文作用域里"。
两个都答得出来，说明你真的理解了 contextvars。

---

## 十、Python / asyncio 特有

| 问题 | 答案在哪 |
|---|---|
| Windows 上 asyncio 的默认事件循环是哪个？为什么不该手动改？ | [scripts/smoke.py](../scripts/smoke.py) |
| 为什么 asyncpg 比 psycopg2 更适合 asyncio 服务？ | [scripts/smoke.py](../scripts/smoke.py) |
| ★ `context.abort()` 在 grpc.aio 里为什么必须 await？ | [common/errors.py:abort](../common/errors.py) |
| 为什么业务层不能依赖传输层？ | [inventory/service.py](../services/inventory/service.py) |
| 为什么传进来的是"协程工厂"而不是"协程"？ | [common/retry.py](../common/retry.py) |
| free-threaded 构建（cp314t）和标准构建差在哪？ | [scripts/smoke.py](../scripts/smoke.py) |
| GIL / 阻塞驱动 / uvloop | [4-concepts.md](4-concepts.md) |

**一个真实的坑**（项目里踩过）：
`grpc.aio` 的 `ServicerContext.abort` 是 `async def`，必须 `await`。
同步版 grpc 的 abort 是普通函数，所以大量教程写成 `context.abort(...)`。
在 aio 里这样写**不会报错** —— 它只创建了一个永不 await 的协程，
abort 静默失效，handler 继续返回**正常响应**。本该报错的地方静默成功了。

---

## 十一、配置、生命周期与工程实践

| 问题 | 答案在哪 |
|---|---|
| 为什么配置要集中读取？ | [common/settings.py](../common/settings.py) |
| 优雅停机的正确顺序是什么？ | [common/lifecycle.py](../common/lifecycle.py) |
| Windows 上为什么不能用 `loop.add_signal_handler()`？ | [common/lifecycle.py](../common/lifecycle.py) |
| 为什么信号处理器里不直接做异步操作？ | [common/lifecycle.py](../common/lifecycle.py) |
| ★ Windows 上为什么不能用 multiprocessing 启动服务？ | [scripts/run_all.py](../scripts/run_all.py) |
| ★ Windows 上怎么"优雅地"通知另一个进程退出？ | [scripts/run_all.py](../scripts/run_all.py) |
| 为什么不能"启动后 sleep 3 秒就算就绪"？ | [scripts/run_all.py](../scripts/run_all.py) |
| 后台任务为什么要绑定 stop_event，而不是直接 cancel？ | [order/main.py](../services/order/main.py) |
| 后台任务异常退出了该怎么办？ | [order/main.py](../services/order/main.py) |
| 网关层的职责边界在哪？ | [gateway/http_api.py](../services/gateway/http_api.py) |
| 为什么生成代码必须和业务代码物理隔离？ | [scripts/gen_proto.py](../scripts/gen_proto.py) |
| 为什么 `_pb2_grpc.py` 里是绝对导入？ | [bootstrap.py](../bootstrap.py) |
| 混沌工程的核心原则？故障注入为什么要确定性？ | [common/chaos.py](../common/chaos.py) |

---

## 十二、服务发现与部署编排

| 问题 | 答案在哪 |
|---|---|
| 注册中心的四个核心动作是什么？ | [common/discovery.py](../common/discovery.py) |
| TTL 健康检查和"注册中心主动探活"的区别？ | [common/discovery.py](../common/discovery.py) |
| 服务挂了注册中心怎么把它摘掉？ | [common/discovery.py](../common/discovery.py) |
| 注册中心是 CP 还是 AP？ | [common/discovery.py](../common/discovery.py) |
| ★ 客户端为什么要缓存解析结果？注销为什么不是立即生效？ | [common/discovery.py](../common/discovery.py) · [7-deployment.md](7-deployment.md) |
| ★ 注册中心式发现 vs DNS 式发现？K8s 里为什么不用 Consul？ | [7-deployment.md](7-deployment.md) |
| ★ 绑定地址和广播地址什么时候会不一样？ | [7-deployment.md](7-deployment.md) · [deploy/k8s/01-configmap.yaml](../deploy/k8s/01-configmap.yaml) |
| gRPC 服务为什么不能用 httpGet 探针？ | [scripts/grpc_probe.py](../scripts/grpc_probe.py) |
| readiness 和 liveness 怎么配？liveness 为什么不能查依赖？ | [deploy/k8s/04-gateway.yaml](../deploy/k8s/04-gateway.yaml) · [4-concepts.md](4-concepts.md) |
| Dockerfile 的 COPY 顺序为什么重要？三个服务为什么共用一个镜像？ | [deploy/Dockerfile](../deploy/Dockerfile) |
| Job 和 Deployment 的区别？播种为什么用 Job？ | [deploy/k8s/05-seed-job.yaml](../deploy/k8s/05-seed-job.yaml) |
| Docker 和 K8s 的分工？K8s 管什么、Docker 管什么？ | [8-ecosystem.md](8-ecosystem.md) |
| 本机 `docker build` 的镜像，为什么 K8s 节点拉不到？ | [8-ecosystem.md](8-ecosystem.md) · [7-deployment.md](7-deployment.md) |
| 镜像仓库 / 镜像加速源 / 私有仓库分别是什么？ | [8-ecosystem.md](8-ecosystem.md) |
| Pod 起不来 / 反复重启，怎么排查？ | [7-deployment.md](7-deployment.md) |

---

## 十三、另外三条

上面各节是分主题的索引。如果要背，背 [README 的「7 件事」](../README.md)，
那是主线。这里只补两条不属于那 7 条、但同样常被问的：

1. **deadline 是逐跳收紧的。** 子调用的超时必须小于自己的剩余时间 ——
   否则超时返回时，你连「记录这次失败」都来不及做。
   → [common/clients.py](../common/clients.py)

2. **顺序很重要：熔断在外、重试在内。** 反过来的话，
   熔断永远拦不住重试产生的放大。
   → [services/order/service.py](../services/order/service.py)

3. **最危险的是「以为有保护，其实没有」。** 比如没 `await` 的 `abort`、
   没生效的 trace 拦截器、加了却不起作用的锁。
   → [4-concepts.md §13](4-concepts.md)

判断标准：**每个机制上线前都要能演示它真的在起作用。**
