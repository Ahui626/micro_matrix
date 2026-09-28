"""
[问题]   下单要同时做三件事：建订单、扣库存、支付。它们分属不同服务、
         不同数据库事务，任何一步失败都要把前面的撤销掉。
[本文件] Saga 编排器。T1 建单 → T2 预留库存 → T3 支付，每步都有补偿动作。
         **不 import grpc**，超时预算由传输层算好传进来。
[面试]   - Saga 和 2PC 的区别？
           （2PC 是**同步阻塞**的两阶段提交，要求所有参与者实现 prepare/commit；
             协调者在提交阶段崩溃会让所有人一直占着锁。
             Saga 是**一串本地事务 + 补偿动作**，没有全局锁、没有长时间阻塞，
             代价是只能保证**最终**一致。）
         - Saga 的两种编排方式？
           （编排式 orchestration：一个中心协调者依次调各步并决定补偿 ——
             逻辑集中、好追踪、好调试。本项目采用。
             协同式 choreography：各服务监听事件自行决定下一步 ——
             去中心化、无单点，但流程散落各处，出问题很难还原全貌。
             步骤少用编排式，参与者多用协同式。）
         - 补偿动作失败了怎么办？
           （补偿必须**幂等**且**可重试**，因为它自己也会失败。
             持续失败就需要人工介入 —— 真实系统必须有"待人工处理"队列和告警。
             Saga 不保证一定成功，只保证一直尝试。）
         - ★ Saga 最容易被追问的局限是什么？
           （**没有隔离性。** 中间的 PENDING 状态对外可见，
             别人可能看到"订单存在但库存还没扣"。
             这叫脏读，Saga 天生无法避免 —— 只能靠业务上容忍，
             或者用"语义锁"（比如把订单标成 PENDING 让别的流程跳过它）。
             答不出这一点，说明只记住了 Saga 的形状，没理解它的代价。）
         - "预留库存调用超时了"该怎么办？库存到底扣没扣？
           （**不知道。** 这是 Saga 最棘手的情形，叫"悬挂"。
             保守做法是不要乱补偿（乱补偿会多退库存），
             而是留给对账任务用幂等键去查证。
             见下面 _reserve_inventory 的异常分支。）
[取舍]   支付步骤折叠成 order 进程里的一个本地函数（可注入失败），
         而不是再起一个 payment 微服务。
         理由：多一个进程不增加新知识点，只增加启动和调试成本。
         但要能说出"真实系统里它是独立服务，那时 Saga 的价值才完全体现"。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import asyncpg
import inventory_pb2

from common import breaker, chaos, clients, db, errors, retry, settings
from common.ids import SnowflakeGenerator
from services.order import repo

log = logging.getLogger(__name__)

# 一个进程一个生成器。多实例部署时每个进程的 worker_id 必须不同。
_snowflake = SnowflakeGenerator(worker_id=settings.settings.worker_id)

# 调 inventory 的熔断器。
# 注意层次：熔断器**包在重试外面**。熔断打开时直接拒绝，
# 根本不给重试放大的机会。顺序反了的话，熔断永远拦不住重试产生的流量放大。
_inventory_breaker = breaker.CircuitBreaker(
    name="inventory",
    fail_threshold=settings.settings.breaker_fail_threshold,
    reset_timeout_s=settings.settings.breaker_reset_ms / 1000.0,
)


@dataclass
class CreateOrderResult:
    order_id: int
    status: str
    message: str = ""
    deduplicated: bool = False


def _now_ms() -> int:
    return int(time.time() * 1000)


def breaker_stats() -> dict:
    """暴露给 metrics 用。熔断器没有指标就是黑盒。"""
    return dict(_inventory_breaker.stats)


# ---------------------------------------------------------------------------
# 委托给 inventory 的两个操作
# ---------------------------------------------------------------------------
async def _reserve_inventory(
    request_id: str, sku: str, quantity: int, budget_s: float | None
) -> inventory_pb2.ReserveResponse:
    """调 inventory 预留库存。熔断在外、重试在内。"""
    stub = await clients.inventory_stub()

    async def call():
        return await stub.Reserve(
            inventory_pb2.ReserveRequest(
                request_id=request_id, sku=sku, quantity=quantity
            ),
            timeout=budget_s,
        )

    return await _inventory_breaker.call(
        lambda: retry.call_with_retry(
            call, attempts=3, budget_s=budget_s, label="reserve-stock"
        )
    )


async def _compensate_release(request_id: str, sku: str, quantity: int) -> None:
    """补偿 T2：把预留的库存放回去。

    这里**不抛异常**。理由：补偿失败不应该掩盖原始的失败原因
    （用户该知道"支付失败"，而不是"补偿也失败了"）。
    但它必须被记录 —— 持续失败就需要对账任务和告警介入。

    补偿自身的幂等由 inventory 侧的 reservations.status 保证，
    所以这里可以放心重试。
    """
    stub = await clients.inventory_stub()
    try:
        resp = await stub.Release(
            inventory_pb2.ReleaseRequest(
                request_id=request_id, sku=sku, quantity=quantity
            ),
            timeout=2.0,
        )
        log.info("补偿完成：已释放库存，剩余 %d", resp.remaining)
    except Exception:
        log.exception(
            "★ 补偿失败！request_id=%s —— 库存可能多占了，需要人工/对账介入",
            request_id,
        )


async def _pay(order_id: int, quantity: int) -> None:
    """支付。折叠成本地函数 —— 真实系统里它是独立服务。

    这是 Saga 里最容易失败的一步：卡里没钱、第三方超时、风控拒绝。
    它的失败会触发 T2 的补偿（释放库存）。
    """
    await chaos.delay_if(chaos.PAYMENT_DELAY_MS)
    if chaos.flag(chaos.PAYMENT_FAIL):
        raise errors.PaymentDeclined("chaos: 注入的支付失败")
    log.info("支付成功 order_id=%d 金额=%.2f", order_id, quantity * 9.9)


# ---------------------------------------------------------------------------
async def _finalize_order(
    order_id: int,
    request_id: str,
    status: str,
    message: str,
    now_ms: int,
    *,
    user_id: str,
    sku: str,
    quantity: int,
) -> None:
    """订单进入终态：更新状态 + 发终态事件 + 写幂等响应，三者在同一事务。

    为什么幂等响应要和状态更新在同一事务：否则可能出现
    "订单已 CANCELLED，但幂等表还是空的" —— 重试的请求会从头再走一遍
    Saga，重新扣一次库存。

    ★ 为什么这里要单独发一个"终态事件"，而不是只用 T1 的 OrderCreated：
      消费端（发积分、发通知、对账）关心的是"这单最终成了没有"。
      一个 Saga 中途失败的订单最终是 CANCELLED —— 给这样的订单发积分是错的。
      所以：
          T1 发 OrderCreated    → 记录"事实发生了"（订单确实被创建了）
          终态发 OrderConfirmed / OrderCancelled → 记录"结果确定了"
      消费端只应该监听**终态事件**。这是事件驱动系统里非常容易踩错的一点：
      把"事实事件"当成"业务结果事件"来用。
    """
    event_type = "OrderConfirmed" if status == "CONFIRMED" else "OrderCancelled"

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await repo.update_order_status(conn, order_id, status)

            await repo.write_outbox(
                conn,
                event_type,
                order_id,
                {
                    "order_id": order_id,
                    "request_id": request_id,
                    "user_id": user_id,
                    "sku": sku,
                    "quantity": quantity,
                    "reason": message,
                },
                now_ms,
            )

            await repo.save_api_idempotency(
                conn,
                request_id,
                {"order_id": order_id, "status": status, "message": message},
                now_ms,
            )


# ---------------------------------------------------------------------------
async def create_order(
    request_id: str,
    user_id: str,
    sku: str,
    quantity: int,
    timeout_s: float | None = None,
) -> CreateOrderResult:
    """下单主流程。timeout_s 是留给"调 inventory"的预算，由传输层算好传入。"""
    now_ms = _now_ms()

    # ---- 第 0 步：幂等检查 ----
    async with db.pool().acquire() as conn:
        cached = await repo.find_api_idempotency(conn, request_id)
        if cached is not None:
            log.info("幂等命中（响应回放）request_id=%s", request_id)
            return CreateOrderResult(
                order_id=cached["order_id"],
                status=cached["status"],
                message=cached.get("message", ""),
                deduplicated=True,
            )

        existing = await repo.find_order_by_request(conn, request_id)
        if existing is not None:
            # 走到这里说明订单建了但 Saga 没走完（进程崩溃过），
            # 且幂等表还没写入。真实系统需要**对账任务**扫描这类
            # 长时间停在 PENDING 的订单并推进或补偿。
            log.warning(
                "订单已存在但无幂等记录 request_id=%s status=%s —— 上次可能中途崩溃",
                request_id, existing["status"],
            )
            return CreateOrderResult(
                order_id=existing["id"], status=existing["status"],
                message="此前已受理，状态待对账", deduplicated=True,
            )

    # ---- T1：建订单 + 写 outbox（同一个事务）----
    order_id = _snowflake.next_id()
    write_event = not chaos.flag(chaos.SKIP_OUTBOX_WRITE)

    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await repo.insert_order_with_outbox(
                    conn,
                    order_id=order_id,
                    request_id=request_id,
                    user_id=user_id,
                    sku=sku,
                    quantity=quantity,
                    now_ms=now_ms,
                    write_event=write_event,
                )
    except asyncpg.UniqueViolationError:
        # 并发重复请求：另一个请求刚刚建了同一个 request_id 的订单。
        # 唯一约束替我们挡住了 —— 这就是"不依赖应用层检查"的意义。
        async with db.pool().acquire() as conn:
            existing = await repo.find_order_by_request(conn, request_id)
        log.info("并发重复请求被唯一约束拦截 request_id=%s", request_id)
        return CreateOrderResult(
            order_id=existing["id"] if existing else 0,
            status=existing["status"] if existing else "UNKNOWN",
            message="并发重复请求",
            deduplicated=True,
        )

    log.info("T1 完成：订单已创建 order_id=%d（含 outbox 事件）", order_id)

    # 注入点：写完订单后立刻崩溃。
    # 因为 outbox 和订单在同一事务里，重启后 relay 会把事件补发出去 ——
    # 这正是本地消息表模式要证明的事。
    await chaos.crash_if(chaos.FAIL_AFTER_ORDER_INSERT)

    # ---- T2：预留库存 ----
    try:
        reserved = await _reserve_inventory(request_id, sku, quantity, timeout_s)
    except Exception as exc:
        # ★ Saga 最棘手的情形：**不知道库存到底扣没扣**。
        # 超时 ≠ 没执行 —— 服务端可能已经扣了，只是响应没回来。
        # 这里选择**不补偿**：乱补偿会凭空多出库存，比少卖更糟。
        # 正确做法是把这个 request_id 交给对账任务，用幂等键去查证真实状态。
        log.exception("预留库存调用失败（结果未知，悬挂）order_id=%d", order_id)
        raise errors.UpstreamUnavailable(f"预留库存失败: {exc}") from exc

    if not reserved.ok:
        # 明确的业务失败（库存不足）—— 可以安全补偿 T1。
        log.info("T2 业务失败 order_id=%d reason=%s", order_id, reserved.reason)
        await _finalize_order(
            order_id, request_id, "CANCELLED", f"库存不足({reserved.reason})", _now_ms(),
            user_id=user_id, sku=sku, quantity=quantity,
        )
        return CreateOrderResult(order_id=order_id, status="CANCELLED", message=reserved.reason)

    log.info("T2 完成：库存已预留 order_id=%d 剩余=%d", order_id, reserved.remaining)

    # ---- T3：支付 ----
    try:
        await _pay(order_id, quantity)
    except errors.BusinessError as exc:
        # T3 失败 → 逆序补偿：先补偿 T2（释放库存），再补偿 T1（取消订单）。
        log.warning("T3 支付失败 order_id=%d，启动逆向补偿", order_id)
        await _compensate_release(request_id, sku, quantity)
        await _finalize_order(
            order_id, request_id, "CANCELLED", f"支付失败({exc.message})", _now_ms(),
            user_id=user_id, sku=sku, quantity=quantity,
        )
        return CreateOrderResult(order_id=order_id, status="CANCELLED", message=exc.message)

    # ---- 全部成功 ----
    await _finalize_order(
        order_id, request_id, "CONFIRMED", "", _now_ms(),
        user_id=user_id, sku=sku, quantity=quantity,
    )
    log.info("Saga 全部完成 order_id=%d status=CONFIRMED", order_id)
    return CreateOrderResult(order_id=order_id, status="CONFIRMED")


# ---------------------------------------------------------------------------
async def get_order(order_id: int) -> dict | None:
    async with db.pool().acquire() as conn:
        return await repo.get_order(conn, order_id)
