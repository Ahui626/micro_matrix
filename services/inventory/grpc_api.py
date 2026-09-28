"""
[问题]   gRPC 的请求消息怎么变成业务调用，业务结果怎么变回响应消息？
[本文件] 传输层适配器。只做三件事：解析请求 → 调 service → 映射结果与错误。
         这一层**不含任何业务判断** —— 不该出现 if 库存不足 then ... 这类逻辑。
[面试]   - 为什么这一层必须"薄"？
           （它是最难测试的部分：要起 server、要构造 proto 消息。
             业务逻辑留在这一层，就意味着业务逻辑很难测。
             把判断都下沉到 service，这层就退化成纯映射，一眼能看完。）
         - 服务端怎么知道调用方还剩多少时间？
           （`context.time_remaining()`。它返回的是**逐跳扣减后**的剩余值，
             不是调用方最初的设定值。见下面的日志。）
         - 服务端处理超时了该怎么办？
           （正常返回 —— 客户端已经因为 DEADLINE_EXCEEDED 走人了，
             这时候再 abort 也没有接收方。但**副作用该回滚的要回滚**：
             这就是为什么订单链路需要 Saga 补偿。）
[取舍]   数据库故障被映射成 UNAVAILABLE（可重试），而不是让它以 UNKNOWN 冒出去。
         理由：UNAVAILABLE 会触发上层重试和熔断，UNKNOWN 不会。
         把"可重试的系统故障"正确表达出来，是这一层最有价值的职责。
"""

from __future__ import annotations

import logging

import asyncpg
import grpc
import inventory_pb2
import inventory_pb2_grpc

from common import chaos, trace
from services.inventory import service

log = logging.getLogger(__name__)


async def _inject_faults(context: grpc.aio.ServicerContext) -> None:
    """故障注入。正常运行时不产生任何行为。"""
    await chaos.delay_if(chaos.INVENTORY_DELAY_MS)

    if chaos.should_fail_randomly(chaos.INVENTORY_ERROR_RATE):
        log.warning("★ 注入随机故障（INVENTORY_ERROR_RATE）")
        # 用 UNAVAILABLE 而不是 FAILED_PRECONDITION：
        # 前者是"可重试的系统故障"，会触发上层的重试和熔断逻辑，
        # 这正是 lab5（重试风暴）要观察的行为。
        # 注意 await —— grpc.aio 的 abort 是协程，不 await 会静默失效
        # （见 common/errors.py 里 abort 的详细说明）
        await context.abort(grpc.StatusCode.UNAVAILABLE, "chaos: 注入的随机故障")


class InventoryServicer(inventory_pb2_grpc.InventoryServiceServicer):
    """每个方法都是同一个套路：记日志 → 注入故障 → 调 service → 转成 proto。"""

    async def Reserve(self, request, context):
        remaining_ms = context.time_remaining()
        log.info(
            "Reserve 收到 sku=%s qty=%d request_id=%s | trace=%s | 剩余预算=%s",
            request.sku, request.quantity, request.request_id[:8],
            trace.current_trace_id()[:8],
            # 这个值是逐跳扣减后的结果，不是调用方的原始设定。
            # 如果上游设了 3000ms，到这里可能只剩 2800ms —— 这就是 deadline 传播。
            f"{remaining_ms * 1000:.0f}ms" if remaining_ms is not None else "无（危险）",
        )

        await _inject_faults(context)

        try:
            result = await service.reserve(
                request_id=request.request_id,
                sku=request.sku,
                quantity=request.quantity,
            )
        except asyncpg.PostgresError as exc:
            log.exception("Postgres 故障，映射为 UNAVAILABLE（可重试）")
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"数据库不可用: {type(exc).__name__}")

        # 幂等命中时也走这里 —— 状态码是 OK，语义是"返回和第一次相同的结果"。
        return inventory_pb2.ReserveResponse(
            ok=result.ok,
            remaining=result.remaining,
            reason=result.reason,
            reservation_id=result.reservation_id,
        )

    async def Release(self, request, context):
        log.info(
            "Release 收到 sku=%s qty=%d request_id=%s",
            request.sku, request.quantity, request.request_id[:8],
        )
        await _inject_faults(context)

        try:
            result = await service.release(
                request_id=request.request_id,
                sku=request.sku,
                quantity=request.quantity,
            )
        except asyncpg.PostgresError as exc:
            log.exception("Postgres 故障，映射为 UNAVAILABLE（可重试）")
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"数据库不可用: {type(exc).__name__}")

        return inventory_pb2.ReleaseResponse(ok=result.ok, remaining=result.remaining)

    async def GetStock(self, request, context):
        stock = await service.get_stock(request.sku)
        if stock is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"SKU 不存在: {request.sku}")
        return inventory_pb2.Stock(
            sku=stock["sku"], available=stock["stock"], version=stock["version"]
        )
