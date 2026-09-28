"""
[问题]   gRPC 请求怎么变成 Saga 调用，结果怎么变回响应。
[本文件] 传输层适配器。**唯一**在这里做的事：算超时预算、调 service、映射错误。
[面试]   - ★ 服务的超时预算该谁算？
           （传输层。因为只有它拿得到 `context.time_remaining()` ——
             也就是"整条链路分给这次调用还剩多少"。
             service 层拿到的是一个算好的秒数，它不需要知道 gRPC 的存在。
             这就是"业务逻辑不依赖传输层"的具体好处。）
         - 为什么要给下游的预算小于自己剩余的预算？
           （见 common/clients.child_timeout。给自己留一点收尾时间，
             否则超时返回时你连"记录这次失败"都来不及做。）
[取舍]   返回响应前重新查一次数据库拿完整订单，而不是让 service 拼装。
         多一次查询，换来"响应里的字段和数据库完全一致"。
         少这一次查询就得让 service 返回一个和表结构耦合的结构体 ——
         一旦表加字段就要同步改两处。清晰优先。
"""

from __future__ import annotations

import logging

import grpc
import order_pb2
import order_pb2_grpc

from common import clients, errors, settings, trace

from services.order import service

log = logging.getLogger(__name__)


def _to_proto(row: dict) -> order_pb2.Order:
    return order_pb2.Order(
        id=row["id"],
        request_id=row["request_id"],
        user_id=row["user_id"],
        sku=row["sku"],
        quantity=row["quantity"],
        status=row["status"],
        created_at_ms=row["created_at_ms"],
    )


class OrderServicer(order_pb2_grpc.OrderServiceServicer):
    async def CreateOrder(self, request, context):
        remaining_ms = context.time_remaining()
        log.info(
            "CreateOrder 收到 request_id=%s user=%s sku=%s qty=%d | 剩余预算=%s",
            request.request_id[:8], request.user_id, request.sku, request.quantity,
            f"{remaining_ms * 1000:.0f}ms" if remaining_ms is not None else "无（危险）",
        )

        # ★ 在这里、也只有在这里，能拿到"整条链路还剩多少时间"。
        # 把它换算成给下游的预算，deadline 就是这样逐跳收紧的。
        child_budget_s = clients.child_timeout(
            context, settings.settings.order_reserve_timeout_ms
        )
        log.debug(
            "分配给下游 inventory 的预算：%.0fms（本层剩余 %.0fms）",
            child_budget_s * 1000, (remaining_ms or 0) * 1000,
        )

        try:
            result = await service.create_order(
                request_id=request.request_id,
                user_id=request.user_id,
                sku=request.sku,
                quantity=request.quantity,
                timeout_s=child_budget_s,
            )
        except errors.BusinessError as exc:
            # 带语义的业务异常 → 正确的 gRPC 状态码。
            # 上层据此决定该不该重试（见 common/errors.py 的白名单）。
            log.warning("CreateOrder 业务失败：%s", exc.message)
            await errors.abort(context, exc)

        row = await service.get_order(result.order_id)
        if row is None:
            # 理论上不该发生：订单刚创建成功却查不到。
            # 真发生了说明有更严重的问题（比如连到了不同的库），应当显式暴露。
            log.error("订单创建后查询不到 order_id=%d", result.order_id)
            await context.abort(grpc.StatusCode.INTERNAL, "订单状态异常")

        return order_pb2.CreateOrderResponse(
            order=_to_proto(row),
            deduplicated=result.deduplicated,
        )

    async def GetOrder(self, request, context):
        log.info("GetOrder 收到 id=%d trace=%s", request.id, trace.current_trace_id()[:8])
        row = await service.get_order(request.id)
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"订单不存在: {request.id}")
        return _to_proto(row)
