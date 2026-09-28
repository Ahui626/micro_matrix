"""
[问题]   order 要调 inventory。连接怎么建、trace_id 怎么带过去、
         超时给多少、下游挂了怎么办——这些不该散落在每个业务函数里。
[本文件] gRPC channel 的创建与复用、stub 工厂、以及子调用超时的计算方式。
[面试]   - gRPC 的 channel 为什么必须复用，不能每次调用新建？
           （每次新建 = 每次重新做 TCP 握手 + HTTP/2 设置帧 + TLS。
             高频调用下这部分开销会远超业务本身。而且每个 channel
             都会占用连接和线程资源，不复用会快速耗尽。）
         - deadline 和 timeout 的本质区别？
           （timeout 是"我这次愿意等多久"的相对值；deadline 是一个绝对时间点，
             它会随请求一起传下去，**每一跳自动扣减已消耗的时间**。
             所以链路末端看到的剩余时间，天然就是"整条链路还剩多少"。）
         - 子调用的超时该怎么算？为什么不能用固定的 3 秒？
           （固定值会和上游预算脱节：上游只剩 100ms 了，你还给下游 3 秒，
             结果必然是上游先超时，下游还在白干活。必须逐跳收紧。）
         - 重试要满足哪些条件才不会变成重试风暴？（见 common/retry.py）
[取舍]   手写 channel 缓存而不是用连接池库。
         代价：没有健康检查、没有重新连接策略。
         收益：只有 30 行，且能看到"channel 是长连接"这个事实。
         生产环境应该用 grpc 自带的 keepalive 选项 + 重连退避。
"""

from __future__ import annotations

import logging

import grpc

from common import discovery, settings, trace

log = logging.getLogger(__name__)

# 给"超时之后的收尾工作"预留的缓冲（秒）。
#
# 为什么必须有这个预留：假设 order 把全部剩余预算都给了 inventory。
# inventory 超时返回的那一刻，order 自己的 deadline 也到了 ——
# 它连"把订单标记为失败"这个操作都来不及做完。
# 留一点缓冲，保证调用方在超时后还有时间做有序的收尾和补偿。
DEADLINE_RESERVE_S = 0.05

# channel 缓存。key 是目标地址。
# 一个进程对每个下游只需要一个 channel —— HTTP/2 本身支持多路复用，
# 所有并发请求共用一条连接就够了。
_channels: dict[str, grpc.aio.Channel] = {}


def channel(addr: str) -> grpc.aio.Channel:
    """拿到（或创建）到某个地址的 channel。

    这里挂了 TraceClientInterceptor —— 于是"把 trace_id 放进 metadata"
    这件事对业务代码完全透明。业务函数只管发请求，不需要知道追踪的存在。
    """
    existing = _channels.get(addr)
    if existing is not None:
        return existing

    ch = grpc.aio.insecure_channel(
        addr,
        interceptors=[trace.TraceClientInterceptor()],
        options=[
            # 断线时自动重连的退避参数。不设的话默认行为比较激进，
            # 下游重启期间会产生大量无谓的重连尝试。
            ("grpc.initial_reconnect_backoff_ms", 200),
            ("grpc.max_reconnect_backoff_ms", 5000),
            # 心跳：长时间空闲的连接会被中间设备（NAT、负载均衡）悄悄掐断，
            # 而两端都以为连接还在。keepalive 让这个问题被提前发现。
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
        ],
    )
    _channels[addr] = ch
    log.info("建立 gRPC channel → %s", addr)
    return ch


async def close_all() -> None:
    """优雅停机时关闭所有 channel。"""
    for addr, ch in _channels.items():
        await ch.close()
        log.debug("已关闭 channel → %s", addr)
    _channels.clear()


# ---------------------------------------------------------------------------
# 子调用超时的计算
# ---------------------------------------------------------------------------
def remaining_s(context: grpc.aio.ServicerContext) -> float | None:
    """当前这次服务端调用还剩多少时间（秒）。

    返回 None 表示调用方**没有设置 deadline** —— 这是很危险的状态，
    意味着这次调用可以无限期挂住。生产环境的服务端应该主动拒绝
    没有 deadline 的请求，否则一个卡住的下游会耗尽你的工作线程。
    """
    return context.time_remaining()


def child_timeout(context: grpc.aio.ServicerContext, cap_ms: int) -> float:
    """算出给下游调用的超时（秒）。

    取两者中更小的那个：
      1. cap_ms —— 业务上允许的等待上限（比如"扣库存不该超过 1 秒"）
      2. 当前剩余时间 - 预留缓冲 —— 物理上还剩多少

    这就是 deadline "逐跳收紧"的实现。
    gateway 设 3 秒 → order 收到时可能只剩 2.9 秒 →
    order 调 inventory 最多给 2.85 秒 → inventory 看到自己的 deadline
    也是 2.85 秒。整条链路上没有任何一跳能超出总预算。
    """
    cap_s = cap_ms / 1000.0
    left = context.time_remaining()
    if left is None:
        # 上游没设 deadline，只能用业务上限兜底。
        log.warning("上游未设置 deadline，本次调用只能靠业务上限 %dms 保护", cap_ms)
        return cap_s
    return max(0.001, min(cap_s, left - DEADLINE_RESERVE_S))


# ---------------------------------------------------------------------------
# stub 工厂
#
# 生成的 stub 很轻（只是包了一层 channel），所以每次新建也没问题。
# 真正需要复用、也真正昂贵的是它背后的 channel。
#
# 这里是 async def：因为要先向注册中心解析地址（见 common/discovery.py），
# 而解析是一次网络调用，不能同步阻塞事件循环。解析结果在 discovery 内部
# 有本地缓存，所以"每次新建 stub"并不会真的每次都打一次注册中心。
# ---------------------------------------------------------------------------
async def inventory_stub() -> "inventory_pb2_grpc.InventoryServiceStub":  # noqa: F821
    import inventory_pb2_grpc

    addr = await discovery.resolve("inventory", settings.settings.inventory_addr)
    return inventory_pb2_grpc.InventoryServiceStub(channel(addr))


async def order_stub() -> "order_pb2_grpc.OrderServiceStub":  # noqa: F821
    import order_pb2_grpc

    addr = await discovery.resolve("order", settings.settings.order_addr)
    return order_pb2_grpc.OrderServiceStub(channel(addr))
