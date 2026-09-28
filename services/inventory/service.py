"""
[问题]   库存服务的业务逻辑：预留、释放、查询。幂等在这里落地。
[本文件] 编排 repo 的 SQL、管理事务边界。**不 import grpc** ——
         业务逻辑不该知道自己在被什么协议调用。
[面试]   - 为什么业务层不能依赖传输层？
           （同一段逻辑要能被 gRPC / HTTP / 消息消费者 / 定时任务复用。
             一旦 import grpc，它就只能在 gRPC 服务里用了，
             连单元测试都得先起一个 server。
             依赖方向永远是：传输层 → 业务层 → 数据层，单向。）
         - 幂等键为什么必须由客户端生成？
           （只有调用方知道自己"重试了同一个逻辑操作"。
             服务端每次生成的键都不同，幂等就无从谈起。）
         - 为什么"扣库存"和"写预留记录"必须在同一个事务里？
           （分开写会出现两种破损状态：库存扣了但没留记录 → 补偿时不知道加回多少；
             留了记录但库存没扣 → 凭空占用库存。
             事务保证两者同生共死，不存在中间状态。）
         - **这个场景需要分布式锁吗？**
           （不需要。正确性来自单条原子 UPDATE 和唯一约束，不需要任何锁。
             这是个重要认知：很多项目引入分布式锁，是因为没找到合适的
             原子操作，而不是因为锁真的必要。
             锁有自己的失效模式（超时、误删、GC 停顿），
             能不用就不用。反面对照见 labs/lab3。）
[取舍]   事务边界放在 service 层而不是 repo 层。
         只有 service 知道"哪几条语句必须一起成功"。
         repo 只管单条 SQL 的正确性，不管业务上的一致性要求。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from common import chaos, db
from services.inventory import repo

log = logging.getLogger(__name__)


@dataclass
class ReserveResult:
    ok: bool
    reason: str
    remaining: int
    reservation_id: str
    deduplicated: bool = False


@dataclass
class ReleaseResult:
    ok: bool
    remaining: int
    deduplicated: bool = False


def _now_ms() -> int:
    return int(time.time() * 1000)


async def reserve(request_id: str, sku: str, quantity: int) -> ReserveResult:
    """预留库存。幂等 —— 同一个 request_id 重复调用返回第一次的结果。"""
    async with db.pool().acquire() as conn:
        # ---- 第一步：幂等检查 ----
        existing = await repo.find_reservation(conn, request_id)
        if existing is not None:
            log.info(
                "幂等命中 request_id=%s → 复用预留 %s",
                request_id, existing["reservation_id"][:8],
            )
            stock = await repo.get_stock(conn, sku)
            return ReserveResult(
                ok=existing["status"] == "RESERVED",
                reason="",
                remaining=stock["stock"] if stock else 0,
                reservation_id=existing["reservation_id"],
                deduplicated=True,
            )

        # ---- 第二步：扣库存 + 写预留记录，同一个事务 ----
        # 事务让这两步同生共死，不存在"扣了但没记录"的中间状态。
        async with conn.transaction():
            if chaos.flag(chaos.DISABLE_ATOMIC_DECR):
                # 反面教材路径，只在故障注入时启用。
                log.warning("★ 已注入 DISABLE_ATOMIC_DECR —— 正在使用会超卖的实现")
                outcome = await repo.reserve_naive(conn, sku, quantity)
            else:
                outcome = await repo.reserve_atomic(conn, sku, quantity)
            # 想对比乐观锁的实现，把上面这行换成：
            #   outcome = await repo.reserve_optimistic(conn, sku, quantity)

            if not outcome.ok:
                # 事务里什么都没写，直接返回失败（退出 with 块会提交一个空事务）
                return ReserveResult(False, outcome.reason, outcome.remaining, "")

            reservation_id = await repo.insert_reservation(
                conn, request_id, sku, quantity, _now_ms()
            )

        return ReserveResult(True, "", outcome.remaining, reservation_id)


async def release(request_id: str, sku: str, quantity: int) -> ReleaseResult:
    """释放预留（Saga 补偿）。幂等 —— 重复释放不会把库存加回去多次。

    注意 quantity 参数基本用不上：真正要加回多少，以**当初预留的记录**为准，
    而不是调用方现在说的数字。用调用方传的值是危险的 ——
    它可能传错，或者在意在"多退一点"。
    """
    async with db.pool().acquire() as conn:
        existing = await repo.find_reservation(conn, request_id)
        if existing is None:
            # 从来没有预留过，没有东西需要释放。
            # 这不是错误 —— 可能是补偿动作先于正向操作到了，或者正向操作根本没执行。
            log.info("释放请求对应的预留不存在 request_id=%s，无需补偿", request_id)
            return ReleaseResult(ok=False, remaining=0)

        async with conn.transaction():
            # 先"抢占"释放权。只有第一次调用能拿到，后续都返回 rowcount=0。
            claimed = await repo.mark_reservation_released(conn, existing["reservation_id"])
            if not claimed:
                # 已经被释放过了 → 幂等返回，**不重复加库存**。
                # 少了这个判断，一次失败的订单会把库存加回两次，凭空多出货。
                stock = await repo.get_stock(conn, sku)
                log.info("预留 %s 已释放过，跳过库存回补", existing["reservation_id"][:8])
                return ReleaseResult(
                    ok=True,
                    remaining=stock["stock"] if stock else 0,
                    deduplicated=True,
                )

            # 用记录里的数量，不用参数里的数量。
            remaining = await repo.release(conn, sku, existing["quantity"])

        return ReleaseResult(ok=True, remaining=remaining)


async def get_stock(sku: str) -> dict | None:
    async with db.pool().acquire() as conn:
        return await repo.get_stock(conn, sku)
