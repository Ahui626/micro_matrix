"""
[问题]   订单写进了数据库，但"通知下游"的消息没发出去 —— 或者反过来。
         这两种不一致都会**永久存在**，没有任何机制能自己恢复。
[本文件] 三个实验：
         A. 只写订单、不发事件 → 事件永久丢失，下游永远不知道这笔单
         B. 只发事件、不写订单 → 幽灵订单，下游处理了一个不存在的单
         C. Outbox 模式 → 单事务写入 + relay 补发 → 崩溃后自动收敛
[面试]   - ★ 为什么"先写库再发消息"和"先发消息再写库"都不行？
           （因为库和消息队列是两个**独立的存储**，你无法让它们的写入
             原子化。两次写入之间必然存在一个窗口，进程在那个窗口崩溃，
             就会留下永久的不一致：
               先写库后发消息 → 事件丢失（少卖了通知、漏了积分、对账对不上）
               先发消息后写库 → 幽灵订单（下游处理了一个不存在的单）
             注意这不是概率问题：只要运行足够久，这个窗口一定会被撞上。）
         - ★ 本地消息表是怎么解决的？
           （关键洞察：**不要真的去"发消息"**。把"要发的消息"
             当作**业务数据**，和业务变更写在同一个数据库事务里。
             于是"订单已创建"和"事件待发送"要么一起成功、要么一起失败 ——
             因为它们在同一个事务里，这是数据库本来就能保证的事。
             然后由 relay 进程在事务之外，把 outbox 里的事件真的投递出去。）
         - 那 relay 挂了、或者投递到一半崩溃呢？
           （事件还在 outbox 表里、published 还是 false，
             relay 重启后会重新投递。这就是"最终一致" ——
             不保证即时，但保证一定会到达。
             代价：relay 只能做到 at-least-once（见 lab2），
             所以消费端必须幂等。）
         - 这个模式的代价是什么？
           （1. 额外一次数据库写入（outbox 表）和一次轮询延迟；
             2. outbox 表会持续增长，需要归档/清理策略；
             3. relay 是额外的组件，要监控它的积压量（lag）。
             面试里主动说出代价，比只讲优点可信得多。）
[取舍]   用"不调用 relay"来模拟崩溃，而不是真的 kill 进程。
         理由：确定性。真 kill 进程需要控制"在事务提交之后、relay 之前"
         这个精确时点，靠 sleep 撞不可靠。
         用"写完不跑 relay"达到完全相同的状态：订单在库、outbox 未发送。
         这比 kill 进程更可控，而且状态**完全一致**。

运行前提：只需要中间件。
    docker compose up -d
    python labs/lab4_outbox_twinwrite.py
"""

from __future__ import annotations

# 入口脚本固定前缀：向上找到 bootstrap.py 并导入。
import pathlib as _pathlib
import sys

for _p in [_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents]:
    if (_p / "bootstrap.py").exists():
        sys.path.insert(0, str(_p))
        break
import bootstrap  # noqa: F401  pylint: disable=unused-import

import asyncio
import json
import time
import uuid

from common import db, redisx, reset, settings
from common.ids import SnowflakeGenerator
from services.order import relay, repo

SKU = "LAB4-SKU"
USER = "lab4-user"
_snowflake = SnowflakeGenerator(worker_id=7)


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _order_exists(order_id: int) -> bool:
    async with db.pool().acquire() as conn:
        return await conn.fetchval("SELECT count(*) FROM orders WHERE id = $1", order_id) == 1


async def _outbox_rows() -> list[dict]:
    async with db.pool().acquire() as conn:
        rows = await conn.fetch("SELECT id, event_id, event_type, published FROM outbox ORDER BY id")
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
async def case_a_order_without_event() -> tuple[bool, int]:
    """A. 只写订单，不写事件。

    模拟：INSERT 订单成功 → 进程崩溃 → 发消息那行代码没执行到。
    """
    await reset.reset_world()
    order_id = _snowflake.next_id()

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            # write_event=False 就是"只写订单不写事件"的路径。
            # 真实代码里这不是一个参数，而是"写完订单后进程崩了"。
            await repo.insert_order_with_outbox(
                conn, order_id=order_id, request_id="lab4-a", user_id=USER,
                sku=SKU, quantity=1, now_ms=_now_ms(), write_event=False,
            )

    # 之后无论 relay 跑多少轮，都补不回来 —— outbox 里根本没有这条记录。
    for _ in range(3):
        await relay.publish_batch()

    return await _order_exists(order_id), len(await _outbox_rows())


# ---------------------------------------------------------------------------
async def case_b_event_without_order() -> tuple[int, bool]:
    """B. 只发事件，不写订单。

    模拟：先把事件投进消息队列 → 进程崩溃 → 订单没写进去。
    下游会收到一个"幽灵订单"的事件。
    """
    await reset.reset_world()
    ghost_order_id = 999_999_999

    await redisx.client().xadd(
        redisx.stream_order_events(),
        {
            "event_id": uuid.uuid4().hex,
            "event_type": "OrderConfirmed",
            "payload": json.dumps({
                "order_id": ghost_order_id, "request_id": "lab4-b", "user_id": USER,
                "sku": SKU, "quantity": 1, "reason": "",
            }),
        },
    )
    # 订单从没被写进去
    stream_len = await redisx.client().xlen(redisx.stream_order_events())
    return stream_len, await _order_exists(ghost_order_id)


# ---------------------------------------------------------------------------
async def case_c_outbox() -> tuple[bool, int, int, int]:
    """C. Outbox 模式：单事务写入 → 崩溃 → relay 补发 → 收敛。

    返回 (订单存在, 崩溃时未发送数, 补发后未发送数, 流中事件数)
    """
    await reset.reset_world()
    order_id = _snowflake.next_id()

    # ---- 临界点：订单和事件在**同一个事务**里写入 ----
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await repo.insert_order_with_outbox(
                conn, order_id=order_id, request_id="lab4-c", user_id=USER,
                sku=SKU, quantity=1, now_ms=_now_ms(), write_event=True,
            )

    # 此刻"崩溃"：relay 还没来得及投递。
    # 但请注意状态：订单已经落库，事件也已经在 outbox 里 —— **两者同时存在**。
    order_saved = await _order_exists(order_id)
    pending_before = sum(1 for r in await _outbox_rows() if not r["published"])

    # ---- "重启"后 relay 补发 ----
    await relay.publish_batch()

    rows = await _outbox_rows()
    pending_after = sum(1 for r in rows if not r["published"])
    stream_len = await redisx.client().xlen(redisx.stream_order_events())

    return order_saved, pending_before, pending_after, stream_len


# ---------------------------------------------------------------------------
async def main() -> int:
    await db.connect(settings.settings.pg_dsn)
    await db.init_schema()
    redisx.connect(settings.settings.redis_url)

    print("=" * 78)
    print(" lab4 —— 双写不一致，以及本地消息表（Outbox）")
    print("=" * 78)
    print(" 问题：你无法把「写数据库」和「发消息」变成一个原子操作。")
    print("=" * 78)
    print()

    # --- A ---
    print("A. 只写订单，不写事件（先写库后发消息 → 崩溃）")
    a_order, a_outbox = await case_a_order_without_event()
    print(f"  订单是否落库：{a_order}")
    print(f"  outbox 记录数：{a_outbox}")
    print(f"  ★ 事件永久丢失。之后无论 relay 跑多少轮都补不回来 ——")
    print(f"    outbox 里压根没有这条记录。下游永远不知道这笔订单存在。")
    a_broken = a_order and a_outbox == 0
    print()

    # --- B ---
    print("B. 只发事件，不写订单（先发消息后写库 → 崩溃）")
    b_stream, b_order = await case_b_event_without_order()
    print(f"  流中事件数：{b_stream}")
    print(f"  对应订单是否存在：{b_order}")
    print(f"  ★ 幽灵订单。下游会收到一个事件，回头查订单却发现不存在。")
    print(f"    它可能已经给用户加了积分、发了通知 —— 而这些都无法撤销。")
    b_broken = b_stream == 1 and not b_order
    print()

    # --- C ---
    print("C. Outbox 模式（正确做法）")
    c_order, c_before, c_after, c_stream = await case_c_outbox()
    print(f"  崩溃时：订单已落库={c_order}，outbox 未发送={c_before}")
    print(f"     ★ 注意：订单和事件**同时存在**。事务保证了两者同生共死。")
    print(f"  relay 补发后：outbox 未发送={c_after}，流中事件数={c_stream}")
    print(f"     ★ 收敛了。relay 重启后自动把积压的事件投递出去。")
    c_ok = c_order and c_before == 1 and c_after == 0 and c_stream == 1
    print()

    # --- 结论 ---
    print("=" * 78)
    print(" 结论")
    print("=" * 78)
    print(" 1. 双写不一致不是概率问题，是结构问题 —— 运行够久一定会撞上。")
    print("    而且两种方向的破损都无法自愈：A 少事件，B 多幽灵。")
    print()
    print(" 2. 关键洞察：**不要真的去「发消息」**。")
    print("    把要发的消息当作业务数据，和业务变更写进同一个事务。")
    print("    于是数据库的 ACID 保证自动覆盖了「消息不丢」。")
    print()
    print(" 3. 代价（面试时要主动说）：")
    print("    · 多一次数据库写入 + 一次轮询延迟（本项目 200ms）")
    print("    · outbox 表会持续增长，需要归档清理策略")
    print("    · relay 只做到 at-least-once，所以消费端必须幂等（见 lab2）")
    print("    · relay 要监控积压量（lag），积压说明投递出了问题")
    print()

    if a_broken and b_broken and c_ok:
        print(" ✓ 三个实验全部符合预期：双写会永久破损，Outbox 能收敛。")

    await db.close()
    await redisx.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
