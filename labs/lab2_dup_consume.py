"""
[问题]   消息系统给的是 at-least-once（至少一次）——
         这意味着重复投递是**必然**的，不是异常。
         那怎么保证副作用（加积分、发短信、扣款）不被执行两次？
[本文件] 三个实验：
         A. 幂等消费者   —— 同一事件投递 3 次，积分只加 1 次（正确）
         B. 关掉幂等     —— 同一事件投递 3 次，积分加 3 次（错误）
         C. ack 的时机   —— 先 ack 后处理 vs 先处理后 ack，崩溃后差别巨大
[面试]   - ★ 为什么 at-least-once 是"必然重复"而不是"偶尔重复"？
           （消息投递涉及"送达"和"确认"两个动作，它们无法原子完成。
             消费者处理完但 ack 丢失 → 消息必然被重投。
             这个窗口不是概率问题，是结构问题 —— 一定会发生。）
         - ★ 那 exactly-once 呢？
           （**在分布式投递里基本做不到。** 要做到 exactly-once，
             需要"处理"和"确认"在一个事务里，而这两个动作通常在
             不同的系统（你的数据库 vs 消息队列）里。
             跨系统的原子提交需要 2PC，代价极高且消息队列普遍不支持。
             工程上的正确答案是：**at-least-once + 消费端幂等 = effectively-once**。
             主动说出这句话，比被追问出来强得多。）
         - ★ ack 的时机为什么改变语义？
           （先 ack 再处理 = at-most-once：ack 完崩溃，消息永久丢失。
             先处理再 ack = at-least-once：处理完崩溃，消息重投，可能重复。
             看起来"先 ack"更简单更快 —— 但它把"可能重复"换成了"可能丢失"，
             而丢消息比重复严重得多：重复可以靠幂等消除，丢了就真没了。）
         - 幂等和副作用为什么必须在同一个事务里？
           （见 services/order/repo.try_claim_event 的注释。
             顺序错了会导致"标记为已处理但副作用从没发生"—— 消息被永久跳过。）
[取舍]   这个 lab 直接调用 consumer._process()，而不通过正在运行的服务。
         理由：要确定性复现"同一事件投递 3 次"，必须完全控制投递时机。
         走真实的消费者循环只能靠 sleep 碰运气，断言就不可靠了。
         代价：调用了一个下划线开头的"内部函数"。
         这里是可以接受的 —— lab 的目的就是看内部实现。
         如果哪天 _process 的签名变了，这个 lab 会立刻报错，这是好事。

运行前提：只需要中间件。
    docker compose up -d
    python labs/lab2_dup_consume.py
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
import os
import uuid

from common import chaos, db, redisx, reset, settings
from services.order import consumer, repo

USER = "lab2-user"
QTY = 2
POINTS_PER_UNIT = 10
EXPECTED_ONCE = QTY * POINTS_PER_UNIT  # 20


async def _points() -> int:
    async with db.pool().acquire() as conn:
        return await repo.get_points(conn, USER)


def _fields(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": "OrderConfirmed",
        "payload": json.dumps(
            {"order_id": 1, "request_id": "lab2", "user_id": USER,
             "sku": "LAB2-SKU", "quantity": QTY, "reason": ""}
        ),
    }


async def _deliver(event_id: str, times: int) -> None:
    """把同一个事件投递 times 次 —— 模拟 at-least-once 的重复投递。"""
    fields = _fields(event_id)
    for _ in range(times):
        await consumer._process(event_id, fields)


# ---------------------------------------------------------------------------
async def case_idempotent() -> int:
    """A. 幂等消费者：投递 3 次，副作用只执行 1 次。"""
    await reset.reset_world()
    # 确保幂等开启
    os.environ.pop("MM_CHAOS_SKIP_CONSUMER_IDEMPOTENCY", None)

    event_id = uuid.uuid4().hex
    print(f"  投递同一事件 3 次（event_id={event_id[:8]}…），幂等开启")
    await _deliver(event_id, 3)
    return await _points()


async def case_not_idempotent() -> int:
    """B. 关掉幂等：投递 3 次，副作用执行 3 次。"""
    await reset.reset_world()
    # chaos.flag 是每次调用时读环境变量的，所以可以在运行中动态开关
    os.environ["MM_CHAOS_SKIP_CONSUMER_IDEMPOTENCY"] = "1"

    event_id = uuid.uuid4().hex
    print(f"  投递同一事件 3 次（event_id={event_id[:8]}…），幂等**关闭**")
    await _deliver(event_id, 3)

    os.environ.pop("MM_CHAOS_SKIP_CONSUMER_IDEMPOTENCY", None)
    return await _points()


# ---------------------------------------------------------------------------
async def case_ack_timing() -> tuple[str, int, int]:
    """C. 在 Redis 层面演示 ack 时机的差别。

    用一套独立的 stream + 消费组，不干扰正在运行的服务。
    返回 (说明, ack 先行的残留消息数, 处理先行的残留消息数)
    """
    raw = redisx.client()
    stream = redisx.key("lab2", "stream")
    group = "lab2-group"

    async def setup() -> None:
        await raw.delete(stream)
        # id="0" 表示从头消费
        await raw.xgroup_create(stream, group, id="0", mkstream=True)

    # --- C1：先 ack 再处理 ---
    await setup()
    await raw.xadd(stream, _fields("c1-event"))
    read = await raw.xreadgroup(group, "consumer-A", {stream: ">"}, count=1)
    msg_id = read[0][1][0][0]
    await raw.xack(stream, group, msg_id)      # ← 先 ack
    # 此刻"崩溃"：根本没执行副作用。
    # 消息已经离开 pending，谁也不记得它了。
    c1_pending = await _pending_count(raw, stream, group)
    # 试着接管遗留消息 —— 什么也捞不到
    c1_reclaimed = await _try_reclaim(raw, stream, group, "consumer-B")

    # --- C2：先处理再 ack ---
    await raw.delete(stream)
    await raw.xgroup_create(stream, group, id="0", mkstream=True)
    await raw.xadd(stream, _fields("c2-event"))
    read = await raw.xreadgroup(group, "consumer-A", {stream: ">"}, count=1)
    msg_id = read[0][1][0][0]
    # 这里假装处理完成了，但**还没 ack** 就崩溃
    c2_pending = await _pending_count(raw, stream, group)
    c2_reclaimed = await _try_reclaim(raw, stream, group, "consumer-B")

    return ("C1 先 ack 再处理 / C2 先处理再 ack", c1_reclaimed, c2_reclaimed)


async def _pending_count(raw, stream: str, group: str) -> int:
    info = await raw.xpending(stream, group)
    return info["pending"] if isinstance(info, dict) else int(info[0])


async def _try_reclaim(raw, stream: str, group: str, consumer_name: str) -> int:
    """尝试接管遗留消息，返回接管到的条数。

    注意 min_idle_time=0：实验室里不等空闲超时，立刻接管。
    """
    try:
        result = await raw.xautoclaim(
            stream, group, consumer_name, min_idle_time=0, start_id="0-0", count=10
        )
    except Exception:
        return 0
    messages = result[1] if len(result) > 1 else []
    return sum(1 for _mid, fields in messages if fields)


# ---------------------------------------------------------------------------
async def main() -> int:
    await db.connect(settings.settings.pg_dsn)
    await db.init_schema()
    redisx.connect(settings.settings.redis_url)

    print("=" * 74)
    print(" lab2 —— 重复消费：at-least-once 的代价与对策")
    print("=" * 74)
    print(f" 一个 OrderConfirmed 事件，quantity={QTY}，应加积分 {EXPECTED_ONCE}")
    print(f" 当前故障注入：{chaos.describe()}")
    print("=" * 74)
    print()

    # --- A ---
    print("A. 幂等消费者（正确做法）")
    points_a = await case_idempotent()
    ok_a = points_a == EXPECTED_ONCE
    print(f"  [{'正确' if ok_a else '★ 错误'}] 投递 3 次后积分 = {points_a}（期望 {EXPECTED_ONCE}）")
    print("           关键：INSERT ... ON CONFLICT DO NOTHING + rowcount 判断，")
    print("                 而且认领和副作用在同一个事务里。")
    print()

    # --- B ---
    print("B. 关掉幂等（错误做法）")
    points_b = await case_not_idempotent()
    ok_b = points_b == EXPECTED_ONCE * 3
    print(f"  [{'如预期地错误' if ok_b else '? 未复现'}] 投递 3 次后积分 = {points_b}"
          f"（期望本应 {EXPECTED_ONCE}，实际变成 {EXPECTED_ONCE * 3}）")
    print("           这就是为什么每个消费者都**必须**自己幂等：")
    print("           消息队列只保证「至少一次」，去重是你自己的责任。")
    print()

    # --- C ---
    print("C. ack 的时机决定语义（Redis 层面）")
    label, c1, c2 = await case_ack_timing()
    print(f"  C1 「先 ack 再处理」+ 崩溃 → 能被接管的消息数 = {c1}")
    print(f"       ★ 0 条。消息已经 ack 掉了，不在任何人的 pending 列表里，")
    print("         连 XAUTOCLAIM 也救不回来 —— 副作用永远没执行。")
    print("         这是 at-most-once：可能丢。")
    print()
    print(f"  C2 「先处理再 ack」+ 崩溃 → 能被接管的消息数 = {c2}")
    print(f"       ★ {c2} 条。消息还挂在 pending 列表里，")
    print("         XAUTOCLAIM 会把它交给活着的消费者重投 —— 副作用会重新执行。")
    print("         这是 at-least-once：可能重复，但不会丢。")
    print("         配合 A 的幂等，就得到了 effectively-once。")
    print()

    print("=" * 74)
    print(" 结论")
    print("=" * 74)
    print(" 1. at-least-once 意味着重复投递是**必然**的，不是异常。")
    print("    所以每个消费者都必须自己幂等 —— 这是消费者的责任，不是队列的。")
    print()
    print(" 2. exactly-once 在跨系统投递里基本做不到：")
    print("    「处理」和「确认」分属你的数据库和消息队列，无法原子完成。")
    print("    正确说法是：at-least-once + 消费端幂等 = effectively-once。")
    print()
    print(" 3. ack 的时机是语义的分水岭。选「先处理后 ack」，")
    print("    然后用幂等吸收重复 —— 因为丢消息无法补救，重复可以。")
    print()

    ok_c = c1 == 0 and c2 == 1
    if ok_a and ok_b and ok_c:
        print(" ✓ 三个实验全部符合预期。")

    await db.close()
    await redisx.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
