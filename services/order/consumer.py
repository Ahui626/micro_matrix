"""
[问题]   事件发出来了，怎么消费？消费失败怎么办？重复投递怎么办？
[本文件] Redis Streams 消费组 + 幂等去重 + 失败计数 + 死信队列 + 遗留消息接管。
[面试]   - ★ XACK 的时机为什么决定了投递语义？
           （先 ack 再处理 = **at-most-once**：ack 之后崩溃，消息永久丢失。
             先处理再 ack = **at-least-once**：处理完崩溃会重投，可能重复处理。
             正确选择是后者 + 消费端幂等 = effectively-once。
             顺序写反了，系统就悄悄从"可能重复"变成"可能丢" ——
             而丢消息通常比重复严重得多，因为重复可以幂等，丢了就没了。）
         - XAUTOCLAIM 是干什么的？
           （消费者崩溃后，它读走但没 ack 的消息会一直挂在它的 pending 列表里，
             别的消费者**不会**收到。
             XAUTOCLAIM 让活着的消费者接管这些"遗留消息"。
             没有这一步，一个消费者崩一次就会让一批消息永久卡住 ——
             而且消息还在，看起来一切正常，只是永远不被处理。
             这种静默故障最难排查。）
         - 为什么要死信队列？直接无限重试不行吗？
           （一条"永远处理不了"的消息（比如 payload 格式不对）会无限重试，
             持续打下游、刷满日志，并且阻塞它后面的消息。
             DLQ 把它隔离出来，让正常消息继续流动，问题消息留待人工分析。）
         - 消费端幂等为什么必须和副作用在**同一个事务**里？
           （见 services/order/repo.try_claim_event 的注释：
             顺序错了会导致"标记为已处理但副作用从没发生" —— 消息被永久跳过。）
[取舍]   失败计数存在 Redis 的 HASH 里，而不是用 Stream 自带的投递计数。
         理由：XPENDING 的投递计数要用 XPENDING/XCLAIM 拿，接口繁琐；
         自己计数 5 行搞定，且能一眼看懂"重试到第几次了"。
         代价：多一个 Redis key 要清理（处理后 HGETDEL，见下方）。
         真实项目里用 Stream 自带计数更"纯粹"，因为不需要额外状态。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from common import chaos, db, redisx, settings
from services.order import repo

log = logging.getLogger(__name__)

# 消费者名。同名消费者会分摊消息；不同名会各自收到全量消息。
CONSUMER_NAME = "points-granter"
GROUP = settings.settings.consumer_group

MAX_ATTEMPTS = 3
BLOCK_MS = 300
RECLAIM_MIN_IDLE_MS = 5000
RECLAIM_COUNT = 10

_ATTEMPTS_KEY = redisx.key("consumer", "attempts")


def _now_ms() -> int:
    return int(time.time() * 1000)


async def _ensure_group(stream: str) -> None:
    """创建消费组。已存在时 Redis 返回 BUSYGROUP 错误 —— 忽略它。

    id="0" 表示从流的最开头开始消费（包括组创建之前就存在的消息）。
    生产环境通常用 "$"（只消费新消息），但实验室里我们要能看到全部历史。
    """
    try:
        await redisx.client().xgroup_create(stream, GROUP, id="0", mkstream=True)
        log.info("消费组 %s 已创建", GROUP)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _grant_points(conn, payload: dict) -> None:
    """副作用：给用户加积分。必须接受一个连接，好和"认领"共处一个事务。"""
    user_id = payload["user_id"]
    points = int(payload["quantity"]) * 10
    total = await repo.add_points(conn, user_id, points)
    log.info("已为用户 %s 加 %d 积分（累计 %d）", user_id, points, total)


# 事件类型 → 处理函数。
#
# ★ 注意这里**只订阅终态事件**，不订阅 OrderCreated。
#   因为 OrderCreated 只说明"订单被创建了"，不代表"订单成了" ——
#   一个 Saga 中途失败的订单最终是 CANCELLED，给它发积分是错的。
#   把"事实事件"和"业务结果事件"混用，是事件驱动系统里非常常见的错误。
#
#   同时注意：没有处理器的事件类型不是错误，直接跳过并 ack。
#   一个 Stream 上会挂多种消费者，每个只关心自己订阅的那几种。
EVENT_HANDLERS = {
    "OrderConfirmed": _grant_points,
}


async def _process(event_id: str, fields: dict) -> bool:
    """处理一条消息。返回 True 表示真的执行了副作用。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 顺序至关重要，这个函数的结构就是本文件的核心：                    │
    │                                                                  │
    │   事务 { 认领事件 + 执行副作用 }  →  提交  →  XACK               │
    │                                                                  │
    │ 为什么"认领"和"副作用"必须同一个事务：                            │
    │   先认领（提交）再副作用 → 中间崩溃则事件被标记为已处理，          │
    │   但积分从没加过 —— 消息被**永久跳过**，而且没有任何报错。         │
    │   一起提交则要么都发生、要么都不发生，崩溃后消息重投即可。        │
    │                                                                  │
    │ 为什么 XACK 在事务提交之后：                                      │
    │   提前 ack 就变成 at-most-once，崩溃即丢。见文件头部的说明。      │
    └──────────────────────────────────────────────────────────────────┘
    """
    event_type = fields.get("event_type", "")
    handler = EVENT_HANDLERS.get(event_type)
    if handler is None:
        log.debug("事件 %s 类型 %s 无处理器，跳过（仍会 ack）", event_id[:8], event_type)
        return False

    payload = json.loads(fields["payload"])

    skip_idempotency = chaos.flag(chaos.SKIP_CONSUMER_IDEMPOTENCY)
    if skip_idempotency:
        log.warning("★ 已注入 SKIP_CONSUMER_IDEMPOTENCY —— 重复投递将导致重复加分")

    async with db.pool().acquire() as conn:
        async with conn.transaction():
            if not skip_idempotency:
                claimed = await repo.try_claim_event(conn, event_id, CONSUMER_NAME, _now_ms())
                if not claimed:
                    # 重复投递，而且之前已经完整处理过了。
                    # 直接返回，**不重复执行副作用**。这就是"幂等消费者"。
                    log.info("事件 %s 已处理过，跳过（幂等生效）", event_id[:8])
                    return False

            await handler(conn, payload)
            log.debug("已处理事件 %s 类型=%s", event_id[:8], event_type)

    return True


async def _handle(stream: str, dlq: str, msg_id: str, fields: dict) -> None:
    """处理单条消息：计数 → 检查失败上限 → 处理 → ack。"""
    raw = redisx.client()

    attempts = await raw.hincrby(_ATTEMPTS_KEY, msg_id, 1)

    if attempts > MAX_ATTEMPTS:
        # 超过重试上限：移入死信队列，然后 ack 掉原消息，
        # 这样它不会再被重投，也不会阻塞后面的消息。
        log.error("消息 %s 已失败 %d 次，移入死信队列", msg_id, attempts)
        await raw.xadd(dlq, {**fields, "failed_attempts": str(attempts), "src_id": msg_id})
        await raw.xack(stream, GROUP, msg_id)
        await raw.hdel(_ATTEMPTS_KEY, msg_id)
        return

    # 反面教材注入点：先 ack 再处理。
    # 之后如果崩溃，这条消息就永久丢了 —— at-most-once。
    ack_first = chaos.flag(chaos.ACK_BEFORE_PROCESS)
    if ack_first:
        log.warning("★ 已注入 ACK_BEFORE_PROCESS —— 先确认后处理，崩溃即丢消息")
        await raw.xack(stream, GROUP, msg_id)

    try:
        processed = await _process(fields["event_id"], fields)
    except Exception:
        # 处理失败：**不 ack**。消息留在 pending 列表里，
        # 稍后会被 XAUTOCLAIM 接管并重投。这就是 at-least-once 的重试机制。
        log.exception("处理消息 %s 失败（第 %d 次），不 ack，等待重投", msg_id, attempts)
        return

    # 注入点：处理完了但还没 ack 就崩溃。
    # 重启后消息会被重投，用来验证消费端幂等是否真的生效。
    await chaos.crash_if(chaos.CRASH_BEFORE_ACK)

    if not ack_first:
        await raw.xack(stream, GROUP, msg_id)
    await raw.hdel(_ATTEMPTS_KEY, msg_id)

    if processed:
        log.debug("消息 %s 处理完成并已 ack", msg_id)


async def _reclaim_stale(stream: str, dlq: str) -> None:
    """接管其它消费者遗留的 pending 消息。

    这是最容易被遗漏、也最致命的一环：
    消费者 A 读走消息后崩溃，消息就挂在 A 的 pending 列表里。
    消费者 B 用 ">" 读取时**看不到**它 —— 它已经不是"新消息"了。
    没有 XAUTOCLAIM，这条消息就永远卡在那里，静默地不被处理。
    """
    raw = redisx.client()
    try:
        result = await raw.xautoclaim(
            stream,
            GROUP,
            CONSUMER_NAME,
            min_idle_time=RECLAIM_MIN_IDLE_MS,
            start_id="0-0",
            count=RECLAIM_COUNT,
        )
    except Exception:
        # 老版本 Redis 不支持 xautoclaim，或流还不存在。不影响主流程。
        return

    if not result:
        return
    # redis-py 在不同版本下返回 2 元组或 3 元组：(cursor, messages[, deleted_ids])
    messages = result[1] if len(result) > 1 else []
    for msg_id, fields in messages:
        if not fields:
            # 消息已被 XDEL 删除，只剩个空壳，直接清掉 pending 记录。
            await raw.xack(stream, GROUP, msg_id)
            continue
        log.warning("接管遗留消息 %s（原消费者已失联）", msg_id)
        await _handle(stream, dlq, msg_id, fields)


async def run(stop_event: asyncio.Event) -> None:
    """消费者主循环。作为后台 task 启动。"""
    stream = redisx.stream_order_events()
    dlq = redisx.stream_dlq()
    await _ensure_group(stream)
    log.info(
        "consumer 已启动 group=%s consumer=%s 监听=%s", GROUP, CONSUMER_NAME, stream
    )

    while not stop_event.is_set():
        try:
            # 每轮先接管遗留消息，再读新消息。
            # 顺序有讲究：先处理"别人留下的坑"，再拿新的，避免堆积。
            await _reclaim_stale(stream, dlq)

            resp = await redisx.client().xreadgroup(
                GROUP, CONSUMER_NAME, {stream: ">"}, count=10, block=BLOCK_MS
            )
            for _stream_name, messages in resp or []:
                for msg_id, fields in messages:
                    await _handle(stream, dlq, msg_id, fields)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # NOGROUP 是一个**可恢复**的状态，不是致命错误：
            # Redis 被 FLUSH、流被手动删除、或者 lab 重置环境时都会出现。
            # 注意：在 Redis 里删除 stream 会连带销毁挂在它上面的消费组，
            # 所以"清空消息"这个动作会让消费者失去组身份。
            # 正确处理是重建消费组并继续，而不是让消费者一直报错空转。
            if "NOGROUP" in str(exc):
                log.warning("消费组已不存在（流被删除或 Redis 被清空），重新创建…")
                await _ensure_group(stream)
                continue

            # 其它错误不能让它退出 —— 退出了就再也没有消息被消费了。
            log.exception("consumer 循环出错，500ms 后继续")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass

    log.info("consumer 已停止")
