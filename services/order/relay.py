"""
[问题]   订单事务提交了，outbox 里那条"待发事件"怎么真的发出去？
[本文件] 中继：轮询未发送的 outbox 行 → XADD 到 Redis Stream → 标记已发送。
         作为后台任务跑在 order 进程里（也可以拆成独立进程，见下方取舍）。
[面试]   - ★ 为什么中继只能做到"至少一次"（at-least-once）？
           （XADD 成功之后、标记 published 之前崩溃 → 重启后会**再发一遍**。
             这不是 bug，是这个结构的固有属性：数据库和消息队列是两个存储，
             你无法让它们的写入原子化。
             所以消费端**必须**幂等 —— 这就是"exactly-once 是假象"的出处。
             面试里主动说出这句话，比被追问出来强得多。）
         - 为什么用轮询，不用数据库触发器或 CDC？
           （轮询：实现简单、零额外组件、天然容错，代价是延迟（这里 200ms）
             和空轮询开销。
             生产系统常用 Debezium 读 binlog 做 CDC —— 本质是把"轮询"
             下沉到数据库日志层，延迟和开销都更低。但那是另一个组件的运维成本。）
         - 多个 relay 实例同时跑会怎样？
           （会重复发送。标准解法见 repo.fetch_unpublished 里的 SKIP LOCKED 注释。
             注意：即使解决了重复发送，消费端幂等依然不能省。）
[取舍]   relay 作为后台任务跑在 order 进程内，而不是独立进程。
         收益：少一个进程、少一套部署和监控。
         代价：relay 的负载和 order 抢同一个事件循环；
             而且 order 进程挂掉时 relay 也停了（事件会延迟，但不会丢 ——
             因为 outbox 是持久化的，这正是它的价值）。
         真实系统里如果是高吞吐，应该拆成独立进程独立伸缩。
"""

from __future__ import annotations

import asyncio
import json
import logging

from common import db, redisx
from services.order import repo

log = logging.getLogger(__name__)

POLL_INTERVAL_S = 0.2
BATCH_SIZE = 50


async def _wait(stop_event: asyncio.Event, timeout_s: float) -> None:
    """要么等到超时，要么等到停止信号。用于让 sleep 可以被立即中断。"""
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=timeout_s)
    except asyncio.TimeoutError:
        pass


async def publish_batch(batch_size: int = BATCH_SIZE) -> int:
    """把一批未发送的事件投递到 Redis Stream。返回投递条数。"""
    async with db.pool().acquire() as conn:
        rows = await repo.fetch_unpublished(conn, batch_size)
        if not rows:
            return 0

        stream = redisx.stream_order_events()
        published = 0
        for row in rows:
            # payload 已经是 dict —— db.py 里给 jsonb 注册了编解码器。
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)

            await redisx.client().xadd(
                stream,
                {
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "payload": json.dumps(payload, ensure_ascii=False),
                },
            )

            # ┌──────────────────────────────────────────────────────────┐
            # │ 就是这里：XADD 已经成功，但这一条还没执行。              │
            # │ 此刻进程崩溃 → 事件已投递、状态未标记 → 下次重启会重发。 │
            # │ 这就是 at-least-once 的来源。                            │
            # │ labs/lab4 会在这里注入崩溃来演示。                       │
            # └──────────────────────────────────────────────────────────┘
            await repo.mark_published(conn, row["id"])
            published += 1

        log.info("relay 投递了 %d 条事件 → %s", published, stream)
        return published


async def run(stop_event: asyncio.Event) -> None:
    """relay 主循环。作为后台 task 启动，靠 stop_event 优雅退出。"""
    log.info(
        "outbox relay 已启动（轮询间隔 %.0fms，批量 %d）", POLL_INTERVAL_S * 1000, BATCH_SIZE
    )
    empty_rounds = 0
    while not stop_event.is_set():
        try:
            count = await publish_batch()
            if count:
                empty_rounds = 0
                continue  # 还有活干，立刻进下一轮，不用等
            empty_rounds += 1
        except Exception:
            # relay 出错不能让整个 order 服务挂掉。
            # outbox 是持久化的，事件不会丢 —— 下一轮会重试。
            log.exception("relay 本轮失败，%0.0fms 后重试", POLL_INTERVAL_S * 1000)
            empty_rounds = 0

        # 空轮询时按间隔等待。空闲久了就退避到 1 秒，
        # 避免在没什么事件时也每秒打 5 次数据库。
        await _wait(stop_event, POLL_INTERVAL_S if empty_rounds < 10 else 1.0)
