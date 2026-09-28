"""
[问题]   每个 lab 和测试都必须从**确定的**初始状态开始，
         否则断言无法复现 —— 你分不清"这个结果是我的注入造成的"
         还是"上一轮实验的残留造成的"。
[本文件] 一键把整个系统恢复到干净状态：清库、播种库存、清 Redis 里的消息与计数。
[面试]   - 为什么"可复现"是实验的前提？
           （不可复现的失败等于没观察到 —— 你既不能断言它，也不能验证修复。
             这也是为什么生产环境的偶发 bug 那么难查。）
         - 为什么测试要自己准备数据，而不是依赖某个已有的数据集？
           （依赖外部状态会让测试之间产生顺序耦合：
             单独跑能过、一起跑就挂。每个测试自带前置条件是唯一可靠的做法。）
[取舍]   这个东西在生产环境当然不该存在。
         它存在的唯一理由，是让 lab 的断言成为可能。
         把它独立成一个模块而不是散在 labs 里，是为了让"实验前要重置"
         这件事变成一个显式、显眼的动作。
"""

from __future__ import annotations

import logging

from common import db, redisx

log = logging.getLogger(__name__)

# 需要清掉的 Redis key 模式。用 KEYS 会阻塞 Redis ——
# 这里只在实验重置时用一次，量很小，可以接受。
# 生产环境永远不要用 KEYS，要用 SCAN。
_EPHEMERAL_KEY_PATTERNS = (
    "mm:lock:*",
    "mm:fence:*",
    "mm:rl:*",
)


async def reset_world(seed: dict[str, int] | None = None) -> None:
    """把系统恢复到干净状态。

    seed：{sku: 初始库存}，比如 {"SKU-1": 100}。
          不传则库存表为空。
    """
    # --- 数据库 ---
    await db.reset_all()
    for sku, stock in (seed or {}).items():
        await db.seed_inventory(sku, stock)

    # --- Redis ---
    raw = redisx.client()
    keys = [
        redisx.stream_order_events(),
        redisx.stream_dlq(),
        redisx.key("consumer", "attempts"),
    ]
    for pattern in _EPHEMERAL_KEY_PATTERNS:
        found = await raw.keys(pattern)
        if found:
            keys.extend(found)

    # 注意：删除 stream 会**连带销毁挂在它上面的消费组**。
    # 消费者需要能从这个状态恢复（见 services/order/consumer.py 的 NOGROUP 处理）。
    # 如果哪天消费者不能自愈了，症状就是"重启后消息不再被消费"。
    await raw.delete(*keys)

    log.info(
        "环境已重置：库存=%s，已清空订单/outbox/消息/锁/限流桶",
        (seed or {}) or "（空）",
    )
