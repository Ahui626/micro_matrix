"""
[问题]   这个项目用 Redis 干四件事：限流、分布式锁、消息队列、缓存。
         怎么让这四种用途的 key 不会互相踩？
[本文件] Redis 客户端单例 + 统一的 key 命名规约。
[面试]   - Redis 为什么能同时当缓存、锁、消息队列用？它的数据结构是怎么支撑的？
           （字符串 → 锁（SET NX PX）和缓存；
             哈希 → 令牌桶的状态；
             列表/Stream → 消息队列。
             一条命令的原子性 + Lua 脚本的多命令原子性，是它万能的原因。）
         - Redis Streams 和 Kafka 有什么区别？
           （Streams 没有真正的分区和 rebalance，持久化强度也弱。
             适合轻量场景；Kafka 的吞吐、顺序性保证、回放能力是另一个量级。
             但 Streams 支持消费组 + XACK + XAUTOCLAIM，
             足以演示 at-least-once 和幂等消费的全部机制。）
[取舍]   全部 key 加 `mm:` 前缀 + 分层的 `用途:实体:标识` 命名。
         多花一行代码，换来 redis-cli 里 KEYS mm:* 的自解释性，
         以及未来和别的应用共用一个 Redis 实例时不会互相覆盖。
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

log = logging.getLogger(__name__)

# 全局命名空间。改这一个地方就能让整个应用的 key 换前缀。
NAMESPACE = "mm"

_client: aioredis.Redis | None = None


def key(*parts: str) -> str:
    """拼一个带命名空间的 key。用法：key("lock", "stock", sku)"""
    return ":".join((NAMESPACE, *parts))


def connect(url: str) -> aioredis.Redis:
    global _client
    if _client is not None:
        return _client
    # decode_responses=True：让返回的字节自动解码成 str。
    # 代价是二进制安全的场景（比如存 protobuf）需要单独处理，
    # 但本项目存的都是文本和数字，方便优先。
    _client = aioredis.from_url(url, decode_responses=True)
    log.info("Redis 客户端已建立 %s", url)
    return _client


def client() -> aioredis.Redis:
    if _client is None:
        raise RuntimeError("Redis 还没初始化，请先 redisx.connect(url)")
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def health() -> bool:
    try:
        return bool(await client().ping())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# key 命名集中在这里。想找"系统里有哪些 Redis key、分别长什么样"，
# 只看这个函数区就够了 —— 不用去全文搜索字符串字面量。
# 这和 settings.py 集中配置是同一个思路：让需要全局一致的东西只有一处定义。
# ---------------------------------------------------------------------------

def stream_order_events() -> str:
    """订单领域事件的 Stream。relay 往里写，consumer 从里读。"""
    return key("stream", "order-events")


def stream_dlq() -> str:
    """死信队列。处理失败超过重试上限的消息进来，等待人工介入。"""
    return key("stream", "order-events-dlq")


def lock_resource(resource: str) -> str:
    """分布式锁的 key。resource 形如 "stock:SKU-1"。

    注意名字里用冒号分层，最终 key 是 mm:lock:stock:SKU-1，
     在 redis-cli 里 KEYS 'mm:lock:*' 能一次看清所有锁。
    """
    return key("lock", *resource.split(":"))


def fence_counter(resource: str) -> str:
    """fencing token 的计数器 key。单调递增，见 common/lock.py。"""
    return key("fence", *resource.split(":"))


def ratelimit_user(user_id: str) -> str:
    return key("rl", "user", user_id)
