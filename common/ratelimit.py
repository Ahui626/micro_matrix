"""
[问题]   某个用户狂刷下单接口，怎么限制住他，又不误伤正常用户？
[本文件] Redis + Lua 实现的令牌桶。容量决定能容忍多大的突发，速率决定长期平均流量。
[面试]   - 四种限流算法的区别？
           固定窗口：最简单，但窗口边界会放过 2 倍流量，治不了边界穿越。
           滑动窗口：精确，但要存每个请求的时间戳，内存开销大。
           漏桶：恒定速率流出，能削峰，但**无法应对突发**。
           令牌桶：**允许突发**（只要桶里有存货），是最常用的方案。
         - 令牌桶为什么能容忍突发？
           （令牌是攒出来的。空闲 10 秒就攒了 10 秒的量，
             一波请求来可以直接放行到桶容量上限。
             这符合真实业务：用户不会平均地每秒下一单。）
         - 为什么必须用 Lua，不能分三步在 Python 里写？
           （"读令牌 → 判断 → 写回"三步之间，事件循环会切到别的协程。
             两个并发请求都读到旧值、都判断通过、都扣减 —— 超发。
             Redis 执行 Lua 是**原子**的，把三步变成一步。
             这是本文件最核心的一句话。）
         - 为什么 Lua 里用 redis.call('TIME') 而不是客户端传时间进来？
           （多台应用服务器的时钟有偏差。假设 A 机器快 3 秒，
             它会认为自己那个桶已经攒满了，于是多放行一批。
             用 Redis 自己的时间，所有实例共享同一个时钟源。）
         - 限流 key 该用什么维度？
           （按用户 ID 会漏掉未登录流量；按 IP 会误伤 NAT 后的整栋楼。
             实践中是组合：用户级 + 接口级 + 全局兜底，任何一层超限就拒绝。）
[取舍]   令牌桶而不是滑动窗口。
         令牌桶的全部状态就两个数（现有令牌、上次刷新时间），一个 HASH 存完；
         滑动窗口要存时间戳集合，或者用 Redis 的 ZSET 做，内存和复杂度都更高。
         代价：令牌桶保证的是**长期平均速率**，不是"精确每秒 N 次"。
         对限流这个场景，近似足够 —— 我们要的是保护下游，不是计费精确。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from common import redisx

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 令牌桶的 Lua 脚本。
#
# 全部状态装在一个 HASH 里：
#   tokens —— 当前令牌数（浮点，因为要按小数时间补充）
#   ts     —— 上次补充令牌的时间戳（秒，浮点）
#
# 关键设计：**不存储"上次请求时间"之外的任何历史**。
# 桶的状态是幂等的 —— 给定 (tokens, ts, now)，输出完全确定。
# 这让它可以安全地重试、可以分布式共享。
# ---------------------------------------------------------------------------
TOKEN_BUCKET_LUA = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local need     = tonumber(ARGV[3])

-- 用 Redis 自己的时间，避免多台应用服务器时钟不一致。
-- TIME 返回 {秒, 微秒}，拼成浮点秒。
local t    = redis.call('TIME')
local now  = tonumber(t[1]) + tonumber(t[2]) / 1000000

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
  -- 第一次见到这个 key：桶是满的。
  -- 这个选择很重要 —— 新用户第一秒就能用满容量，而不是从 0 慢慢攒。
  tokens = capacity
  ts = now
end

-- 按流逝的时间补充令牌。
-- delta 可能为负（Redis 主从切换、脚本被重放），钳到 0 避免"倒扣令牌"。
local delta = now - ts
if delta < 0 then delta = 0 end
tokens = math.min(capacity, tokens + delta * rate)

local allowed = 0
if tokens >= need then
  tokens = tokens - need
  allowed = 1
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))

-- 过期时间：桶空了之后再过 TTL 就没人关心了，自动清理避免冷 key 占内存。
-- 取 capacity/rate 是"装满一桶需要的时间"，再加 1 秒余量。
redis.call('EXPIRE', key, math.ceil(capacity / rate) + 1)

return {allowed, math.floor(tokens)}
"""


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int

    @property
    def rejected(self) -> bool:
        return not self.allowed


class TokenBucketLimiter:
    """每个 key（通常是 user_id）一个桶。"""

    def __init__(self, capacity: int, rate_per_sec: float) -> None:
        if capacity <= 0 or rate_per_sec <= 0:
            raise ValueError("capacity 和 rate 必须为正数")
        self._capacity = capacity
        self._rate = rate_per_sec
        self._script = None  # 惰性注册，见 _ensure_script

    def _ensure_script(self):
        """惰性注册 Lua 脚本。

        为什么惰性：模块被 import 时 Redis 可能还没连上。
        注册动作本身会把脚本缓存到 Redis（EVALSHA），
        之后每次调用只传 SHA，省掉传输整个脚本的开销。
        """
        if self._script is None:
            self._script = redisx.client().register_script(TOKEN_BUCKET_LUA)
        return self._script

    async def acquire(self, key: str, cost: int = 1) -> RateLimitResult:
        """尝试取走 cost 个令牌。

        返回 RateLimitResult 而不是抛异常 —— 被限流是**正常结果**，
        不是异常。调用方需要根据它做决策（返回 429、或者排队重试）。
        """
        script = self._ensure_script()
        allowed, remaining = await script(
            keys=[key],
            args=[self._capacity, self._rate, cost],
        )
        return RateLimitResult(allowed=bool(allowed), remaining=int(remaining))


# 全局限流器。容量 = 速率（即桶最多存 1 秒的量），
# 也就是说：空闲一秒后最多能突发 capacity 个请求，长期平均不超过 rate。
_limiter: TokenBucketLimiter | None = None


def limiter_for(capacity: int, rate_per_sec: float) -> TokenBucketLimiter:
    global _limiter
    if _limiter is None:
        _limiter = TokenBucketLimiter(capacity, rate_per_sec)
    return _limiter


async def check_user(user_id: str, capacity: int, rate_per_sec: float) -> RateLimitResult:
    """按用户维度限流。gateway 的入口调用。"""
    return await limiter_for(capacity, rate_per_sec).acquire(redisx.ratelimit_user(user_id))
