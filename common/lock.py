"""
[问题]   多个 order 实例要改同一条库存记录，怎么互斥？
         更麻烦的是：持锁者可能因为 GC 停顿或网络卡顿而超时，
         锁被自动释放了，但它自己不知道，继续往下写 —— 把别人的成果覆盖掉。
[本文件] 一个"基本可用"的 Redis 锁，加上 fencing token —— 后者才是真正的正确性保证。
[面试]   - SET NX PX 和 SETNX + EXPIRE 有什么区别？
           （后者**不是原子操作**：拿到锁之后、设置过期之前的那个瞬间进程崩溃，
             锁就永远不会释放 —— 死锁。
             一条 `SET key val NX PX 1000` 是原子的，不存在这个窗口。）
         - 锁超时了但业务没做完，会发生什么？
           （两个进程同时进入临界区。这比"完全没有锁"更危险，
             因为它让所有人以为自己是被保护的。）
         - 释放锁时为什么要校验持有者？
           （A 的锁超时释放，B 拿到了锁。这时 A 执行完来 DEL，
             删掉的是 **B 的锁** —— B 还在临界区里，却已经没有锁保护了。
             用 owner token 校验可以避免误删。）
         - 但"校验 + 删除"仍然不是原子的，怎么办？
           （校验和删除之间也可能被打断。必须用 Lua 脚本把两步合成一步，
             Redis 执行 Lua 是原子的。见 RELEASE_LUA。）
         - fencing token 是什么？为什么说它才是真正的解法？
           （每次加锁分配一个**单调递增**的编号，写数据时带上它，
             存储侧拒绝任何编号小于"已见过的最大编号"的写入。
             这样即使锁失效、过期持有者还在写，它的写入也会被存储拒绝。
             **核心认知：锁的过期不是一个正确性机制。**
             正确性必须来自被保护的资源本身。）
         - Redlock 到底靠不靠谱？
           （Martin Kleppmann 与 antirez 的著名争论。
             要点：在"效率场景"（避免重复劳动）Redlock 够用；
             在"正确性场景"依赖它是不安全的，因为它对时钟漂移和
             进程停顿（GC、虚拟机挂起）没有假设。
             结论不是"Redlock 错了"，而是"用错地方的锁会骗你"。）
[取舍]   用单实例 Redis 锁而不是 Redlock。
         理由：本项目要讲清楚"锁本身不是正确性保证"，
         fencing token 把这层窗户纸捅破了。
         Redlock 的完整讨论放在 docs/4-concepts.md，不必再写一份代码。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

from common import chaos, redisx

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 释放锁：必须"校验持有者"和"删除"原子完成。
#
# 反例（lab3 会演示它的后果）：
#     if await redis.get(key) == token:
#         await redis.delete(key)      # ← 这两行之间可能被打断
#
# 更糟的反例（也很常见）：
#     await redis.delete(key)          # ← 根本不校验，会删掉别人的锁
# ---------------------------------------------------------------------------
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class LockNotAcquired(Exception):
    """在给定的等待时间内没抢到锁。"""


@dataclass
class LockHandle:
    """一次成功的加锁。

    fencing_token 是本项目最重要的字段：它单调递增，
    用它可以让**被保护的资源**识别出"过期的持有者"。
    """

    key: str
    name: str
    owner_token: str
    fencing_token: int
    ttl_ms: int


class RedisLock:
    """单个资源的互斥锁。

    用法一（手动）：
        handle = await lock.acquire(wait_s=1.0)
        try:
            ...
        finally:
            await lock.release(handle)

    用法二（上下文管理器，推荐）：
        async with lock.hold(wait_s=1.0) as handle:
            ...
    """

    def __init__(self, name: str, ttl_ms: int, retry_interval_s: float = 0.02) -> None:
        self._name = name
        # 名字里用冒号分层（比如 "stock:SKU-1"）→ key 是 mm:lock:stock:SKU-1。
        # key 的拼法统一由 redisx.key 决定，不在这里手写字符串。
        self._key = redisx.lock_resource(name)
        self._ttl_ms = ttl_ms
        self._retry_interval_s = retry_interval_s
        self._release_script = None

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._name

    def _effective_ttl_ms(self) -> int:
        """TTL 可以被故障注入覆盖 —— lab3 就是靠这个制造锁过期的。"""
        override = chaos.number(chaos.LOCK_TTL_MS, 0.0)
        return int(override) if override > 0 else self._ttl_ms

    async def acquire(self, wait_s: float = 0.0) -> LockHandle | None:
        """尝试加锁。wait_s 是愿意等待的秒数，0 表示只试一次。

        返回 None 表示超时没抢到。
        """
        ttl_ms = self._effective_ttl_ms()
        owner_token = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s

        while True:
            # 一条 SET 同时完成"不存在才设置"和"设置过期"两件事。
            # 这一行的原子性就是它比 SETNX+EXPIRE 安全的原因。
            acquired = await redisx.client().set(
                self._key, owner_token, nx=True, px=ttl_ms
            )
            if acquired:
                # 分配一个单调递增的 fencing token。
                # INCR 是原子的，所以全局唯一且严格递增。
                fencing_token = await redisx.client().incr(redisx.fence_counter(self._name))
                log.debug(
                    "[锁 %s] 加锁成功 owner=%s fence=%d ttl=%dms",
                    self._name, owner_token[:8], fencing_token, ttl_ms,
                )
                return LockHandle(
                    key=self._key,
                    name=self._name,
                    owner_token=owner_token,
                    fencing_token=fencing_token,
                    ttl_ms=ttl_ms,
                )

            if loop.time() >= deadline:
                return None
            # 退避重试。加一点随机避免多个等待者同步抢锁。
            await asyncio.sleep(self._retry_interval_s)

    async def release(self, handle: LockHandle) -> bool:
        """释放锁。只有持有者才能释放成功。"""
        if self._release_script is None:
            self._release_script = redisx.client().register_script(RELEASE_LUA)
        removed = await self._release_script(keys=[handle.key], args=[handle.owner_token])
        if removed:
            log.debug("[锁 %s] 已释放 fence=%d", handle.name, handle.fencing_token)
        else:
            # 走到这里说明锁已经因为超时被自动释放、并且可能已经被别人拿到了。
            # 这是一个**值得告警的信号**：说明临界区执行时间超过了 TTL。
            log.warning(
                "[锁 %s] 释放失败 —— 锁已过期或被他人持有（fence=%d）。"
                "临界区执行时间超过了 TTL %dms，此时依赖锁保护是不安全的。",
                handle.name, handle.fencing_token, handle.ttl_ms,
            )
        return bool(removed)

    @asynccontextmanager
    async def hold(self, wait_s: float = 0.0) -> AsyncIterator[LockHandle]:
        handle = await self.acquire(wait_s=wait_s)
        if handle is None:
            raise LockNotAcquired(f"等待 {wait_s}s 仍未能获取锁 {self._name}")
        try:
            yield handle
        finally:
            await self.release(handle)
