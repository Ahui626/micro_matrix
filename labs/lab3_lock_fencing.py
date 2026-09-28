"""
[问题]   分布式锁看起来能解决互斥问题。但它有自己的失效模式，
         而且失效的时候**比没有锁更危险** —— 因为它让人以为自己是安全的。
[本文件] 三个实验：
         A. 锁 TTL 到期而业务没做完 → 两个持有者同时进入临界区
         B. 释放锁时不校验持有者 → 删掉了别人的锁
         C. fencing token —— 唯一的正确解法
[面试]   - ★ 锁的 TTL 设多久？设错了会怎样？
           （设短了：业务没做完锁就过期，出现两个持有者。
             设长了：持有者崩溃后别人要等很久。
             **没有安全的取值** —— 因为 TTL 是对"业务耗时"的猜测，
             而业务耗时会被 GC 停顿、网络抖动、磁盘 IO 影响，
             你无法给它一个上界。这正是"锁的过期不是正确性机制"的含义。）
         - SET NX PX 和 SETNX + EXPIRE 的区别？
           （后者不是原子的：拿到锁后、设过期前崩溃 → 锁永不释放，死锁。
             一条 `SET k v NX PX 1000` 消除了这个窗口。）
         - 释放锁为什么要校验持有者？校验了就安全了吗？
           （不校验 → A 的锁超时释放后 B 拿到，A 执行完 DEL 删掉了 B 的锁。
             校验 + 删除要用 Lua 保证原子，否则两步之间还是可能被打断。
             但**即使两步原子了，也不够** —— 见 C。）
         - ★★ fencing token 为什么是真正的解法？
           （每次加锁分配一个单调递增的编号，写数据时带上它，
             存储侧拒绝任何编号小于"已见过的最大值"的写入。
             这样 A 醒来后带着旧编号写，会被存储直接拒绝。
             **核心认知：锁的过期不是正确性机制。**
             正确性必须来自被保护的资源本身 ——
             让资源自己有能力拒绝过期的写入。）
         - Redlock 到底行不行？
           （Martin Kleppmann 与 antirez 的著名争论。要点：
             在"效率场景"（避免重复劳动）Redlock 够用；
             在"正确性场景"依赖它不安全 —— 它对时钟漂移和进程停顿
             没有假设，而这些恰恰是真实世界会发生的。
             结论不是"Redlock 错了"，而是"用错地方的锁会骗你"。）
[取舍]   这个 lab 里"被保护的资源"用一个 Redis key 模拟，而不是真的改库存表。
         理由：要演示 fencing 需要"资源能拒绝旧编号的写入"这个能力，
         而我们的库存表没有这一列。加一列会让生产代码为了教学而变复杂。
         代价：资源的模拟物比较抽象。
         真实的做法就是给资源表加一列 last_fence，写入时带上
         `WHERE $fence > last_fence` —— 本质完全一样。

运行前提：只需要中间件。
    docker compose up -d
    python labs/lab3_lock_fencing.py
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

from common import lock, redisx, settings

RESOURCE = "lab3-resource"
TTL_MS = 500
BUSINESS_MS = 900  # 故意超过 TTL —— 模拟 GC 停顿 / 网络卡顿 / 慢查询


# ---------------------------------------------------------------------------
# 被保护的"资源"。它能拒绝过期持有者的写入 —— 这就是 fencing 的全部。
# 真实系统里它是资源表上的一列（比如 inventory.last_fence）。
# ---------------------------------------------------------------------------
class FencedResource:
    """只有当写入携带的 fence 严格大于已见过的最大值时才接受。

    注意：这个检查必须在**存储侧**原子完成，不能"先读再判断再写"。
    否则又变成 lab1 里那种先读后写的竞态了。
    """

    LUA = """
    local current = tonumber(redis.call('GET', KEYS[1]) or '0')
    local fence = tonumber(ARGV[1])
    if fence > current then
        redis.call('SET', KEYS[1], fence)
        return 1
    end
    return 0
    """

    def __init__(self, name: str) -> None:
        self._key = redisx.key("lab3", "resource", name)
        self._script = None

    async def write(self, fence: int) -> bool:
        if self._script is None:
            self._script = redisx.client().register_script(self.LUA)
        accepted = await self._script(keys=[self._key], args=[fence])
        return bool(accepted)

    async def current_fence(self) -> int:
        value = await redisx.client().get(self._key)
        return int(value) if value else 0


# ---------------------------------------------------------------------------
async def case_a_ttl_expiry() -> tuple[lock.LockHandle, lock.LockHandle | None]:
    """A. 锁 TTL 到期，业务还没做完 → 两个持有者。

    场景：A 拿到锁 → A 被 GC 停顿/网络卡住 900ms → 锁在 500ms 时自动过期
          → B 拿到锁 → 此刻 A 和 B 都在"临界区"里。
    """
    raw = redisx.client()
    await raw.delete(redisx.lock_resource(RESOURCE), redisx.fence_counter(RESOURCE))

    lk = lock.RedisLock(RESOURCE, ttl_ms=TTL_MS)

    # A 拿到锁
    h_a = await lk.acquire()
    print(f"  A 拿到锁：fence={h_a.fencing_token}，TTL={TTL_MS}ms")

    # A 的业务耗时超过 TTL。用 sleep 模拟 —— 但真实世界里
    # 这就是一次 GC 停顿，或者一次卡住的网络调用。
    print(f"  A 开始干活，预计耗时 {BUSINESS_MS}ms（超过 TTL）…")
    await asyncio.sleep(BUSINESS_MS / 1000.0)

    # 此时锁早已被 Redis 自动释放（TTL 到期）。B 拿到它。
    h_b = await lk.acquire()
    if h_b is not None:
        print(f"  ★ B 也拿到了锁：fence={h_b.fencing_token}")
        print(f"    —— 锁在 A 还没干完时就过期了，现在**有两个持有者**。")
    else:
        print("  （B 没拿到锁，说明 TTL 比预期长）")

    return h_a, h_b


async def case_b_naive_release(main_lock: lock.RedisLock, h_a, h_b) -> lock.LockHandle | None:
    """B. 释放锁不校验持有者 → 删掉了别人的锁。"""
    raw = redisx.client()

    print(f"  A 终于干完了，执行释放。")
    print(f"    但此刻锁的持有者是 B（fence={h_b.fencing_token}）——")
    # 朴素释放：直接 DEL，不校验 owner。
    # 这是非常多教程里写的写法，因为它"看起来"没问题。
    await raw.delete(main_lock.key)
    print(f"    ★ A 用朴素的 DEL 释放，删掉的是 **B 的锁**。")

    # 现在锁没了，而 B 还在临界区里 —— C 能直接进来
    h_c = await main_lock.acquire()
    if h_c is not None:
        print(f"    ★ C 立刻也拿到了锁：fence={h_c.fencing_token}")
        print(f"    —— B 还在临界区，锁却已经不在它手里了。")

    # 对比：正确的释放方式会校验 owner，所以删不掉别人的锁
    return h_c


async def case_c_fencing(h_a, h_b) -> tuple[bool, bool]:
    """C. fencing token：即使锁失效，数据也不会被破坏。

    顺序：B 先写（它是当前合法持有者，fence 更大），
         然后 A 醒来也想写（拿着过期的 fence），被存储拒绝。
    """
    resource = FencedResource(RESOURCE)
    await redisx.client().delete(redisx.key("lab3", "resource", RESOURCE))

    # B 是当前持有者，写入被接受
    accepted_b = await resource.write(h_b.fencing_token)
    print(f"  B 写入，携带 fence={h_b.fencing_token} → {'接受' if accepted_b else '拒绝'}")

    # A 是过期的持有者，带着更小的 fence 来写
    accepted_a = await resource.write(h_a.fencing_token)
    print(f"  A 写入，携带 fence={h_a.fencing_token}（过期）→ {'接受' if accepted_a else '★ 拒绝'}")
    print(f"    存储当前记录的 fence = {await resource.current_fence()}（仍是 B 的）")

    return accepted_b, accepted_a


# ---------------------------------------------------------------------------
async def main() -> int:
    redisx.connect(settings.settings.redis_url)
    raw = redisx.client()

    print("=" * 78)
    print(" lab3 —— 分布式锁的失效模式，以及 fencing token")
    print("=" * 78)
    print(f" 锁 TTL = {TTL_MS}ms，业务耗时 = {BUSINESS_MS}ms（故意超过 TTL）")
    print("=" * 78)
    print()

    # --- A ---
    print("A. 锁 TTL 到期而业务未完成")
    h_a, h_b = await case_a_ttl_expiry()
    a_violated = h_b is not None
    print()

    # --- B ---
    print("B. 释放锁不校验持有者")
    h_c = await case_b_naive_release(lock.RedisLock(RESOURCE, ttl_ms=TTL_MS), h_a, h_b)
    b_violated = h_c is not None
    print()

    # --- C ---
    print("C. fencing token —— 数据侧的兜底")
    accepted_b, accepted_a = await case_c_fencing(h_a, h_b)
    c_correct = accepted_b and not accepted_a
    print()

    # --- 对照：正确的释放方式 ---
    print("对照：如果用带 owner 校验的释放（common/lock.py 的实现）")
    await raw.delete(redisx.lock_resource(RESOURCE), redisx.fence_counter(RESOURCE))
    lk = lock.RedisLock(RESOURCE, ttl_ms=TTL_MS)
    h1 = await lk.acquire()
    # 模拟 h1 的锁被"别人"抢走了（这里直接改 key 的值来模拟）
    await raw.set(lk.key, "someone-else-token", px=TTL_MS)
    released = await lk.release(h1)
    print(f"  h1 尝试释放 → {'成功' if released else '★ 失败（正确！因为锁已不属于它）'}")
    print(f"  锁仍然存在，值 = {await raw.get(lk.key)}（属于别人）")
    print()

    # --- 结论 ---
    print("=" * 78)
    print(" 结论")
    print("=" * 78)
    print(" 1. 锁的 TTL 是个猜测，而业务耗时没有上界（GC、网络、磁盘）。")
    print("    所以「锁一定在业务完成前有效」这个假设**不成立**。")
    print()
    print(" 2. 释放锁必须：校验 owner + 用 Lua 保证校验和删除原子。")
    print("    但这也只是减少出错，不是消除 —— 因为 A 可能压根等不到释放那一步。")
    print()
    print(" 3. ★ 真正的解法是 fencing token：让**被保护的资源**有能力拒绝")
    print("    过期持有者的写入。这样即使锁失效，数据也不会被破坏。")
    print("    一句话总结：锁的过期不是正确性机制，正确性来自资源本身。")
    print()
    print(" 4. 实践建议：**能不用锁就不用锁**。")
    print("    本项目其他地方（库存扣减、幂等）都没有用锁 ——")
    print("    它们靠单条原子 SQL 和唯一约束解决，比锁更简单也更可靠。")
    print("    锁应该只用在「确实需要跨多步操作互斥」的场景，")
    print("    而且必须配上 fencing 或幂等作为兜底。")
    print()

    if a_violated and b_violated and c_correct:
        print(" ✓ 三个实验全部符合预期：锁会失效，但 fencing 保住了数据。")

    await redisx.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
