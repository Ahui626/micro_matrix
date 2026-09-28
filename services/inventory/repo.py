"""
[问题]   并发扣库存怎么才能不超卖？这个文件放三种实现，专门用来对比。
[本文件] 数据访问层。只做 SQL，不管事务边界 —— 所有函数都显式接收连接，
         由 service 层决定"哪几条语句属于同一个事务"。
[面试]   - 为什么"先 SELECT 再 UPDATE"会超卖？
           （两条语句之间存在窗口。其它协程在这期间读到旧的库存值，
             各自都判断"够扣"，于是都扣成功。
             在 asyncio 里这个窗口就是 **await 点** ——
             数据库往返本身就是 await，控制权会在那里交出去。）
         - 乐观锁和单条原子 UPDATE 有什么区别？该用哪个？
           （单条 UPDATE 更好：一次往返、无重试、无 ABA 问题。
             乐观锁要"读版本 → 算新值 → 带版本写 → 查 rowcount → 失败重试"，
             往返多、高冲突下重试率高。
             乐观锁的价值在"改动依赖多个读值"的复杂场景；
             简单计数用单条 UPDATE 就够。两种都写出来是为了看清区别。）
         - 为什么不能用 Python 的锁（threading.Lock / asyncio.Lock）？
           （只能锁住**当前进程**。多实例部署时 A 进程的锁对 B 进程毫无约束。
             这是"单机测试通过、分布式上线出事"的经典陷阱。）
         - 为什么 SQL 里要写 WHERE stock >= $1？
           （把"检查"和"扣减"合并成一条语句，让数据库在同一个原子操作里
             完成判断和修改。这一行就是防超卖的全部秘密。
             不是性能优化，是正确性保证。）
[取舍]   三种实现都保留，而不是只留正确的那一种。
         只看正确代码，读者不知道它在防什么；对照着看错误实现，
         才知道 `WHERE stock >= $1` 那半句为什么一个字符都不能省。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass

import asyncpg

from common import chaos

log = logging.getLogger(__name__)


@dataclass
class ReserveOutcome:
    ok: bool
    remaining: int
    reason: str  # "" / OUT_OF_STOCK / SKU_NOT_FOUND / CONFLICT_EXHAUSTED


# ---------------------------------------------------------------------------
async def get_stock(conn: asyncpg.Connection, sku: str) -> dict | None:
    row = await conn.fetchrow(
        "SELECT sku, stock, version FROM inventory WHERE sku = $1", sku
    )
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# 正确的实现（甲）：单条原子 UPDATE —— 生产首选
# ---------------------------------------------------------------------------
async def reserve_atomic(conn: asyncpg.Connection, sku: str, quantity: int) -> ReserveOutcome:
    """把"检查库存"和"扣减库存"合并成一条语句。

    ★ 这是整个项目最重要的一行 SQL：

        UPDATE inventory
           SET stock = stock - $1, version = version + 1
         WHERE sku = $2 AND stock >= $1

    为什么必须这么写：
      - `stock = stock - $1` 让**数据库**做减法，而不是"读到 Python 里减完再写回"。
        数据库执行这一行时锁住该行，其它事务必须排队。
      - `AND stock >= $1` 把"够不够扣"的判断也交给数据库，
        和扣减在同一个原子操作里完成。判断和修改之间没有缝隙。

    RETURNING stock 让扣减后的余额直接返回，省掉一次查询。

    rowcount 为 0 表示"没有满足条件的行" —— 可能是库存不足，也可以是 SKU 不存在。
    这两种对调用方意味着完全不同的处理，必须分清楚。
    """
    row = await conn.fetchrow(
        """
        UPDATE inventory
           SET stock = stock - $1,
               version = version + 1
         WHERE sku = $2 AND stock >= $1
        RETURNING stock
        """,
        quantity,
        sku,
    )
    if row is not None:
        return ReserveOutcome(True, row["stock"], "")

    exists = await conn.fetchval("SELECT 1 FROM inventory WHERE sku = $1", sku)
    if exists is None:
        return ReserveOutcome(False, 0, "SKU_NOT_FOUND")
    return ReserveOutcome(False, 0, "OUT_OF_STOCK")


# ---------------------------------------------------------------------------
# 正确的实现（乙）：版本号乐观锁
# ---------------------------------------------------------------------------
async def reserve_optimistic(
    conn: asyncpg.Connection, sku: str, quantity: int, max_retries: int = 5
) -> ReserveOutcome:
    """读 → 算 → 带版本写 → 检查影响行数，冲突则重读重试。

    "乐观"的含义：假设冲突很少发生，所以先干活，提交时才检查有没有被改过。
    对比"悲观锁"（SELECT ... FOR UPDATE）—— 那是一开始就锁住、让别人排队等。

    需要重试循环的原因：版本冲突说明我们基于的是过期数据，必须重读重算。
    冲突率越高重试越多、性能越差 —— 这是乐观锁的代价。
    高冲突场景下，带条件的原子 UPDATE（上面那个）反而是更优解。
    """
    for attempt in range(max_retries):
        row = await conn.fetchrow(
            "SELECT stock, version FROM inventory WHERE sku = $1", sku
        )
        if row is None:
            return ReserveOutcome(False, 0, "SKU_NOT_FOUND")

        stock, version = row["stock"], row["version"]
        if stock < quantity:
            return ReserveOutcome(False, stock, "OUT_OF_STOCK")

        result = await conn.execute(
            """
            UPDATE inventory
               SET stock = $1, version = version + 1
             WHERE sku = $2 AND version = $3
            """,
            stock - quantity,
            sku,
            version,
        )
        # asyncpg 的 execute 返回 "UPDATE 1" / "UPDATE 0" 这样的命令标签
        if result.endswith(" 1"):
            return ReserveOutcome(True, stock - quantity, "")

        log.debug("乐观锁冲突 sku=%s version=%d attempt=%d，重读重试", sku, version, attempt)
        # 加随机退避，避免多个冲突者同步重试（和 common/retry.py 是同一个道理）
        await asyncio.sleep(0.005 * (attempt + 1))

    log.warning("乐观锁重试 %d 次仍冲突 sku=%s，放弃", max_retries, sku)
    return ReserveOutcome(False, 0, "CONFLICT_EXHAUSTED")


# ---------------------------------------------------------------------------
# 错误的实现：先读后写 ← labs/lab1 演示的就是它
# ---------------------------------------------------------------------------
async def reserve_naive(
    conn: asyncpg.Connection,
    sku: str,
    quantity: int,
    barrier: asyncio.Barrier | None = None,
) -> ReserveOutcome:
    """先 SELECT 再 UPDATE —— 这两步之间就是超卖的窗口。

    这个函数是**反面教材**，保留它有两个用途：
      1. 服务里通过 MM_CHAOS_DISABLE_ATOMIC_DECR=1 打开，
         在真实并发下观察超卖（概率性复现）。
      2. labs/lab1 传入一个 barrier，把窗口**确定性地**撑开，
         让超卖 100% 复现 —— 教学实验必须可复现，不能靠运气。

    barrier 参数只服务于教学。真实项目不会有这种参数；
    这里保留它，是因为"能稳定复现的 bug"比"偶尔出现的 bug"有教学价值得多。
    """
    row = await conn.fetchrow("SELECT stock FROM inventory WHERE sku = $1", sku)
    if row is None:
        return ReserveOutcome(False, 0, "SKU_NOT_FOUND")

    stock = row["stock"]

    # ┌──────────────────────────────────────────────────────────────────┐
    # │ 竞态窗口。从这一刻到下面 UPDATE 之间，其它并发请求读到的是       │
    # │ **同一个** stock 值。                                            │
    # │                                                                  │
    # │ 注意：这个窗口在真实代码里天然存在 —— SELECT 是一次网络往返，    │
    # │ await 会让出控制权。barrier 只是把它撑到必定命中。               │
    # └──────────────────────────────────────────────────────────────────┘
    if barrier is not None:
        await barrier.wait()  # 实验：强制所有协程都读到旧值后再继续
    elif chaos.flag(chaos.DISABLE_ATOMIC_DECR):
        await asyncio.sleep(0.02)  # 服务路径：把窗口拉宽到大概率命中

    if stock < quantity:
        return ReserveOutcome(False, stock, "OUT_OF_STOCK")

    # 用 Python 算好新值再写回 —— 这个值基于已经过期的 stock。
    await conn.execute(
        "UPDATE inventory SET stock = $1, version = version + 1 WHERE sku = $2",
        stock - quantity,
        sku,
    )
    return ReserveOutcome(True, stock - quantity, "")


# ---------------------------------------------------------------------------
# 补偿操作：把库存加回去
# ---------------------------------------------------------------------------
async def release(conn: asyncpg.Connection, sku: str, quantity: int) -> int:
    """Saga 的补偿动作：库存加回去。

    注意这里不需要 `WHERE stock >= $1` 那样的条件 —— 加库存总是安全的。
    补偿操作通常比正向操作简单，因为它不参与资源竞争。

    但它仍然必须**幂等**：补偿也会被重试。幂等由 service 层通过
    reservations.status 保证 —— 已释放过的直接返回，不重复加。
    """
    row = await conn.fetchrow(
        """
        UPDATE inventory
           SET stock = stock + $1,
               version = version + 1
         WHERE sku = $2
        RETURNING stock
        """,
        quantity,
        sku,
    )
    return row["stock"] if row else 0


# ---------------------------------------------------------------------------
# 预留记录：支撑幂等与补偿
# ---------------------------------------------------------------------------
async def find_reservation(conn: asyncpg.Connection, request_id: str) -> dict | None:
    row = await conn.fetchrow(
        "SELECT * FROM reservations WHERE request_id = $1", request_id
    )
    return dict(row) if row else None


async def insert_reservation(
    conn: asyncpg.Connection, request_id: str, sku: str, quantity: int, now_ms: int
) -> str:
    """写入预留记录并返回 reservation_id。

    用 request_id 做幂等的**最后一道防线**：即使两个并发请求同时通过了
    service 层的 find_reservation 检查，唯一约束也只会让一个插入成功。
    不要依赖"应用层先查一下"来保证唯一性 —— 并发下它一定会漏。
    数据库约束是唯一可靠的守门人。
    """
    reservation_id = uuid.uuid4().hex
    await conn.execute(
        """
        INSERT INTO reservations
            (reservation_id, request_id, sku, quantity, status, created_at_ms)
        VALUES ($1, $2, $3, $4, 'RESERVED', $5)
        """,
        reservation_id,
        request_id,
        sku,
        quantity,
        now_ms,
    )
    return reservation_id


async def mark_reservation_released(conn: asyncpg.Connection, reservation_id: str) -> bool:
    """把预留标记为已释放。返回 True 表示这次调用真的执行了释放。

    `AND status = 'RESERVED'` 保证只有第一次调用生效 ——
    第 2..N 次 rowcount 为 0，调用方据此跳过实际的库存回补。
    这就是"补偿动作的幂等"，全靠这一个条件实现。
    """
    result = await conn.execute(
        "UPDATE reservations SET status = 'RELEASED' "
        "WHERE reservation_id = $1 AND status = 'RESERVED'",
        reservation_id,
    )
    return result.endswith(" 1")
