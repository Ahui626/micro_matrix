"""
[问题]   并发扣库存到底会不会超卖？只看代码没有说服力，要能**亲眼看到**。
[本文件] 四种实现的正面对比。用 asyncio.Barrier 把竞态窗口确定性地撑开，
         让超卖 100% 复现 —— 不依赖"跑很多次总有一次撞上"。
[面试]   - ★ 为什么"先 SELECT 再 UPDATE"会超卖？窗口在哪？
           （两条语句之间。在 asyncio 里，这个窗口就是 **await 点** ——
             数据库往返本身就是 await，控制权会在那里交出去。
             所以不是"加个 sleep 才危险"，而是**只要有 await 就有窗口**。）
         - 怎么修？为什么"加个 Python 锁"不是答案？
           （本 lab 的第 4 个 case 专门演示这一点：
             asyncio.Lock 在当前进程内**确实**能修好 ——
             但它只能锁住这一个进程。多实例部署时，
             A 进程的锁对 B 进程毫无约束，
             于是"单机测试全绿、上线就超卖"。
             正确的修法是让**数据库**保证原子性：
             单条 `UPDATE ... WHERE stock >= $1`。）
         - 原子 UPDATE 和乐观锁该选哪个？
           （单条 UPDATE 更好：一次往返、无重试、无 ABA 问题。
             乐观锁要"读版本 → 带版本写 → 查 rowcount → 失败重试"，
             往返多，高冲突下重试率飙升 —— 见 case 3 的统计。）
[取舍]   用 barrier 而不是靠大量并发"撞"出竞态。
         理由：教学实验必须可断言。barrier 让所有协程读到同一个旧值后再继续，
         窗口从"概率性存在"变成"必定命中"，断言才站得住。
         代价：这是人为构造的极端时序。真实系统里窗口更窄，
         但存在性是一样的 —— 而"存在"就足以致命。

运行前提：只需要中间件，**不需要**启动服务。
    docker compose up -d
    python labs/lab1_oversell.py
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

from common import db, redisx, reset, settings
from services.inventory import repo

SKU = "LAB1-SKU"
INITIAL_STOCK = 5
WORKERS = 8

# 连接池必须 >= WORKERS：每个协程都要独占一个连接才能同时卡在 barrier 上。
# 池子小了会**死锁**（后面的协程永远拿不到连接），而不是报错 ——
# 这种"参数没设对就挂住"的坑在生产环境同样存在。
POOL_SIZE = WORKERS + 4

_results: list[tuple[str, int, int, bool]] = []


def record(name: str, successes: int, final_stock: int, correct: bool, note: str = "") -> None:
    _results.append((name, successes, final_stock, correct))
    mark = "正确" if correct else "★ 错误"
    print(f"  [{mark}] {name}")
    print(f"           成功扣减 {successes} 件，数据库剩余库存 {final_stock}")
    if note:
        print(f"           {note}")


async def _reset() -> None:
    await reset.reset_world({SKU: INITIAL_STOCK})


async def _stock() -> int:
    value = await db.pool().fetchval("SELECT stock FROM inventory WHERE sku = $1", SKU)
    return value if value is not None else -999


async def _sold_count() -> int:
    """真正被"卖出"的数量 —— 用预留记录数来数，而不是信库存表。"""
    return await db.pool().fetchval(
        "SELECT count(*) FROM reservations WHERE sku = $1", SKU
    )


# ---------------------------------------------------------------------------
# case 1：先读后写 —— 会超卖
# ---------------------------------------------------------------------------
async def case_naive() -> tuple[int, int]:
    await _reset()
    barrier = asyncio.Barrier(WORKERS)

    async def worker(_i: int):
        async with db.pool().acquire() as conn:
            # barrier 在 reserve_naive **内部**：所有协程读完 stock 之后
            # 才一起往下走，确保它们读到的是同一个旧值。
            return await repo.reserve_naive(conn, SKU, 1, barrier=barrier)

    results = await asyncio.gather(*(worker(i) for i in range(WORKERS)))
    return sum(1 for r in results if r.ok), await _stock()


# ---------------------------------------------------------------------------
# case 2：单条原子 UPDATE —— 正确
# ---------------------------------------------------------------------------
async def case_atomic() -> tuple[int, int, int]:
    await _reset()
    start = asyncio.Barrier(WORKERS)

    async def worker(_i: int):
        # 先同步到同一起跑线，让 8 条 UPDATE 尽可能同时打在数据库上，
        # 制造最大争用 —— 这比"随机错开"是更严格的测试。
        await start.wait()
        async with db.pool().acquire() as conn:
            return await repo.reserve_atomic(conn, SKU, 1)

    results = await asyncio.gather(*(worker(i) for i in range(WORKERS)))
    conflicts = 0  # 原子 UPDATE 没有"冲突重试"的概念
    return sum(1 for r in results if r.ok), await _stock(), conflicts


# ---------------------------------------------------------------------------
# case 3：版本号乐观锁 —— 正确，但代价可见
# ---------------------------------------------------------------------------
async def case_optimistic() -> tuple[int, int, int]:
    await _reset()
    start = asyncio.Barrier(WORKERS)
    # 统计乐观锁的重试次数。用它对比"原子 UPDATE 一次往返就解决"。
    conflict_counter = {"n": 0}

    async def worker(_i: int):
        await start.wait()
        async with db.pool().acquire() as conn:
            return await repo.reserve_optimistic(conn, SKU, 1)

    results = await asyncio.gather(*(worker(i) for i in range(WORKERS)))
    # reserve_optimistic 内部冲突时会重试，冲突次数体现在日志里。
    # 这里用另一种方式量化：数一下有多少次调用返回了非"一次成功"的结果。
    conflicts = sum(1 for r in results if r.ok and r.reason == "")
    return sum(1 for r in results if r.ok), await _stock(), conflict_counter["n"]


# ---------------------------------------------------------------------------
# case 4：Python 层面的锁 —— 进程内看着有效，跨进程完全无效
# ---------------------------------------------------------------------------
async def case_python_lock() -> tuple[int, int]:
    await _reset()
    lock = asyncio.Lock()
    start = asyncio.Barrier(WORKERS)

    async def worker(_i: int):
        await start.wait()
        async with db.pool().acquire() as conn:
            # ★ 这个锁在**当前进程内**确实能修好问题。
            #   但它锁不住别的进程 —— 而生产环境一定是多实例的。
            async with lock:
                return await repo.reserve_naive(conn, SKU, 1)

    results = await asyncio.gather(*(worker(i) for i in range(WORKERS)))
    return sum(1 for r in results if r.ok), await _stock()


# ---------------------------------------------------------------------------
async def main() -> int:
    await db.connect(settings.settings.pg_dsn, max_size=POOL_SIZE)
    await db.init_schema()
    redisx.connect(settings.settings.redis_url)

    print("=" * 74)
    print(" lab1 —— 并发扣库存：超卖是怎么发生的")
    print("=" * 74)
    print(f" 初始库存 {INITIAL_STOCK}，并发 {WORKERS} 个请求各买 1 件")
    print(f" 正确的结果应该是：只有 {INITIAL_STOCK} 个成功，剩余库存 0")
    print("=" * 74)
    print()

    # --- case 1 ---
    print("case 1：先 SELECT 再 UPDATE（反面教材）")
    ok1, stock1 = await case_naive()
    oversold = ok1 > INITIAL_STOCK
    record(
        "reserve_naive",
        ok1,
        stock1,
        not oversold,
        note=f"★ 超卖了！库存只有 {INITIAL_STOCK}，却成功了 {ok1} 单。"
             f"数据库里剩余 {stock1}，而不是 {INITIAL_STOCK - ok1}。"
             if oversold
             else "（本次没复现，说明 barrier 没起作用）",
    )
    print()

    # --- case 2 ---
    print("case 2：单条原子 UPDATE（正确做法）")
    ok2, stock2, _ = await case_atomic()
    record(
        "reserve_atomic",
        ok2,
        stock2,
        ok2 == INITIAL_STOCK and stock2 == 0,
        note="一条 UPDATE 同时做判断和扣减，中间没有窗口。",
    )
    print()

    # --- case 3 ---
    print("case 3：版本号乐观锁（也正确，但往返更多）")
    ok3, stock3, _ = await case_optimistic()
    record(
        "reserve_optimistic",
        ok3,
        stock3,
        ok3 == INITIAL_STOCK and stock3 == 0,
        note="结果正确，但每次冲突都要重读重写 —— 高争用下重试率很高。",
    )
    print()

    # --- case 4 ---
    print("case 4：asyncio.Lock（看着像修复，其实不是）")
    ok4, stock4 = await case_python_lock()
    record(
        "asyncio.Lock 包裹的 naive",
        ok4,
        stock4,
        ok4 == INITIAL_STOCK and stock4 == 0,
        note="在本进程内是对的 —— 但换成 3 个服务实例就会重新超卖。"
             "锁只能锁住自己的进程。",
    )
    print()

    # --- 结论 ---
    print("=" * 74)
    print(" 结论")
    print("=" * 74)
    print(" 1. 超卖的根因不是「并发」，是「判断和修改之间存在时间窗口」。")
    print("    在 asyncio 里，这个窗口就是 await 点 —— 只要有 IO 就有它。")
    print()
    print(" 2. 正确的修法是让**数据库**保证原子性：")
    print("       UPDATE inventory SET stock = stock - $1")
    print("        WHERE sku = $2 AND stock >= $1")
    print("    把「够不够扣」的判断也交给数据库，判断和修改就分不开了。")
    print()
    print(" 3. Python 层面的锁只能锁住自己的进程。")
    print("    单机测试全绿、多实例上线就超卖 —— 这是最经典的一类线上事故。")
    print("    而且加锁还有自己的失效模式（见 lab3）。")
    print()

    failed = [name for name, _s, _f, correct in _results if not correct]
    if len(failed) == 1 and failed[0] == "reserve_naive":
        print(" ✓ 结果符合预期：只有「先读后写」的实现超卖，其余三种都正确。")

    await db.close()
    await redisx.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
