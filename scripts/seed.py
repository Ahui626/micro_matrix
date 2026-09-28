"""
[问题]   首次运行时数据库是空的，任何下单都会得到「库存不足」——
         因为表里根本没有 SKU。需要先播种。
[本文件] 建表（幂等）并给指定 SKU 设置初始库存。带 --reset 时先清空整个环境。
[面试]   - 为什么"第一次跑必须先播种"这件事应该做成脚本，
           而不是写在 README 里让人手动敲？
           （多行代码贴在文档里：复制时容易漏行、引号会被 shell 吃掉、
             而且 PowerShell 和 bash 的多行字符串语法不同 —— 一次只能照顾一种。
             做成脚本则两种 shell 都是同一句 `python scripts/seed.py`。
             这是"把正确做成默认"的一个小例子：与其要求人做对，不如让工具替人做对。）
[取舍]   默认给一个很大的库存（100000），而不是 100。
         理由：这个项目的重点是分布式问题，不是库存耗尽 ——
         默认值太小会让你在读代码时不断撞到"库存不足"，干扰主线。
         想演示库存耗尽，用 --stock 显式传一个小值，或者跑 lab1。

用法：
    python scripts/seed.py                  # 播种（保留已有数据）
    python scripts/seed.py --reset          # 清空环境再播种
    python scripts/seed.py --stock 5        # 小库存，方便演示耗尽
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

import argparse
import asyncio

from common import db, redisx, reset, settings

DEFAULT_SKUS = ["SKU-1", "SKU-2"]
DEFAULT_STOCK = 100_000


async def main() -> int:
    parser = argparse.ArgumentParser(description="给库存表播种 / 重置环境")
    parser.add_argument(
        "--sku", nargs="+", default=DEFAULT_SKUS, help=f"要播种的 SKU（默认 {DEFAULT_SKUS}）"
    )
    parser.add_argument(
        "--stock", type=int, default=DEFAULT_STOCK,
        help=f"每个 SKU 的初始库存（默认 {DEFAULT_STOCK}）",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="先清空数据库和 Redis（订单、outbox、消息、锁、限流桶）再播种",
    )
    args = parser.parse_args()

    await db.connect(settings.settings.pg_dsn)
    # init_schema 是幂等的（全是 IF NOT EXISTS），所以重复运行安全
    await db.init_schema()

    if args.reset:
        redisx.connect(settings.settings.redis_url)
        seed_map = {sku: args.stock for sku in args.sku}
        await reset.reset_world(seed_map)
        await redisx.close()
        print("环境已重置（数据库 + Redis 全部清空）")
    else:
        for sku in args.sku:
            await db.seed_inventory(sku, args.stock)

    # 回读一次，确认真的写进去了 —— 而不是"我以为写进去了"
    print(f"已播种 {len(args.sku)} 个 SKU，每个库存 {args.stock}：")
    for sku in args.sku:
        row = await db.pool().fetchrow(
            "SELECT stock, version FROM inventory WHERE sku = $1", sku
        )
        print(f"  {sku:<10} stock={row['stock']:<8} version={row['version']}")

    await db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
