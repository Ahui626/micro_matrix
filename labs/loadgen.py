"""
[问题]   要验证"并发下会不会超卖"，得先能发出真正的并发请求。
[本文件] 纯 asyncio 的并发压测器。既能当命令行工具用，也能被别的 lab import。
[面试]   - 为什么不用 ab / hey / wrk？
           （它们在 Windows 上要么没有、要么难装。
             而且用纯 Python 写，读者能看懂每一行 ——
             压测器本身不该是个黑盒，否则你无法确定它测的是什么。）
         - ★ 为什么"串行循环发 100 次"不是并发测试？
           （每次请求之间的耗时会让状态被重置。
             比如令牌桶限流：串行发 100 次耗时 5 秒，
             令牌按 20/s 补充了 100 个，桶永远是满的，限流永远不会触发。
             只有真正并发（同一时刻有大量在途请求）才能压出问题。
             这个坑我在验证限流时就踩过一次。）
         - 为什么要用 Semaphore 限制并发度？
           （不限的话 10000 个协程会同时打开 10000 个连接，
             先把自己打挂 —— 你测的是客户端，不是服务端。
             真实压测工具都有并发度参数，就是这个原因。）
[取舍]   用信号量控制并发，而不是 asyncio.gather 一把梭。
         代价：多两行代码。
         收益：能复现"固定并发度下持续施压"这个真实场景，
         也能避免把自己压挂导致的假阳性。
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
import time
from dataclasses import dataclass, field

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000"


@dataclass
class LoadStats:
    """一次压测的统计结果。"""

    total: int = 0
    statuses: dict = field(default_factory=dict)
    latencies: list[float] = field(default_factory=list)
    errors: dict = field(default_factory=dict)

    def percentile(self, p: float) -> float:
        if not self.latencies:
            return 0.0
        ordered = sorted(self.latencies)
        idx = max(0, min(len(ordered) - 1, int(round(p / 100.0 * len(ordered) + 0.5)) - 1))
        return ordered[idx]

    @property
    def succeeded(self) -> int:
        return self.statuses.get(201, 0)

    @property
    def rate_limited(self) -> int:
        return self.statuses.get(429, 0)

    @property
    def failed(self) -> int:
        return sum(v for k, v in self.statuses.items() if k >= 400 and k != 429)

    def summary(self) -> str:
        parts = [f"总计 {self.total}"]
        parts.append("状态码 " + ", ".join(f"{k}×{v}" for k, v in sorted(self.statuses.items())))
        if self.errors:
            parts.append("异常 " + ", ".join(f"{k}×{v}" for k, v in self.errors.items()))
        parts.append(f"p50={self.percentile(50)*1000:.0f}ms p99={self.percentile(99)*1000:.0f}ms")
        return " | ".join(parts)


async def burst_orders(
    n: int,
    concurrency: int = 50,
    sku: str = "SKU-1",
    quantity: int = 1,
    user_id: str = "loadgen",
    base_url: str = DEFAULT_BASE_URL,
    request_id_prefix: str = "lg",
    timeout_s: float = 30.0,
) -> LoadStats:
    """并发下单 n 次，最多同时有 concurrency 个在途请求。"""
    stats = LoadStats(total=n)
    semaphore = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(
        base_url=base_url, timeout=timeout_s, limits=limits
    ) as client:

        async def one(i: int) -> None:
            # 信号量保证"同时在途的请求数"不超过 concurrency。
            # 这是压测工具的标准做法 —— 不限并发的话会先把自己打挂。
            async with semaphore:
                start = time.perf_counter()
                try:
                    resp = await client.post(
                        "/orders",
                        json={
                            "user_id": user_id,
                            "sku": sku,
                            "quantity": quantity,
                            "request_id": f"{request_id_prefix}-{i}",
                        },
                    )
                    stats.statuses[resp.status_code] = stats.statuses.get(resp.status_code, 0) + 1
                except Exception as exc:
                    name = type(exc).__name__
                    stats.errors[name] = stats.errors.get(name, 0) + 1
                finally:
                    stats.latencies.append(time.perf_counter() - start)

        await asyncio.gather(*(one(i) for i in range(n)))

    return stats


async def _sustained(
    total: int, concurrency: int, duration_s: float, **kwargs
) -> LoadStats:
    """在指定时长内持续施压，用来观察稳态行为（lab5 需要）。"""
    stats = LoadStats()
    stop_at = time.perf_counter() + duration_s
    semaphore = asyncio.Semaphore(concurrency)
    counter = {"n": 0}

    async with httpx.AsyncClient(base_url=kwargs.pop("base_url", DEFAULT_BASE_URL), timeout=30.0) as client:

        async def one() -> None:
            async with semaphore:
                if time.perf_counter() >= stop_at:
                    return
                counter["n"] += 1
                idx = counter["n"]
                start = time.perf_counter()
                try:
                    resp = await client.post(
                        "/orders",
                        json={
                            "user_id": kwargs.get("user_id", "loadgen"),
                            "sku": kwargs.get("sku", "SKU-1"),
                            "quantity": kwargs.get("quantity", 1),
                            "request_id": f"st-{idx}",
                        },
                    )
                    stats.statuses[resp.status_code] = stats.statuses.get(resp.status_code, 0) + 1
                except Exception as exc:
                    name = type(exc).__name__
                    stats.errors[name] = stats.errors.get(name, 0) + 1
                finally:
                    stats.latencies.append(time.perf_counter() - start)
                    stats.total += 1

        # 反复补位，直到时间用完
        while time.perf_counter() < stop_at:
            await asyncio.gather(*(one() for _ in range(concurrency)))

    return stats


async def main() -> int:
    parser = argparse.ArgumentParser(description="micro_matrix 并发压测器")
    parser.add_argument("--n", type=int, default=100, help="总请求数")
    parser.add_argument("--concurrency", type=int, default=50, help="并发度（同时在途的请求数）")
    parser.add_argument("--sku", default="SKU-1")
    parser.add_argument("--qty", type=int, default=1, help="每个请求买多少")
    parser.add_argument("--user", default="loadgen", help="用户 ID（限流按用户维度）")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    args = parser.parse_args()

    print(f"对 {args.base_url} 发起 {args.n} 个请求，并发度 {args.concurrency}…")
    start = time.perf_counter()
    stats = await burst_orders(
        n=args.n,
        concurrency=args.concurrency,
        sku=args.sku,
        quantity=args.qty,
        user_id=args.user,
        base_url=args.base_url,
    )
    elapsed = time.perf_counter() - start

    print()
    print(stats.summary())
    print(f"耗时 {elapsed:.2f}s → 吞吐 {stats.total / elapsed:.0f} req/s")
    if stats.rate_limited:
        print(
            f"注意：{stats.rate_limited} 个请求被限流（429）。"
            f"当前每用户配额见 MM_RATE_LIMIT_PER_SEC，默认 20/s。"
            f"压测时要么提高配额、要么给每个请求用不同的 user_id。"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
