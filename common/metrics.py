"""
[问题]   系统跑起来之后，怎么知道它健不健康？变慢了是谁变慢了？
[本文件] 极简的进程内指标：计数器 + 延迟样本（用来算 p50/p95/p99）。
[面试]   - 可观测性的三大支柱是什么？
           （日志 log：发生了什么；指标 metric：系统整体什么状态；
             链路追踪 trace：一次请求经过了哪些环节。
             三者互补 —— 指标告诉你"出问题了"，trace 告诉你"问题在哪一跳"，
             日志告诉你"具体为什么"。）
         - ★ 为什么看 p99 而不是平均值？
           （平均值会被大量快请求稀释。假设 99% 的请求 10ms、1% 的请求 3 秒，
             平均值只有约 40ms，看起来很健康 —— 但那 1% 的用户正在骂人。
             尾延迟才是用户体验的真实反映，也是容量规划的依据。
             而且注意：把 10 个服务的 p99 加起来，得到的不是真实的长尾。）
         - 为什么这里手写而不用 prometheus_client？
           （prometheus_client 的 Summary 会隐藏"分位数怎么算出来的"。
             手写这几十行，能亲眼看到 p99 就是把样本排序取第 99 百分位 ——
             而且能顺便理解"进程内分位数无法跨实例聚合"这个局限。）
[取舍]   延迟样本用**有界环形缓冲**（deque maxlen），而不是全量保留。
         代价：分位数是"最近 N 个请求"的，不是历史全量。
         收益：内存不会随运行时间无限增长。
         生产环境用 Prometheus 的直方图桶（histogram），能跨实例聚合，
         且分位数在查询时算 —— 那是更正确的做法，
         代价是桶边界需要调优，而且分位数是近似的。
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager


class Metrics:
    """进程内指标收集器。

    注意"进程内"这三个字：多实例部署时每个实例各有一份，
    要得到全局视图必须把数据推到 Prometheus 这类系统里聚合。
    这也是为什么生产环境不自己手写而用 Prometheus —— 不是算不出来的问题，
    是**聚合**的问题。
    """

    def __init__(self, max_samples: int = 2000) -> None:
        self._counters: dict[str, float] = defaultdict(float)
        self._samples: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=max_samples)
        )

    # --- 写入 ---------------------------------------------------------
    def inc(self, name: str, value: float = 1.0) -> None:
        self._counters[name] += value

    def observe(self, name: str, seconds: float) -> None:
        self._samples[name].append(seconds)

    @asynccontextmanager
    async def timer(self, name: str):
        """用法：async with metrics.timer("order.create"): ..."""
        start = time.perf_counter()
        try:
            yield
        finally:
            # perf_counter 是单调时钟，不受系统时间调整影响 ——
            # 测延迟必须用它，绝不能用 time.time()。
            self.observe(name, time.perf_counter() - start)

    # --- 读取 ---------------------------------------------------------
    def percentile(self, name: str, p: float) -> float:
        """算第 p 百分位（p 取 0..100）。"""
        samples = self._samples.get(name)
        if not samples:
            return 0.0
        ordered = sorted(samples)
        # 用 nearest-rank 法：第 p 百分位 = 排序后第 ceil(p/100 * n) 个元素。
        # 真实的百分位算法有十几种（最近秩、线性插值、P² 算法……），
        # 没有"正确"答案，只有"约定" —— 重要的是整个团队用同一种。
        index = max(0, min(len(ordered) - 1, int(round(p / 100.0 * len(ordered) + 0.5)) - 1))
        return ordered[index]

    def count(self, name: str) -> float:
        return self._counters.get(name, 0.0)

    def sample_count(self, name: str) -> int:
        return len(self._samples.get(name, ()))

    def snapshot(self) -> dict:
        return {
            "counters": dict(self._counters),
            "latency": {
                name: {
                    "count": len(samples),
                    "p50_ms": round(self.percentile(name, 50) * 1000, 2),
                    "p95_ms": round(self.percentile(name, 95) * 1000, 2),
                    "p99_ms": round(self.percentile(name, 99) * 1000, 2),
                    "max_ms": round(max(samples) * 1000, 2) if samples else 0.0,
                }
                for name, samples in self._samples.items()
            },
        }

    def render_prometheus(self) -> str:
        """/metrics 端点的文本输出。

        格式模仿 Prometheus 的文本协议，所以你能直接用 curl 看，
        也能真的接一个 Prometheus 来抓。
        """
        lines: list[str] = []
        for name, value in sorted(self._counters.items()):
            safe = name.replace(".", "_").replace("-", "_")
            lines.append(f"mm_counter_{safe} {value}")
        for name, samples in sorted(self._samples.items()):
            if not samples:
                continue
            safe = name.replace(".", "_").replace("-", "_")
            for pct in (50, 95, 99):
                lines.append(f"mm_latency_p{pct}_ms_{safe} {self.percentile(name, pct) * 1000:.2f}")
            lines.append(f"mm_latency_max_ms_{safe} {max(samples) * 1000:.2f}")
            lines.append(f"mm_latency_count_{safe} {len(samples)}")
        return "\n".join(lines) + "\n"


# 全进程共用一份。gRPC 服务也可以往里记，这里为了简洁只在 gateway 用。
metrics = Metrics()
