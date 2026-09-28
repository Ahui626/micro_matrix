"""
[问题]   下游已经挂了，你的每个请求还在老老实实等 3 秒超时。
         这些等待会占满连接和内存，最后你也挂了 —— 一个下游故障
         通过"等待"传染成了全站故障。
[本文件] 熔断器：连续失败到阈值就直接快速失败，冷却一段时间后放一个探测请求试水。
[面试]   - 三个状态是什么？怎么转换？
           （CLOSED 正常放行 →（失败达阈值）→ OPEN 直接拒绝
             →（冷却到）→ HALF_OPEN 放行**一个**探测
             →（成功）CLOSED /（失败）立刻回 OPEN）
         - 熔断和限流的区别？
           （限流控制"进入自己"的速率，保护自己不被流量压垮；
             熔断减少"自己对下游"的调用，避免被已故障的下游拖死。
             一个管入口，一个管出口。）
         - HALF_OPEN 为什么只放一个请求？
           （放开一批，还没恢复的下游会再次被打垮，
             前面的等待全部白费。探测必须是最小代价的。）
         - 熔断和重试冲突吗？该谁套谁？
           （冲突。重试**增加**调用量，熔断**减少**调用量。
             正确顺序是熔断在外、重试在内：熔断打开时直接拒绝，
             根本不给重试机会。反过来写的话，熔断永远拦不住重试产生的放大。）
         - 熔断打开时该返回什么？
           （**降级结果**，不是错误。比如返回缓存里的旧库存、
             返回"系统繁忙请稍后再试"、或者走备用逻辑。
             熔断的价值一半在"少打下游"，一半在"给用户一个体面的响应"。）
[取舍]   用"连续失败计数"而不是"滑动窗口失败率"。
         窗口法更精确（能表达"失败率超过 50% 才熔断"），
         但需要环形缓冲 + 定时清理，代码量翻三倍。
         连续计数 20 行就能演示全部状态转换，教学够用。
         代价：偶尔一次失败也会累计，没有"低频失败不熔断"的精度。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

CLOSED = "CLOSED"
OPEN = "OPEN"
HALF_OPEN = "HALF_OPEN"


class CircuitOpen(Exception):
    """熔断器处于打开状态，本次调用被快速拒绝（没有真正打到下游）。"""

    def __init__(self, name: str) -> None:
        super().__init__(f"熔断器 [{name}] 已打开，快速失败")
        self.name = name


class CircuitBreaker:
    """一个下游对应一个熔断器实例。

    注意：状态是**进程内**的。三个服务实例就有三份独立的熔断状态。
    这通常没问题（每个实例按自己的健康状况决定），但如果想全局协调，
    就需要把状态放到 Redis 里 —— 代价是每次调用多一次网络往返。
    这是"精确性 vs 开销"的典型权衡。
    """

    def __init__(
        self,
        name: str,
        fail_threshold: int = 5,
        reset_timeout_s: float = 3.0,
    ) -> None:
        self.name = name
        self._fail_threshold = fail_threshold
        self._reset_timeout_s = reset_timeout_s

        self._state = CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_in_flight = False

        # 指标。熔断器没有指标就是黑盒 ——
        # 你只知道"被拒绝了"，不知道为什么、什么时候开始的。
        self.stats = {"allowed": 0, "rejected": 0, "tripped": 0, "recovered": 0}

    # --- 状态读取 ---------------------------------------------------------
    def _resolve_state(self) -> str:
        """惰性推进 OPEN → HALF_OPEN。

        为什么用惰性而不是起一个定时任务：定时任务需要管理生命周期
        （启动、取消、异常处理），而"冷却时间到了就转换"这件事
        完全可以等到下次有人调用时再判断。少一个后台任务，
        就少一类"任务泄漏"的 bug。
        """
        if self._state == OPEN:
            elapsed = asyncio.get_running_loop().time() - self._opened_at
            if elapsed >= self._reset_timeout_s:
                self._state = HALF_OPEN
                self._half_open_in_flight = False
                log.info("[%s] 冷却 %.1fs 结束 → HALF_OPEN，放行一个探测请求", self.name, elapsed)
        return self._state

    @property
    def state(self) -> str:
        return self._resolve_state()

    # --- 主入口 -----------------------------------------------------------
    async def call(
        self,
        make_call: Callable[[], Awaitable[T]],
        fallback: Optional[Callable[[], Awaitable[T]]] = None,
    ) -> T:
        """通过熔断器执行一次调用。

        make_call —— 协程工厂（同 retry.py，重试需要重新创建协程）
        fallback  —— 熔断打开时的降级逻辑。这是熔断真正有用的地方：
                     不是"报错"，而是"给一个体面的替代结果"。
        """
        state = self._resolve_state()

        if state == OPEN:
            self.stats["rejected"] += 1
            if fallback is not None:
                log.debug("[%s] 已熔断，走降级逻辑", self.name)
                return await fallback()
            raise CircuitOpen(self.name)

        if state == HALF_OPEN:
            if self._half_open_in_flight:
                # 已经有一个探测请求在途，其余一律拒绝。
                # 这就是"只放一个"的实现。
                self.stats["rejected"] += 1
                if fallback is not None:
                    return await fallback()
                raise CircuitOpen(self.name)
            self._half_open_in_flight = True

        try:
            result = await make_call()
        except Exception:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result

    # --- 状态迁移 ---------------------------------------------------------
    def _on_success(self) -> None:
        self.stats["allowed"] += 1
        if self._state == HALF_OPEN:
            # 探测成功 → 下游恢复了，完全关闭并清零。
            self._state = CLOSED
            self._consecutive_failures = 0
            self._half_open_in_flight = False
            self.stats["recovered"] += 1
            log.info("[%s] 探测成功 → CLOSED，恢复正常放行", self.name)
        else:
            # 正常成功，清零连续失败计数。
            # 注意是"连续"—— 中间成功一次就重新数，这正是连续计数法的特点。
            self._consecutive_failures = 0

    def _on_failure(self) -> None:
        self._consecutive_failures += 1
        self._half_open_in_flight = False

        if self._state == HALF_OPEN:
            # 探测失败 → 立刻回到 OPEN，重新计时冷却。
            # 不给半开状态任何"再试几次"的机会，避免在恢复期持续冲击下游。
            self._trip(reason="探测请求失败")
            return

        if self._consecutive_failures >= self._fail_threshold:
            self._trip(reason=f"连续失败 {self._consecutive_failures} 次达阈值")

    def _trip(self, reason: str) -> None:
        self._state = OPEN
        self._opened_at = asyncio.get_running_loop().time()
        self.stats["tripped"] += 1
        log.warning("[%s] → OPEN（%s），%.1fs 内快速失败", self.name, reason, self._reset_timeout_s)
