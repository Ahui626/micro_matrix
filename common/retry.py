"""
[问题]   下游抖一下，重试就好了。但"重试"做错了会把小故障放大成雪崩：
         下游本来就慢，你还把请求量乘 3 打过去，它永远好不了。
[本文件] 只对可重试错误重试 + 指数退避 + full jitter + 总预算约束。
[面试]   - 一次安全的重试要满足哪三个条件？
           1. **只重试可重试的错误** —— 参数错误重试一万次还是错
           2. **必须有退避** —— 立即重试等于持续打压
           3. **必须有抖动** —— 否则所有客户端会同步重试，形成尖峰
           缺任何一条，重试都会从"自愈机制"变成"故障放大器"。
         - 抖动为什么必须有？
           （1000 个客户端同时失败、同时等 1 秒、同时重试，
             下游收到一个 1000 QPS 的瞬时尖峰，再次被打垮 —— 循环。
             加了随机抖动，重试被摊开在时间轴上。）
         - 重试次数和 deadline 是什么关系？
           （重试必须活在总预算内。上游只给 500ms，你重试 3 次每次等 1 秒，
             第 2 次还没发出去上游就超时了，剩下的重试全是浪费。）
         - 为什么传进来的是"协程工厂"而不是"协程"？
           （协程对象只能被 await 一次，重复 await 会抛
             "cannot reuse already awaited coroutine"。重试必须每次都新建一个。）
[取舍]   用了 "full jitter"：sleep = random(0, base * 2^attempt)。
         AWS 的实测结论是它在总耗时和峰值负载两方面都优于等值抖动和比例抖动，
         而且实现最简单。这是少数"又简单又更好"的工程选择。
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Awaitable, Callable, Optional, TypeVar

import grpc

from common import errors

log = logging.getLogger(__name__)

T = TypeVar("T")

# 退避的基准和上限。基准取 50ms：太短起不到错开作用，太长会让正常重试也变慢。
DEFAULT_BASE_S = 0.05
DEFAULT_CAP_S = 2.0


def backoff_delay(attempt: int, base_s: float = DEFAULT_BASE_S, cap_s: float = DEFAULT_CAP_S) -> float:
    """算第 attempt 次重试前该等多久（秒）。attempt 从 0 开始。

    两个动作：
      指数增长 —— base * 2^attempt，给下游越来越多的时间恢复
      封顶 —— 不能无限增长，否则第 10 次重试要等十几分钟
      抖动 —— 在 [0, ceiling] 里随机取值，把重试摊开
    """
    ceiling = min(cap_s, base_s * (2**attempt))
    return random.uniform(0, ceiling)


def default_is_retryable(exc: BaseException) -> bool:
    """默认的重试判定：只有明确知道可重试的才重试。

    用白名单而不是黑名单。理由：错误地重试比不重试更危险 ——
    不重试只是这一次失败，错误重试会放大故障。
    """
    if isinstance(exc, grpc.aio.AioRpcError):
        return errors.is_retryable(exc.code())
    if isinstance(exc, errors.BusinessError):
        return exc.retryable
    # 未知异常一律不重试。
    return False


async def call_with_retry(
    make_call: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    budget_s: Optional[float] = None,
    is_retryable: Optional[Callable[[BaseException], bool]] = None,
    label: str = "rpc",
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
) -> T:
    """带重试地执行一次调用。

    参数说明：
      make_call   —— 协程**工厂**，不是一个协程对象。每次重试都会调用它重新创建。
      budget_s    —— 这次重试总共允许花多少秒。None 表示不限制（仅用于演示，
                     生产环境调用链上的每一跳都应该有预算）。
      is_retryable—— 自定义的重试判定，默认用 default_is_retryable。
      on_retry    —— 每次决定重试时的回调，用来记录指标。

    返回值：make_call 的返回值。
    异常：最后一次失败会原样抛出（如果不是可重试错误，则第一次就抛出）。
    """
    should_retry = is_retryable or default_is_retryable
    loop = asyncio.get_running_loop()
    started = loop.time()
    last_exc: BaseException | None = None

    for attempt in range(attempts):
        try:
            return await make_call()
        except Exception as exc:
            last_exc = exc
            is_last_attempt = attempt == attempts - 1

            if not should_retry(exc):
                # 不可重试：立刻放弃，不做任何多余的等待。
                # 这是最容易被忽略的一条 —— 很多实现无脑重试，把
                # 一个"参数错误"重试三次，白白让用户多等 150ms。
                raise

            if is_last_attempt:
                raise

            delay = backoff_delay(attempt)

            # 预算检查：如果等完这个退避就已经超出总预算，那重试没有意义 ——
            # 上游早就超时走了，我们这边还在傻等。提前放弃。
            if budget_s is not None:
                elapsed = loop.time() - started
                if elapsed + delay >= budget_s:
                    log.warning(
                        "[%s] 预算不足，放弃第 %d 次重试（已用 %.0fms，还需等 %.0fms，预算 %.0fms）",
                        label, attempt + 2, elapsed * 1000, delay * 1000, budget_s * 1000,
                    )
                    raise

            log.warning(
                "[%s] 第 %d 次失败（%s），%.0fms 后重试",
                label, attempt + 1, type(exc).__name__, delay * 1000,
            )
            if on_retry is not None:
                on_retry(attempt, exc, delay)

            await asyncio.sleep(delay)

    # 理论上不可达（循环里要么 return 要么 raise），兜底防止静态检查报错。
    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# 反面教材：把它打开可以复现 labs/lab5 的"重试风暴"
#
# 这个函数和 call_with_retry 唯一的区别是**没有抖动**：
# 所有并发调用者都会在完全相同的时刻重试，形成同步尖峰。
# 保留它不是为了用，而是为了让读者能亲手对比"有抖动"和"没抖动"的区别 ——
# 光看文字说明是感受不到那个尖峰的。
# ---------------------------------------------------------------------------
async def call_with_retry_no_jitter(
    make_call: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    delay_s: float = 0.05,
    label: str = "rpc-no-jitter",
) -> T:
    """没有抖动的重试 —— 教学用的反例，不要在真实项目里这么写。"""
    for attempt in range(attempts):
        try:
            return await make_call()
        except Exception as exc:
            if attempt == attempts - 1 or not default_is_retryable(exc):
                raise
            # 固定延迟、无抖动：所有调用者会同步重试。
            log.warning("[%s] 第 %d 次失败，%.0fms 后重试（无抖动）", label, attempt + 1, delay_s * 1000)
            await asyncio.sleep(delay_s)
    raise RuntimeError("unreachable")
