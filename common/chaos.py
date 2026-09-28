"""
[问题]   labs/ 里的"反面案例"如果要复制一份改坏了的代码，就会有两份实现需要
         各自维护，而且读者看不出它们差在哪。
[本文件] 环境变量控制的故障注入点。lab 不是另一份代码，而是同一份代码上
         打开一个开关。
[面试]   - 混沌工程（chaos engineering）的核心原则是什么？
           （在真实系统上注入受控故障，验证系统行为，而不是靠想象。
             Netflix 的 Chaos Monkey 是这个思路的起点。）
         - 为什么故障注入必须"确定性"而不是靠随机撞？
           （随机故障导致的偶发失败无法验证、无法复现、无法断言。
             可复现是实验的前提。）
[取舍]   用环境变量而不是配置文件 —— 因为 lab 是一个独立进程，
         `MM_CHAOS_X=1 python labs/lab1.py` 这种写法最省事，
         不需要改任何文件、也不需要重启别的服务。

用法：
    # 在 order 进程里注入：写完订单后立刻崩溃
    MM_CHAOS_FAIL_AFTER_ORDER_INSERT=1 python services/order/main.py

    # 让 inventory 变慢 800ms
    MM_CHAOS_INVENTORY_DELAY_MS=800 python services/inventory/main.py

命名约定：MM_CHAOS_<注入点名>。
所有注入点在**正常运行时**都必须是关闭的 —— 默认值统一为"不注入"。
"""

from __future__ import annotations

import asyncio
import os
import random

PREFIX = "MM_CHAOS_"


def flag(name: str) -> bool:
    """读一个布尔开关。1 / true / yes 都算打开。"""
    return os.getenv(PREFIX + name, "").strip().lower() in {"1", "true", "yes", "on"}


def number(name: str, default: float = 0.0) -> float:
    """读一个数值参数（毫秒、概率等）。"""
    raw = os.getenv(PREFIX + name, "")
    if not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def text(name: str, default: str = "") -> str:
    raw = os.getenv(PREFIX + name, "")
    return raw if raw.strip() else default


# ---------------------------------------------------------------------------
# 具体的注入点。每加一个，都在这里登记，让"系统能被怎么破坏"一目了然。
# 这个清单本身就是文档。
# ---------------------------------------------------------------------------

# --- 订单路径 ---
FAIL_AFTER_ORDER_INSERT = "FAIL_AFTER_ORDER_INSERT"  # 插入订单后崩溃 → 验证 outbox 的必要性
SKIP_OUTBOX_WRITE = "SKIP_OUTBOX_WRITE"  # 写订单但不写 outbox → 验证"事件丢失"
PAYMENT_FAIL = "PAYMENT_FAIL"  # 支付步骤失败 → 触发 Saga 补偿
PAYMENT_DELAY_MS = "PAYMENT_DELAY_MS"  # 支付变慢 → 撑爆锁 TTL / deadline

# --- 库存路径 ---
DISABLE_ATOMIC_DECR = "DISABLE_ATOMIC_DECR"  # 把原子的 UPDATE 换成"先读后写" → 制造超卖
INVENTORY_DELAY_MS = "INVENTORY_DELAY_MS"  # 库存服务变慢 → 制造重试风暴
INVENTORY_ERROR_RATE = "INVENTORY_ERROR_RATE"  # 按概率返回失败 → 触发熔断

# --- 消费路径 ---
ACK_BEFORE_PROCESS = "ACK_BEFORE_PROCESS"  # 先 ack 再处理 → at-most-once，崩溃即丢消息
CRASH_BEFORE_ACK = "CRASH_BEFORE_ACK"  # 处理完但 ack 前崩溃 → 消息会被重投 → 验证消费幂等
SKIP_CONSUMER_IDEMPOTENCY = "SKIP_CONSUMER_IDEMPOTENCY"  # 关掉消费端幂等 → 重复投递即重复副作用

# --- 锁 ---
LOCK_TTL_MS = "LOCK_TTL_MS"  # 覆盖锁的 TTL → 制造"业务没做完锁就过期"


# ---------------------------------------------------------------------------
def describe() -> str:
    """把所有打开的注入点列出来。

    为什么需要它：一个 lab 跑出"错误"结果时，第一件要确认的事是
    "这错误是我故意注入的，不是真 bug"。启动日志里打印这段，
    就不会出现"我以为在演示超卖，其实是代码写错了"这种混乱。
    """
    active = [
        key for key in sorted(os.environ)
        if key.startswith(PREFIX) and os.environ[key].strip()
    ]
    if not active:
        return "（无故障注入，系统处于正常运行状态）"
    return "激活的故障注入：" + ", ".join(f"{k}={os.environ[k]}" for k in active)


# ---------------------------------------------------------------------------
async def crash_if(name: str) -> None:
    """如果该注入点打开，立刻杀掉当前进程。

    用 os._exit 而不是 sys.exit / raise：os._exit 不做任何清理、不触发
    finally、不 flush 缓冲区 —— 这正是"进程被 kill -9"的真实形态。
    用 raise 的话异常会被上层 except 捕获，就演示不出"崩溃"了。
    """
    if flag(name):
        # 先把要说的话写出去，再死。否则读者不知道发生了什么。
        os.write(2, f"[chaos] 注入 {name}：进程自杀（模拟崩溃）\n".encode())
        os._exit(3)


async def delay_if(name: str, extra_ms: float = 0.0) -> None:
    """如果该注入点打开，睡眠指定毫秒数。"""
    ms = number(name, 0.0) + extra_ms
    if ms > 0:
        await asyncio.sleep(ms / 1000.0)


def should_fail_randomly(name: str) -> bool:
    """按概率决定这次调用是否失败。用于制造间歇性故障（触发熔断）。"""
    rate = number(name, 0.0)
    return rate > 0 and random.random() < rate
