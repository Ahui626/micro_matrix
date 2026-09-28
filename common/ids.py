"""
[问题]   订单 ID 用什么生成？数据库自增在分库分表后一定会冲突，
         UUID 无序且占空间。
[本文件] Snowflake：64 位整数 = 41 位时间戳 + 5 位数据中心 + 5 位机器 + 12 位序列号。
[面试]   - 64 位是怎么分配的？为什么时间戳放高位？
           （高位放时间戳 → 生成的 ID 整体趋势递增 → 作为主键时 B+ 树
             是顺序追加而不是随机插入，避免频繁页分裂。）
         - 时钟回拨会怎样？怎么处理？
           （可能生成重复 ID。三种处理：等待追平 / 直接报错 / 借位。
             本项目选"等待"，见下方 next_id 的注释。）
         - 为什么不用 UUID 当主键？
           （128 位 vs 64 位，索引更大；且完全无序，随机写入会导致
             索引页分裂和缓存命中率下降。）
         - worker_id 冲突会发生什么？（生成重复 ID，且很难排查。）
[取舍]   时钟回拨处理选"小偏差等待、大偏差报错"。
         不选"直接报错"：NTP 校时导致几毫秒回拨是常态，报错会让服务
         在那一刻整体不可用。不选"借位扩展"：逻辑复杂，收益在这个规模下不值得。
         这是典型的工程权衡——没有最优解，只有可接受的最坏情况。
"""

from __future__ import annotations

import threading
import time

# 自定义纪元（2023-11-14 22:13:20 UTC）。
# 为什么不用 Unix 纪元：41 位时间戳从 Unix 纪元起算只够用到 2039 年。
# 挪一个近的纪元，可用年限直接推到 2090 年以后。
EPOCH_MS = 1_700_000_000_000

# 位数分配。加起来必须是 64。
TIMESTAMP_BITS = 41
DATACENTER_BITS = 5
WORKER_BITS = 5
SEQUENCE_BITS = 12

MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1  # 4095，即每毫秒最多 4096 个 ID
WORKER_SHIFT = SEQUENCE_BITS                                    # 12
DATACENTER_SHIFT = SEQUENCE_BITS + WORKER_BITS                  # 17
TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_BITS + DATACENTER_BITS # 22

# 时钟回拨超过这个毫秒数就直接报错，不再等待。
# 为什么不无限等待：如果回拨了几分钟，等待就等于服务不可用。
# 这时宁可快速失败、让运维介入，也不要静默地挂在那里。
MAX_BACKWARD_TOLERANCE_MS = 5


class ClockMovedBackwards(RuntimeError):
    """时钟回拨幅度过大。这通常意味着 NTP 出了严重问题，需要人工介入。"""


class SnowflakeGenerator:
    """线程安全的 Snowflake 生成器。

    一个进程一个实例。多进程部署时，每个进程必须配不同的 worker_id。
    """

    def __init__(self, worker_id: int, datacenter_id: int = 0) -> None:
        if not 0 <= worker_id < (1 << WORKER_BITS):
            raise ValueError(f"worker_id 必须在 0..{(1 << WORKER_BITS) - 1} 之间，收到 {worker_id}")
        if not 0 <= datacenter_id < (1 << DATACENTER_BITS):
            raise ValueError(
                f"datacenter_id 必须在 0..{(1 << DATACENTER_BITS) - 1} 之间，收到 {datacenter_id}"
            )
        self._worker_id = worker_id
        self._datacenter_id = datacenter_id
        self._sequence = 0
        self._last_ts = -1
        self._lock = threading.Lock()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def next_id(self) -> int:
        with self._lock:
            ts = self._now_ms()

            # --- 时钟回拨处理 ---
            if ts < self._last_ts:
                drift = self._last_ts - ts
                if drift > MAX_BACKWARD_TOLERANCE_MS:
                    raise ClockMovedBackwards(
                        f"时钟回拨 {drift}ms，超过容忍上限 {MAX_BACKWARD_TOLERANCE_MS}ms"
                    )
                # 小幅度回拨：睡到追上上次的时间戳为止。
                # 注意这里 time.sleep 是在 async 服务里被调用的——
                # 它会短暂阻塞事件循环。因为偏差 ≤5ms 且一年也遇不到几次，
                # 这里接受这个代价。生产环境更稳妥的做法是预分配一些 ID 备用。
                time.sleep(drift / 1000.0)
                ts = self._now_ms()
                # 睡醒后可能还是没追上（sleep 精度问题），兜底用上次的时间戳
                if ts < self._last_ts:
                    ts = self._last_ts

            if ts == self._last_ts:
                # 同一毫秒内的后续请求：序列号 +1
                self._sequence = (self._sequence + 1) & MAX_SEQUENCE
                if self._sequence == 0:
                    # 这一毫秒的 4096 个号用完了，自旋等到下一毫秒。
                    # 这是该算法唯一的性能上限：单机每毫秒 4096 个 ID。
                    ts = self._spin_to_next_ms(self._last_ts)
            else:
                # 进入新的毫秒，序列号归零
                self._sequence = 0

            self._last_ts = ts

            # 把四段拼成一个 64 位整数。
            # 时间戳在最高位 —— 保证 ID 整体随时间递增，这是它能当主键的前提。
            return (
                ((ts - EPOCH_MS) << TIMESTAMP_SHIFT)
                | (self._datacenter_id << DATACENTER_SHIFT)
                | (self._worker_id << WORKER_SHIFT)
                | self._sequence
            )

    def _spin_to_next_ms(self, last_ts: int) -> int:
        ts = self._now_ms()
        while ts <= last_ts:
            ts = self._now_ms()
        return ts


def parse_id(snowflake_id: int) -> dict[str, int]:
    """从 ID 里还原出各段。

    这是 Snowflake 的一个隐藏好处：ID 自带生成时间和机器号，
    排查问题时可以反查出"这个订单是哪台机器、什么时候生成的"，
    不需要额外存这些字段。
    """
    return {
        "timestamp_ms": (snowflake_id >> TIMESTAMP_SHIFT) + EPOCH_MS,
        "datacenter_id": (snowflake_id >> DATACENTER_SHIFT) & ((1 << DATACENTER_BITS) - 1),
        "worker_id": (snowflake_id >> WORKER_SHIFT) & ((1 << WORKER_BITS) - 1),
        "sequence": snowflake_id & MAX_SEQUENCE,
    }
