"""
[问题]   全项目的配置从哪来、在哪读。
[本文件] 模块级单例 settings。所有环境变量只在这一处出现，其它任何文件
         都不许再写 os.getenv。
[面试]   - 为什么配置要集中读取，而不是每个模块自己 os.getenv？
           （可发现性：想知道 deploy 时要设哪些变量，只看这一个文件。
             可测试性：改配置只有一个入口。避免"某个隐藏的默认值"导致
             本地能跑线上挂掉。）
         - 12-Factor App 的第三条"配置存于环境"要解决什么问题？
           （构建产物在不同环境间不可变。同一个镜像，靠环境变量区分环境。）
[取舍]   没用 pydantic-settings —— 少一个依赖，而且手写这几十行反而更能看清
         "配置"本身是什么。代价是没有自动类型校验，所以每个 getenv 都要显式
         转换，写错了要等运行时才炸。
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# 所有环境变量统一以 MM_（micro_matrix）开头，避免和系统里别的变量撞名。
PREFIX = "MM_"


def _str(name: str, default: str) -> str:
    value = os.getenv(PREFIX + name)
    return default if value is None or value == "" else value


def _int(name: str, default: int) -> int:
    value = os.getenv(PREFIX + name)
    return default if value is None or value == "" else int(value)


@dataclass(frozen=True)
class Settings:
    """项目全部配置。frozen 是为了防止运行期被意外修改——
    配置应该是只读的，改配置的正确做法是重启进程。"""

    # --- 基础设施地址 ---
    pg_dsn: str
    redis_url: str

    # --- 服务监听地址 ---
    gateway_host: str
    gateway_port: int
    order_addr: str
    inventory_addr: str

    # --- 服务发现 ---
    # 两种模式：consul（注册中心式发现）vs static（用上面的静态地址）。
    # K8s 里设 static：发现交给 Service + DNS，见 common/discovery.py。
    discovery_mode: str
    consul_url: str
    consul_ttl_s: int        # 健康检查 TTL：超过这个时长没心跳，Consul 判为不健康
    consul_heartbeat_s: int  # 心跳间隔。必须显著小于 TTL，否则网络一抖就误判
    discovery_cache_s: int   # 客户端解析结果本地缓存多久（秒）

    # --- 超时预算（毫秒）---
    # 这两个值构成"逐跳递减"的预算链，是本项目演示 deadline 传播的核心数据。
    gateway_deadline_ms: int
    order_reserve_timeout_ms: int

    # --- 消息 ---
    # 注意：Stream 的 key **不在这里** 定义。
    # key 的唯一来源是 common/redisx.py —— 那里集中了所有 Redis key 的拼法。
    # 同一个东西有两处定义，早晚会漂移。
    consumer_group: str
    consumer_name: str

    # --- 观测 ---
    log_level: str

    # --- Snowflake ---
    # 多实例部署时每个进程必须有不同的 worker_id，否则会生成重复 ID。
    worker_id: int

    # --- 弹性 ---
    rate_limit_per_sec: int  # 每个 user_id 每秒允许的请求数
    lock_ttl_ms: int
    breaker_fail_threshold: int
    breaker_reset_ms: int


settings = Settings(
    pg_dsn=_str("PG_DSN", "postgresql://micro:micro@localhost:5432/micro_matrix"),
    redis_url=_str("REDIS_URL", "redis://localhost:6379/0"),
    gateway_host=_str("GATEWAY_HOST", "127.0.0.1"),
    gateway_port=_int("GATEWAY_PORT", 8000),
    order_addr=_str("ORDER_ADDR", "127.0.0.1:50051"),
    inventory_addr=_str("INVENTORY_ADDR", "127.0.0.1:50052"),
    discovery_mode=_str("DISCOVERY", "consul"),
    consul_url=_str("CONSUL_URL", "http://127.0.0.1:8500"),
    consul_ttl_s=_int("CONSUL_TTL_S", 10),
    consul_heartbeat_s=_int("CONSUL_HEARTBEAT_S", 3),
    discovery_cache_s=_int("DISCOVERY_CACHE_S", 5),
    # 客户端最多愿意等 3 秒。服务端拿到的预算一定小于它。
    gateway_deadline_ms=_int("GATEWAY_DEADLINE_MS", 3000),
    # order 调 inventory 时，从自己的剩余预算里再切出不超过 1 秒。
    # 为什么要再切小：如果 order 用光了全部预算，它就没时间处理"超时之后
    # 该做什么"（记录状态、触发补偿）。预留缓冲是超时设计的基本功。
    order_reserve_timeout_ms=_int("ORDER_RESERVE_TIMEOUT_MS", 1000),
    consumer_group=_str("CONSUMER_GROUP", "mm-order-consumers"),
    consumer_name=_str("CONSUMER_NAME", os.getenv("COMPUTERNAME", "worker")),
    log_level=_str("LOG_LEVEL", "INFO"),
    worker_id=_int("WORKER_ID", 1),
    rate_limit_per_sec=_int("RATE_LIMIT_PER_SEC", 20),
    lock_ttl_ms=_int("LOCK_TTL_MS", 2000),
    breaker_fail_threshold=_int("BREAKER_FAIL_THRESHOLD", 5),
    breaker_reset_ms=_int("BREAKER_RESET_MS", 3000),
)
