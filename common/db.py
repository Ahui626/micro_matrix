"""
[问题]   三个服务共用一个 Postgres。连接池怎么建、表结构从哪来、
         以及为什么这里坚持用裸 SQL 而不是 ORM。
[本文件] 连接池的创建与关闭、整个系统的 DDL、以及建表/重置工具。
[面试]   - 为什么异步服务里不能用同步数据库驱动？
           （psycopg2 的每次查询都会阻塞整个事件循环，所有并发请求一起卡住。
             这是 Python 异步服务最常见的性能事故。）
         - 连接池该设多大？(max_size 不是越大越好——数据库的总连接数有上限，
             每个连接占内存，而且连接数过多会加剧锁竞争。）
         - 事务隔离级别：Postgres 默认是 Read Committed，它能防住什么、防不住什么？
           （防脏读；防不住"读-改-写"丢失更新和幻读。我们的防超卖靠的是
             单条原子 UPDATE，而不是靠隔离级别。）
[取舍]   不用 SQLAlchemy。
         理由：本项目最关键的一行是防超卖的那条 UPDATE 语句。ORM 会把它藏在
         一层抽象后面，而读者必须亲眼看到 `WHERE stock >= $1` 这个条件
         才知道超卖是怎么被防住的。教学场景下，透明 > 便利。
         代价：得手写 SQL 和映射，在生产项目里这笔账要重算。
"""

from __future__ import annotations

import json
import logging

import asyncpg

log = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# 为什么是这个表结构：每一张表都对应一个分布式问题，不是随手设计的。
# ---------------------------------------------------------------------------
SCHEMA = """
-- 订单表。
-- id 用 Snowflake（BIGINT）而不是 SERIAL：分库分表后自增会冲突。
-- request_id 上加 UNIQUE：这是幂等的**最后一道防线**。
--   应用层会先查幂等表，但两个并发请求可能同时通过检查，
--   唯一约束保证只有一个能插入成功，另一个拿到唯一冲突。
--   不要依赖"应用层检查"来保证唯一性——并发下它一定会漏。
CREATE TABLE IF NOT EXISTS orders (
    id            BIGINT PRIMARY KEY,
    request_id    TEXT NOT NULL UNIQUE,
    user_id       TEXT NOT NULL,
    sku           TEXT NOT NULL,
    quantity      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

-- 库存表，真相之源。
-- version 列是乐观锁：UPDATE ... WHERE version = $expected。
--   rowcount 为 0 就说明期间有人改过，需要重读重试。
--   这和下面的"原子扣减"是两种不同的防并发手段，本项目两种都实现，
--   用来对比（见 services/inventory/repo.py）。
CREATE TABLE IF NOT EXISTS inventory (
    sku     TEXT PRIMARY KEY,
    stock   INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0
);

-- 预留记录。存在的意义有两个：
--   1. 支撑幂等：同一个 request_id 重复 Reserve，直接返回上次的结果。
--   2. 支撑补偿：Release 时要知道当初预留了多少、是否已经释放过。
-- 补偿动作本身也会被重试，所以它也必须幂等——status 字段就是干这个的。
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    request_id     TEXT NOT NULL,
    sku            TEXT NOT NULL,
    quantity       INTEGER NOT NULL,
    status         TEXT NOT NULL,
    created_at_ms  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservations_request ON reservations(request_id);

-- 本地消息表（Outbox）。
-- 解决的问题：你无法把"写数据库"和"发消息"变成一个原子操作。
--   先写库后发消息 → 中间崩溃则消息永久丢失；
--   先发消息后写库 → 下游会收到幽灵订单。
-- 解法：把"要发的消息"当作**业务数据**写进同一个事务。
--   这样"订单已创建"和"待发送的事件"要么一起成功，要么一起失败。
--   relay 进程再去轮询这张表，把事件真的发出去。
-- published=false 的部分索引：relay 每次只查未发送的，索引让它保持极快。
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL UNIQUE,
    event_type    TEXT NOT NULL,
    aggregate_id  BIGINT NOT NULL,
    payload       JSONB NOT NULL,
    published     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(id) WHERE published = FALSE;

-- API 层幂等表：记录每个 request_id 的响应，重复请求直接回放。
-- 存的是完整响应而不是"一个标记"，这样才能做到"返回和第一次**相同**的结果"。
CREATE TABLE IF NOT EXISTS api_idempotency (
    request_id    TEXT PRIMARY KEY,
    response      JSONB NOT NULL,
    created_at_ms BIGINT NOT NULL
);

-- 消费端幂等表。
-- 消息系统给的是 at-least-once（至少一次），意味着重复投递是**必然**的，
-- 不是异常。所以每个消费者都必须自己能去重。
-- 主键是 (event_id, consumer)：同一个事件可以被不同消费者各自处理一次，
-- 但同一个消费者不能重复处理。
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        TEXT NOT NULL,
    consumer        TEXT NOT NULL,
    processed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (event_id, consumer)
);

-- 消费者产生的副作用（积分）。用来验证"重复投递会不会导致重复加分"。
CREATE TABLE IF NOT EXISTS points (
    user_id TEXT PRIMARY KEY,
    total   INTEGER NOT NULL DEFAULT 0
);
"""


async def _init_connection(conn: asyncpg.Connection) -> None:
    """每个新连接建好时调用一次。

    注册 JSONB 的编解码器。asyncpg 默认把 jsonb 当字符串返回，用起来很别扭；
    注册之后 payload 字段直接就是 dict，省掉到处 json.loads。
    这类"驱动默认行为不合心意"的细节，是实际项目里最常踩的坑之一。
    """
    for type_name in ("jsonb", "json"):
        await conn.set_type_codec(
            type_name,
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )


async def connect(dsn: str, max_size: int = 10) -> asyncpg.Pool:
    """创建全局连接池。进程启动时调一次。

    max_size 可调是因为 labs 需要：确定性复现竞态要同时持有 N 个连接
    （N 个协程各占一个，然后一起卡在 barrier 上）。如果 N > max_size，
    后面的协程永远拿不到连接，实验会死锁 —— 而不是报错。
    这种"参数没设对导致挂住"的坑，在生产环境里同样存在。
    """
    global _pool
    if _pool is not None:
        return _pool

    _pool = await asyncpg.create_pool(
        dsn,
        min_size=min(2, max_size),
        max_size=max_size,
        init=_init_connection,
        # 单条命令的超时。防止一个卡住的查询永久占用连接。
        command_timeout=10,
    )
    log.info("Postgres 连接池已建立 min=%d max=%d", min(2, max_size), max_size)
    return _pool


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("连接池还没初始化，请先 await db.connect(dsn)")
    return _pool


async def close() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def init_schema() -> None:
    """建表。幂等——所有语句都是 IF NOT EXISTS，可以反复执行。"""
    async with pool().acquire() as conn:
        await conn.execute(SCHEMA)
    log.info("表结构已就绪")


async def reset_all() -> None:
    """清空所有数据并重新播种库存。

    仅供 labs 使用。生产环境当然不该有这么个函数——
    这里存在的理由是：每个实验都必须从**确定**的初始状态开始，
    否则断言无法复现。
    """
    async with pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "TRUNCATE orders, outbox, api_idempotency, processed_events, "
                "reservations, points RESTART IDENTITY"
            )
            await conn.execute("DELETE FROM inventory")


async def seed_inventory(sku: str, stock: int) -> None:
    """设置某个 SKU 的库存。labs 的起始条件。"""
    async with pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inventory (sku, stock, version) VALUES ($1, $2, 0)
            ON CONFLICT (sku) DO UPDATE SET stock = $2, version = 0
            """,
            sku,
            stock,
        )


async def health() -> bool:
    try:
        async with pool().acquire() as conn:
            return await conn.fetchval("select 1") == 1
    except Exception:
        return False
