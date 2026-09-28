"""
[问题]   订单服务的所有 SQL。重点是"订单 + outbox 必须同一个事务"。
[本文件] 数据访问层。所有函数显式接收连接，事务边界由 service 层决定。
[面试]   - 为什么订单和 outbox 记录必须在**同一个事务**里插入？
           （这是本地消息表模式的全部要点。见 insert_order_with_outbox。）
         - 为什么 orders.request_id 要加唯一约束？
           （幂等的最后一道防线。应用层的"先查再插"在并发下一定会漏，
             数据库约束是唯一可靠的守门人。）
         - 消费幂等为什么要和副作用在同一个事务里？
           （见 try_claim_event 的注释——顺序错了会导致消息被永久跳过。）
[取舍]   api_idempotency 存的是**完整响应**而不是一个"已处理"标记。
         只有存完整响应，重复请求才能返回和第一次**完全相同**的结果。
         存标记的话，你只能告诉调用方"处理过了"，但给不出当时的结果。
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import asyncpg

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 订单
# ---------------------------------------------------------------------------
async def find_api_idempotency(conn: asyncpg.Connection, request_id: str) -> dict | None:
    row = await conn.fetchrow(
        "SELECT response FROM api_idempotency WHERE request_id = $1", request_id
    )
    return dict(row["response"]) if row else None


async def save_api_idempotency(
    conn: asyncpg.Connection, request_id: str, response: dict[str, Any], now_ms: int
) -> None:
    """记录这次请求的最终响应，供后续重复请求回放。

    用 ON CONFLICT DO NOTHING：并发下可能有两个请求同时走到这里，
    第一个写成功，第二个什么都不做。两者返回的响应内容是一致的，
    所以谁赢都无所谓。

    注意 response 直接传 dict，**不要**自己 json.dumps。
    db.py 已经给 jsonb 注册了编解码器，驱动会负责编码。
    手动再 dumps 一次会双重编码，读回来变成字符串而不是 dict ——
    这个 bug 很隐蔽，因为写入不报错，只有读到的时候才炸。
    """
    await conn.execute(
        """
        INSERT INTO api_idempotency (request_id, response, created_at_ms)
        VALUES ($1, $2, $3)
        ON CONFLICT (request_id) DO NOTHING
        """,
        request_id,
        response,
        now_ms,
    )


async def find_order_by_request(conn: asyncpg.Connection, request_id: str) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM orders WHERE request_id = $1", request_id)
    return dict(row) if row else None


async def get_order(conn: asyncpg.Connection, order_id: int) -> dict | None:
    row = await conn.fetchrow("SELECT * FROM orders WHERE id = $1", order_id)
    return dict(row) if row else None


async def write_outbox(
    conn: asyncpg.Connection,
    event_type: str,
    aggregate_id: int,
    payload: dict[str, Any],
    now_ms: int,
) -> str:
    """把一个待发事件写进 outbox，返回 event_id。

    注意这里**不开事务、不提交** —— 它必须和"业务数据变更"在同一个事务里。
    调用方负责事务边界，这正是本地消息表模式能成立的前提。

    事件语义的区分很重要（见 service._finalize_order）：
        订单创建时  → OrderCreated   （记录"事实发生了"）
        订单进入终态 → OrderConfirmed / OrderCancelled （"结果确定了"）
    消费端应该监听**终态事件**，而不是创建事件 ——
    否则会给一个最终被取消的订单发积分。
    """
    event_id = uuid.uuid4().hex
    body = {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_id": aggregate_id,
        "created_at_ms": now_ms,
        **payload,
    }
    await conn.execute(
        """
        INSERT INTO outbox (event_id, event_type, aggregate_id, payload, published, created_at_ms)
        VALUES ($1, $2, $3, $4, FALSE, $5)
        """,
        event_id, event_type, aggregate_id, body, now_ms,
    )
    return event_id


async def insert_order_with_outbox(
    conn: asyncpg.Connection,
    *,
    order_id: int,
    request_id: str,
    user_id: str,
    sku: str,
    quantity: int,
    now_ms: int,
    write_event: bool = True,
) -> str:
    """★ 本地消息表模式的核心：把"写订单"和"写待发事件"放进同一个事务。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 为什么必须同一个事务：                                           │
    │                                                                  │
    │ 你**无法**把"写数据库"和"发消息"变成一个原子操作。              │
    │   先写库、后发消息 → 中间崩溃，事件永久丢失，下游永远不知道这笔单 │
    │   先发消息、后写库 → 下游先收到事件，回头查订单却查不到（幽灵订单）│
    │                                                                  │
    │ 解法：不要真的"发消息"，而是把"要发的消息"当作**业务数据**        │
    │ 写进同一个事务。于是"订单已创建"和"事件待发送"要么一起成功、      │
    │ 要么一起失败。由 relay 进程在事务之外把事件真的投递出去。         │
    │                                                                  │
    │ 这一行 INSERT INTO outbox 就是整个模式的全部秘密。               │
    └──────────────────────────────────────────────────────────────────┘

    write_event 参数只服务于教学（labs/lab4 要对比"不写 outbox"的后果）。
    正常路径下它永远是 True。
    """
    await conn.execute(
        """
        INSERT INTO orders (id, request_id, user_id, sku, quantity, status, created_at_ms)
        VALUES ($1, $2, $3, $4, $5, 'PENDING', $6)
        """,
        order_id, request_id, user_id, sku, quantity, now_ms,
    )

    if not write_event:
        # 反面教材路径：只写订单不写事件。
        # 对比 lab4 —— 进程在事务之后崩溃，这笔订单的事件就永久消失了。
        return ""

    return await write_outbox(
        conn,
        "OrderCreated",
        order_id,
        {
            "order_id": order_id,
            "request_id": request_id,
            "user_id": user_id,
            "sku": sku,
            "quantity": quantity,
        },
        now_ms,
    )


async def update_order_status(conn: asyncpg.Connection, order_id: int, status: str) -> None:
    await conn.execute("UPDATE orders SET status = $1 WHERE id = $2", status, order_id)


# ---------------------------------------------------------------------------
# Outbox 中继
# ---------------------------------------------------------------------------
async def fetch_unpublished(conn: asyncpg.Connection, limit: int) -> list[dict]:
    """取一批待发送的事件。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 关于多实例：如果同时跑多个 relay，两个实例会捞到同一批行，        │
    │ 导致事件被发两遍。标准解法是：                                   │
    │                                                                  │
    │     SELECT ... FROM outbox WHERE published = FALSE               │
    │      ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED                 │
    │                                                                  │
    │ SKIP LOCKED 让每个 relay 拿到**不同的**行，互不阻塞。             │
    │                                                                  │
    │ 本项目只跑一个 relay，所以没加。但要能说出这件事 —— 它是         │
    │ "多消费者竞争同一张队列表"的标准答案。                           │
    │                                                                  │
    │ 注意即使加了 SKIP LOCKED，重复投递仍然可能发生（见 relay.py），   │
    │ 所以消费端幂等依然是必需的，不能靠这个省掉。                     │
    └──────────────────────────────────────────────────────────────────┘
    """
    rows = await conn.fetch(
        "SELECT id, event_id, event_type, payload FROM outbox "
        "WHERE published = FALSE ORDER BY id LIMIT $1",
        limit,
    )
    return [dict(r) for r in rows]


async def mark_published(conn: asyncpg.Connection, outbox_id: int) -> None:
    await conn.execute("UPDATE outbox SET published = TRUE WHERE id = $1", outbox_id)


# ---------------------------------------------------------------------------
# 消费端幂等与副作用
# ---------------------------------------------------------------------------
async def try_claim_event(
    conn: asyncpg.Connection, event_id: str, consumer: str, now_ms: int
) -> bool:
    """尝试"认领"一个事件。返回 True 表示之前没处理过，可以继续。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 为什么用 ON CONFLICT DO NOTHING + rowcount 判断，而不是"先查再插"？│
    │                                                                  │
    │ "先查再插"在并发下会漏：两个消费者同时查，都发现不存在，          │
    │ 于是都插入、都处理 —— 副作用执行两次。                           │
    │ INSERT ... ON CONFLICT DO NOTHING 由数据库保证只有一个能插入，   │
    │ 另一个拿到 rowcount=0。这是把"判断"交给数据库，而不是应用层。    │
    │                                                                  │
    │ 另一个关键点：这个 INSERT 必须和**副作用在同一个事务里**。        │
    │ 如果先插入认领记录、再单独做副作用，那么中间崩溃就会导致          │
    │ "事件被标记为已处理，但副作用从没发生过" —— 消息被永久跳过。      │
    │ 顺序必须是：事务内（认领 + 副作用）一起提交，提交后再 XACK。     │
    └──────────────────────────────────────────────────────────────────┘
    """
    result = await conn.execute(
        """
        INSERT INTO processed_events (event_id, consumer, processed_at_ms)
        VALUES ($1, $2, $3)
        ON CONFLICT (event_id, consumer) DO NOTHING
        """,
        event_id, consumer, now_ms,
    )
    return result.endswith(" 1")


async def add_points(conn: asyncpg.Connection, user_id: str, delta: int) -> int:
    """副作用：加积分。用来验证"重复投递会不会导致重复加分"。"""
    row = await conn.fetchrow(
        """
        INSERT INTO points (user_id, total) VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET total = points.total + $2
        RETURNING total
        """,
        user_id, delta,
    )
    return row["total"]


async def get_points(conn: asyncpg.Connection, user_id: str) -> int:
    value = await conn.fetchval("SELECT total FROM points WHERE user_id = $1", user_id)
    return value or 0
