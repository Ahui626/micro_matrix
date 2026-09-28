"""
[问题]   这个进程除了 gRPC 服务，还要跑两个后台任务（relay、consumer）。
         怎么让它们和主服务一起启动、一起优雅退出？
[本文件] order 的入口。起 gRPC server + relay + consumer 三个并发单元。
[面试]   - 后台任务为什么要绑定 stop_event，而不是直接被 cancel？
           （cancel 会让它在任意 await 点硬中断 ——
             relay 可能正好在 XADD 和 mark_published 之间，
             于是产生一次"已投递但未标记"的重复（这还好，是 at-least-once）；
             更糟的是在数据库写入中途被切断，留下半个事务。
             置位 stop_event 让循环自己走到安全点再退出。）
         - 后台任务异常退出了该怎么办？
           （必须**让整个进程退出**，而不是带病继续运行。
             一个静默死掉的 consumer 会让消息永远堆积而无人察觉。
             下面用 asyncio.wait(FIRST_COMPLETED) 实现这一点。）
         - 停机的顺序为什么重要？
           （先停监听 → 再停后台任务 → 最后关连接池。
             反过来的话，后台任务还在跑但连接池已经关了，必然报错。）
[取舍]   把 relay 和 consumer 作为后台 task 放在 order 进程里，
         而不是拆成独立进程。理由见 services/order/relay.py 的取舍说明。
         代价：三者共享一个事件循环，一个卡住会影响另外两个 ——
         这也是为什么 consumer 的 block 只设 300ms 而不是无限等待。
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

import asyncio
import logging

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

import order_pb2_grpc

from common import chaos, clients, db, discovery, lifecycle, logging as mmlogging, redisx, settings, trace
from services.order import consumer, relay
from services.order.grpc_api import OrderServicer

log = logging.getLogger(__name__)

SERVICE_NAME = "order"


async def main() -> int:
    mmlogging.setup(SERVICE_NAME, settings.settings.log_level)

    log.info("=" * 64)
    log.info("%s 服务启动中，监听 %s", SERVICE_NAME, settings.settings.order_addr)
    log.info("下游 inventory → %s", settings.settings.inventory_addr)
    log.info("故障注入状态：%s", chaos.describe())
    log.info("=" * 64)

    # --- 依赖初始化（先建资源，再开监听）---
    await db.connect(settings.settings.pg_dsn)
    await db.init_schema()
    redisx.connect(settings.settings.redis_url)

    stop_event = lifecycle.install_signal_handlers(SERVICE_NAME)

    # --- 后台任务先起来 ---
    # 先起 relay/consumer 再开监听：这样第一条进来的请求
    # 产生的事件立刻就会被投递，不会先积压一轮。
    background = [
        asyncio.create_task(relay.run(stop_event), name="relay"),
        asyncio.create_task(consumer.run(stop_event), name="consumer"),
    ]

    # --- gRPC server ---
    server = grpc.aio.server(interceptors=[trace.TraceServerInterceptor()])
    order_pb2_grpc.add_OrderServiceServicer_to_server(OrderServicer(), server)

    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    try:
        bound_port = server.add_insecure_port(settings.settings.order_addr)
    except RuntimeError as exc:
        # 同 inventory/main.py：把 grpc 的隐晦报错翻译成人话。
        log.error("端口绑定失败：%s", settings.settings.order_addr)
        log.error("底层错误：%s", exc)
        log.error("排查方向：")
        log.error("  1. 端口已被占用（是不是已经跑了一个 order 服务？）")
        log.error("  2. Windows 保留了该端口段。查看：")
        log.error("     netsh interface ipv4 show excludedportrange protocol=tcp")
        log.error("     换端口：set MM_ORDER_ADDR=127.0.0.1:50053")
        stop_event.set()
        for task in background:
            task.cancel()
        return 1

    if bound_port == 0:
        log.error("端口绑定失败：%s", settings.settings.order_addr)
        stop_event.set()
        for task in background:
            task.cancel()
        return 1

    await server.start()
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    log.info("%s 服务已就绪 → %s", SERVICE_NAME, settings.settings.order_addr)

    # --- 服务注册 ---
    # 就绪之后才注册。registrar 也加入 background 列表，
    # 这样它能和 relay/consumer 一起被 FIRST_COMPLETED 监视、一起停机收尾。
    host, port = discovery.split_addr(settings.settings.order_addr)
    background.append(
        asyncio.create_task(
            discovery.run_registrar("order", "order", host, port, stop_event),
            name="registrar",
        )
    )

    # --- 等待"停机信号"或"后台任务异常退出" ---
    # 用 FIRST_COMPLETED：任何一个后台任务死掉都要立刻处理，
    # 不能让进程带着一个静默失效的 consumer 继续跑。
    stop_wait = asyncio.create_task(stop_event.wait(), name="stop-wait")
    done, _pending = await asyncio.wait(
        [stop_wait, *background], return_when=asyncio.FIRST_COMPLETED
    )
    for task in done:
        if task is stop_wait:
            continue
        if stop_wait.done():
            # 正常停机：stop_event 已置位，这些后台任务是被 stop_event 唤醒才
            # 退出的（relay/consumer/registrar 都监听它），不算"意外退出"。
            continue
        if task.cancelled():
            log.error("后台任务 %s 被取消", task.get_name())
        elif task.exception() is not None:
            log.error("后台任务 %s 异常退出：%r", task.get_name(), task.exception())
        else:
            log.error("后台任务 %s 意外正常退出（循环不该结束）", task.get_name())

    if not stop_wait.done():
        stop_wait.cancel()

    # --- 优雅停机：先停监听，再停后台，最后关资源 ---
    log.info("停止接受新请求，等待在途请求完成（最多 3 秒）…")
    await server.stop(grace=3)
    await health_servicer.enter_graceful_shutdown()

    log.info("通知后台任务退出…")
    stop_event.set()
    # 给它们 2 秒走到安全点。relay 的轮询间隔是 200ms，
    # consumer 的 block 是 300ms，2 秒足够走完一轮。
    _done, pending = await asyncio.wait(background, timeout=2.0)
    for task in pending:
        log.warning("后台任务 %s 未在 2 秒内退出，强制取消", task.get_name())
        task.cancel()
    await asyncio.gather(*background, return_exceptions=True)

    log.info("关闭下游连接与资源…")
    await discovery.close()
    await clients.close_all()
    await db.close()
    await redisx.close()

    log.info("%s 已完全停机", SERVICE_NAME)
    return 0


if __name__ == "__main__":
    # 裸 asyncio.run()。不设事件循环策略、不调 get_event_loop。
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
