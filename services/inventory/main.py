"""
[问题]   服务进程怎么启动、怎么优雅停机、拦截器怎么装上去。
[本文件] inventory 的入口。裸 asyncio.run() 起一个 grpc.aio server。
[面试]   - 优雅停机的完整流程是什么？
           （收到信号 → 停止接受新请求 → 等在途请求做完 → 关闭连接池和 channel → 退出。
             不做这一步的后果：滚动发布时正在处理的请求被硬切断，
             用户看到随机的失败 —— 而这些失败本来可以不发生。）
         - Windows 上为什么不能用 `loop.add_signal_handler()`？
           （Windows 的事件循环不支持它，会直接抛 NotImplementedError。
             任何从 Linux 博客抄来的优雅停机代码都会在这里崩。
             必须用 signal.signal() + 一个 asyncio.Event。）
         - 为什么要用 asyncio.run() 而不是手动 get_event_loop / run_forever？
           （asyncio.run 负责创建循环、跑完、再关掉，不会漏。
             手动管理很容易忘了 close，或者在 Python 3.14 里踩到
             get_event_loop 的废弃警告 —— 这个函数现在直接抛 RuntimeError。）
[取舍]   信号处理用"设置 Event + 主协程 await"，而不是在信号处理器里直接
         调 server.stop()。理由：信号处理器运行在很受限的上下文里，
         在那里做异步操作是危险的；把实际动作交给主协程更安全。
"""

from __future__ import annotations

# 入口脚本固定前缀：向上找到 bootstrap.py 并导入。
# 这样不管从哪个目录、以哪种方式运行（python services/x/main.py / python -m ...）都成立。
# bootstrap 负责两件事：把仓库根和 gen/ 放进 sys.path、把输出编码强制成 UTF-8。
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

import inventory_pb2_grpc

from common import (
    chaos,
    clients,
    db,
    discovery,
    lifecycle,
    logging as mmlogging,
    redisx,
    settings,
    trace,
)

from services.inventory.grpc_api import InventoryServicer

log = logging.getLogger(__name__)

SERVICE_NAME = "inventory"


async def main() -> int:
    mmlogging.setup(SERVICE_NAME, settings.settings.log_level)

    log.info("=" * 64)
    log.info("%s 服务启动中，监听 %s", SERVICE_NAME, settings.settings.inventory_addr)
    log.info("故障注入状态：%s", chaos.describe())
    log.info("=" * 64)

    # --- 依赖初始化 ---
    # 顺序有讲究：先建资源，再开监听。反过来的话，
    # 服务已经在接受请求了，但数据库还没连上，前几个请求必然失败。
    await db.connect(settings.settings.pg_dsn)
    await db.init_schema()
    redisx.connect(settings.settings.redis_url)

    # --- 组装 server ---
    # 拦截器在这里挂上去，之后所有服务方法自动拥有 trace_id，业务代码零感知。
    server = grpc.aio.server(interceptors=[trace.TraceServerInterceptor()])
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(), server)

    # 标准健康检查。生产环境里 K8s 的 liveness/readiness 探针就是打这个接口。
    # 这里用官方实现而不是自己写一个 HTTP 接口 —— 客户端和编排系统都认识它。
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    try:
        bound_port = server.add_insecure_port(settings.settings.inventory_addr)
    except RuntimeError as exc:
        # grpc 在绑定失败时抛的是 RuntimeError，而且信息很隐晦
        # （"Failed to bind to address ...; set GRPC_VERBOSITY=debug"）。
        # 这里翻译成人话，并给出 Windows 上最容易踩的那个原因。
        log.error("端口绑定失败：%s", settings.settings.inventory_addr)
        log.error("底层错误：%s", exc)
        log.error("排查方向：")
        log.error("  1. 端口已被占用 —— 用 `netstat -ano | findstr :%d` 看看是谁",
                  settings.settings.inventory_addr.rsplit(":", 1)[-1] if ":" in settings.settings.inventory_addr else 0)
        log.error("  2. Windows 保留了该端口段（Hyper-V/WSL/Docker 会预留大片端口）。")
        log.error("     查看：netsh interface ipv4 show excludedportrange protocol=tcp")
        log.error("     换端口：set MM_INVENTORY_ADDR=127.0.0.1:50053")
        return 1

    if bound_port == 0:
        log.error("端口绑定失败：%s", settings.settings.inventory_addr)
        return 1

    await server.start()
    await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    log.info("%s 服务已就绪 → %s", SERVICE_NAME, settings.settings.inventory_addr)

    # --- 优雅停机 ---
    # 信号处理的具体实现（以及 Windows 上的坑）见 common/lifecycle.py
    stop_event = lifecycle.install_signal_handlers(SERVICE_NAME)

    # --- 服务注册 ---
    # 就绪之后才注册：注册表里永远不会出现"还没准备好接流量"的实例。
    # 注册/心跳在后台任务里跑，停机时自动注销（见 common/discovery.py）。
    host, port = discovery.split_addr(settings.settings.inventory_addr)
    registrar = asyncio.create_task(
        discovery.run_registrar("inventory", "inventory", host, port, stop_event),
        name="registrar",
    )

    await stop_event.wait()

    log.info("停止接受新请求，等待在途请求完成（最多 3 秒）…")
    # grace=3：给在途请求最多 3 秒做完。超时后强制切断。
    # 这个值的取舍：太长会让发布变慢，太短会切断正常请求。
    await server.stop(grace=3)
    await health_servicer.enter_graceful_shutdown()

    # 等 registrar 完成注销（它看到 stop_event 置位就会 deregister 并退出）。
    # 必须赶在关闭 discovery 的 HTTP 客户端之前 —— 否则注销请求发不出去。
    await registrar

    log.info("关闭下游连接与资源…")
    await discovery.close()
    await clients.close_all()
    await db.close()
    await redisx.close()

    log.info("%s 已完全停机", SERVICE_NAME)
    return 0


if __name__ == "__main__":
    # 裸 asyncio.run()。不设事件循环策略、不调 get_event_loop。
    # Python 3.14 的 Windows 默认就是 ProactorEventLoop，正是 grpc.aio 需要的。
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        # Ctrl+C 在 Windows 上有时会绕过我们的信号处理器直接抛到这里。
        sys.exit(130)
