"""
[问题]   怎么用代码（而不是命令行的 uvicorn）启动 FastAPI 服务？
[本文件] 用 uvicorn.Server 编程式启动，而不是 `uvicorn main:app` 命令行。
[面试]   - 编程式启动和命令行启动各有什么适用场景？
           （命令行：简单、有 --reload，适合开发。
             编程式：能把启动逻辑放进已有的异步流程里，
             能自己控制日志配置、能在启动前做检查和等待。
             生产环境常用编程式，因为需要"先确认依赖可用再监听端口"。）
         - 为什么这个服务**不**自己装信号处理器？
           （uvicorn 自己已经装了，而且它的优雅停机逻辑更完善
             （等待在途请求、关闭连接）。
             我们再装一遍会把它的覆盖掉，反而破坏停机流程。
             这和 inventory/order 那两个 gRPC 服务不同 ——
             它们用 grpc.aio.server，需要自己处理信号。
             所以"要不要自己装信号"取决于框架有没有替你做。）
[取舍]   这里不像 gRPC 服务那样自己装信号处理器，交给 uvicorn。
         代价：网关的停机行为和另外两个服务不完全一致（uvicorn 的实现）。
         收益：不重复实现一套已经被验证过的停机逻辑。
         如果哪天需要统一，就该把 uvicorn 的 signal_handler 关掉自己接管 ——
         但那要先把 uvicorn 的停机流程读一遍，不是随手改的事。
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

import uvicorn

from common import settings


async def main() -> int:
    config = uvicorn.Config(
        "services.gateway.http_api:app",
        host=settings.settings.gateway_host,
        port=settings.settings.gateway_port,
        # log_config=None：不要用 uvicorn 默认的日志配置。
        # 我们已经用 common/logging.py 配好了带 trace_id 的格式，
        # 让 uvicorn 再配一遍会把我们的 handler 覆盖掉 ——
        # 这是"日志格式突然变了"的最常见原因。
        log_config=None,
        # access_log=False：uvicorn 的访问日志没有 trace_id 字段。
        # 我们自己的 MetricsMiddleware + 业务日志已经把需要的信息都记了，
        # 两套日志并存只会让日志量翻倍且难以对齐。
        access_log=False,
        # 优雅停机的等待时间。和 gRPC 服务的 grace=3 保持一致。
        timeout_graceful_shutdown=3,
    )

    server = uvicorn.Server(config)
    await server.serve()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
