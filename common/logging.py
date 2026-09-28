"""
[问题]   三个进程的日志混在一个终端里，怎么一眼看出哪几行属于同一次请求？
[本文件] 统一日志格式，并把 contextvars 里的 trace_id **自动**注入每一行。
[面试]   - 结构化日志（JSON）和文本日志各自的取舍？
           （JSON 便于机器检索和聚合，文本便于人眼扫。生产用 JSON 进采集系统，
             本地开发用文本。这里为了可读性用文本。）
         - trace_id 为什么要由格式化器自动注入，而不是每次 log 时手动拼？
           （手动拼一定会漏。让"正确"成为默认，而不是靠自觉。）
[取舍]   用标准库 logging 而不是 structlog / loguru —— 本项目只多一个依赖都不划算，
         而且 formatter + contextvar 这 20 行就把机制讲透了，没有什么被藏起来。
"""

from __future__ import annotations

import logging
import sys

from common import trace

# 日志格式里的 %(service)s 和 %(trace_id)s 不是 logging 内置字段，
# 是我们下面用 Filter 塞进去的。
_FORMAT = "%(asctime)s %(levelname)-5s [%(service)-9s] [%(trace_id)s] %(name)s: %(message)s"
_DATEFMT = "%H:%M:%S"


class _ContextFilter(logging.Filter):
    """给每条日志记录补充 service 名和当前 trace_id。

    用 Filter 而不是自定义 Formatter 的 format()，
    是因为 Filter 在 LogRecord 生成后、格式化前执行，
    不会和 formatter 的异常处理纠缠在一起。
    """

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self._service
        # 不在请求上下文里的日志（启动日志、后台任务）没有 trace_id，
        # 用 "-" 占位比留空更容易对齐阅读。
        record.trace_id = trace.current_trace_id() or "-"
        return True


def setup(service: str, level: str = "INFO") -> None:
    """在进程启动时调用一次。"""
    root = logging.getLogger()
    root.setLevel(level.upper())

    # 清掉可能已存在的 handler，避免重复 setup 导致日志打两遍
    # （uvicorn 会自己加 handler，这是很常见的"日志出现两次"的原因）
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
    handler.addFilter(_ContextFilter(service))
    root.addHandler(handler)

    # gRPC 内部日志很吵，而且默认级别下会打印一堆连接细节。
    # 调高到 WARNING，只保留真正需要注意的信息。
    logging.getLogger("grpc").setLevel(logging.WARNING)
    # asyncio 的 "Task was destroyed but it is pending" 之类在开发时有用，
    # 但正常停机时会产生噪音，这里也压掉。
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
