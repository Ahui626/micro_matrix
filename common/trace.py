"""
[问题]   一次下单请求穿过 gateway → order → inventory 三个进程。
         出问题时，怎么把散落在三个进程日志里的行认成"同一次请求"？
[本文件] 用 contextvars 保存 trace_id，用 gRPC 拦截器把它放进 metadata 传过网络。
         一共 100 行左右，把这个机制完整暴露出来。
[面试]   - 分布式链路追踪是什么？trace_id 和 span_id 分别管什么？
         - trace_id 是怎么跨进程传递的？（放进请求头 / gRPC metadata）
         - 为什么用 contextvars 而不是全局变量或 threading.local？
         - 为什么不能只在 intercept_service 里 set 一下就完事？（见下方注释）
         - OpenTelemetry 帮你做了什么？为什么"知道它做了什么"比"会调它的 API"重要？
[取舍]   手写而不是直接上 OpenTelemetry SDK。
         理由：OTel 会把"trace_id 怎么过网络"这件事藏起来，而这恰恰是面试
         真正会问的部分。手写一遍，机制完全暴露。
         代价：没有 span 层级、没有采样、没有可视化 UI。生产环境必须用 OTel，
         这里学的是它的内核。
"""

from __future__ import annotations

import contextvars
import uuid

import grpc

# 自定义的传递头。OTel 用的标准头叫 traceparent，格式是 W3C 规定的。
# 这里为了讲清楚"就是放在 metadata 里传"，用了简化版。
TRACE_HEADER = "x-trace-id"

# contextvars 的核心特性：它在 **asyncio 任务** 维度上隔离。
#
# 为什么不能用全局变量：一个进程同时处理成百上千个请求，全局变量会被互相覆盖。
# 为什么不能用 threading.local：asyncio 是单线程跑多任务的，所有协程共享
#   同一个线程，threading.local 区分不开它们。
# contextvars 正是为此设计的：每个 Task 在创建时复制一份上下文，
#   Task 内部对 ContextVar 的修改不影响其它 Task。
_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")


def new_trace_id() -> str:
    """生成一个新的 trace_id。

    用 uuid4 而不是自增计数器：不同进程的计数器会撞车。
    用 hex 而不是带横线的格式：日志里更容易 grep。
    """
    return uuid.uuid4().hex


def current_trace_id() -> str:
    return _trace_id.get()


def bind(trace_id: str) -> contextvars.Token:
    return _trace_id.set(trace_id)


def unbind(token: contextvars.Token) -> None:
    _trace_id.reset(token)


# ---------------------------------------------------------------------------
# 服务端拦截器：从 metadata 里取出上游传来的 trace_id
# ---------------------------------------------------------------------------
class TraceServerInterceptor(grpc.aio.ServerInterceptor):
    """每个进来的 gRPC 调用都会先经过这里。

    拦截器的意义：如果要在每个服务方法里手动读 metadata、手动 set contextvar，
    那每个方法都要写 5 行样板，而且新人一定会漏。拦截器把这件事收敛到一处。
    """

    async def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata or ())
        trace_id = metadata.get(TRACE_HEADER) or new_trace_id()

        handler = await continuation(handler_call_details)
        if handler is None:
            return None

        # ┌─────────────────────────────────────────────────────────────────┐
        # │ 这里是本文件最值得看的一处坑。                                    │
        # │                                                                 │
        # │ intercept_service 的返回值是一个 handler 对象，真正的业务函数     │
        # │ 是**稍后**在别的地方被调用的。所以在这里直接 _trace_id.set(...)  │
        # │ 是没用的 —— 等业务函数跑起来时，这个 with 块早就退出了。          │
        # │                                                                 │
        # │ 正确做法：把 handler 包一层，在 wrapper **内部** set。             │
        # │ 这也解释了为什么很多"照抄拦截器教程"的代码 trace_id 总是空 ——    │
        # │ 它不是写错了，是上下文作用域理解错了。                            │
        # └─────────────────────────────────────────────────────────────────┘
        return _wrap_unary_unary(handler, trace_id)


def _wrap_unary_unary(handler, trace_id: str):
    if handler.unary_unary is None:
        # 本项目只用 unary-unary。其它模式（stream）需要各自的包装。
        return handler

    original = handler.unary_unary

    async def traced(request, context):
        token = _trace_id.set(trace_id)
        try:
            return await original(request, context)
        finally:
            _trace_id.reset(token)

    return grpc.unary_unary_rpc_method_handler(
        traced,
        request_deserializer=handler.request_deserializer,
        response_serializer=handler.response_serializer,
    )


# ---------------------------------------------------------------------------
# 客户端拦截器：把当前 trace_id 塞进 metadata 发出去
# ---------------------------------------------------------------------------
class TraceClientInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """每个发出去的 gRPC 调用都会先经过这里。

    这是"trace_id 如何跨进程"的答案：它就是一个普通的请求头。
    分布式追踪没有任何魔法，全靠每一跳都把它带上。
    """

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        trace_id = _trace_id.get()
        if not trace_id:
            # 没有上游 trace_id 说明这是链路起点，自己生成一个。
            trace_id = new_trace_id()
            _trace_id.set(trace_id)

        metadata = list(client_call_details.metadata or [])
        metadata.append((TRACE_HEADER, trace_id))

        # ClientCallDetails 是 NamedTuple，必须整个重建而不是改字段。
        new_details = grpc.aio.ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
        )
        return await continuation(new_details, request)
