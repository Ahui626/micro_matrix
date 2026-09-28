"""
[问题]   "库存不足"和"数据库连不上"都是失败，但处理方式完全相反：
         前者重试一万次也没用，后者必须重试。如果两者都用异常表达，
         调用方根本分不清该不该重试。
[本文件] 业务异常层级 + 到 gRPC 状态码的映射 + 一个"可重试"判定表。
[面试]   - gRPC 状态码里哪些可重试、哪些绝对不能重试？
         - 把不可重试的错误标成可重试会发生什么？
           （重试风暴：下游已经过载，你还把无效请求放大 N 倍打过去。
            这正是 labs/lab5 要演示的现象。）
         - 把可重试的错误标成不可重试又会怎样？
           （本来能自愈的瞬时抖动，变成了用户可见的失败。）
[取舍]   业务失败（库存不足）用**正常返回值**表达，而不是抛异常。
         因为它是预期内的正常结果，调用方需要区别对待"业务拒绝"和"系统故障"。
         异常只留给真正的意外。
"""

from __future__ import annotations

import grpc

# ---------------------------------------------------------------------------
# 可重试 vs 不可重试：这张表是面试高频考点，也是熔断器和重试逻辑的依据。
#
# 判断原则：这个失败是"瞬时的"还是"确定的"？
#   瞬时（可重试）：网络抖动、对端重启、超时。重试有意义。
#   确定（不可重试）：参数错了、资源不存在、权限不够。重试只是浪费。
# ---------------------------------------------------------------------------
RETRYABLE_STATUS = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,        # 对端挂了 / 连接被拒。典型瞬时故障。
        grpc.StatusCode.DEADLINE_EXCEEDED,  # 超时。对端可能只是慢，重试有机会。
        grpc.StatusCode.ABORTED,            # 并发冲突导致的中止，通常可重试。
        grpc.StatusCode.RESOURCE_EXHAUSTED, # 限流。**必须配合退避**，否则雪上加霜。
    }
)

NON_RETRYABLE_STATUS = frozenset(
    {
        grpc.StatusCode.INVALID_ARGUMENT,    # 参数不合法，重试还是同一个参数。
        grpc.StatusCode.NOT_FOUND,           # 资源不存在。
        grpc.StatusCode.ALREADY_EXISTS,      # 幂等冲突。
        grpc.StatusCode.PERMISSION_DENIED,   # 权限。
        grpc.StatusCode.UNAUTHENTICATED,     # 未认证。
        grpc.StatusCode.FAILED_PRECONDITION, # 前置条件不满足（如库存不足）。
        grpc.StatusCode.UNIMPLEMENTED,       # 方法不存在，重试毫无意义。
    }
)


def is_retryable(code: grpc.StatusCode) -> bool:
    """给一个 gRPC 状态码，判断该不该重试。

    注意这里用的是**白名单**而不是黑名单：只有明确知道可重试的才重试。
    未知状态码一律不重试——因为错误地重试比不重试更危险。
    """
    return code in RETRYABLE_STATUS


# ---------------------------------------------------------------------------
class BusinessError(Exception):
    """所有业务异常的基类。

    子类通过类属性声明两件事：转成哪个 gRPC 状态码、能不能重试。
    这样调用方只需写 `except BusinessError`，不用逐个枚举。
    """

    code: grpc.StatusCode = grpc.StatusCode.UNKNOWN
    retryable: bool = False

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.__class__.__name__)
        self.message = message or self.__class__.__name__


class OutOfStock(BusinessError):
    """库存不足。这是**正常业务结果**，不是系统故障。

    注意它被映射成 FAILED_PRECONDITION（不可重试）而不是 UNAVAILABLE：
    库存不足时重试只会让用户等更久，不会有货。
    """

    code = grpc.StatusCode.FAILED_PRECONDITION
    retryable = False


class SkuNotFound(BusinessError):
    code = grpc.StatusCode.NOT_FOUND
    retryable = False


class PaymentDeclined(BusinessError):
    """支付被拒。同样不可重试——卡里没钱，重试还是没钱。"""

    code = grpc.StatusCode.FAILED_PRECONDITION
    retryable = False


class OrderNotFound(BusinessError):
    code = grpc.StatusCode.NOT_FOUND
    retryable = False


class UpstreamUnavailable(BusinessError):
    """下游不可用。这是我们**唯一**应该重试的业务异常。

    它通常由"调下游时拿到 UNAVAILABLE"包装而来，保留"可重试"语义
    向上传递，让上层决定重试还是降级。
    """

    code = grpc.StatusCode.UNAVAILABLE
    retryable = True


# ---------------------------------------------------------------------------
async def abort(context: grpc.aio.ServicerContext, exc: BusinessError) -> None:
    """把业务异常转成 gRPC 错误，终止本次调用。

    为什么用 context.abort 而不是直接 raise：
    abort 会正确地设置状态码和 details 并结束 RPC；直接 raise 一个普通异常
    会让客户端收到 UNKNOWN，丢失我们精心设计的语义。

    ★★ 这里有一个非常容易踩、而且**不会报错**的坑：
       grpc.aio 的 ServicerContext.abort 是 `async def`，**必须 await**。

       同步版 grpc（grpc._server）的 abort 是普通函数，直接调用即可。
       大量教程和从同步代码迁移过来的写法都是 `context.abort(...)` ——
       在 aio 里这样写**不会报错**，它只是创建了一个永远不会被 await 的
       协程对象，然后被垃圾回收。abort 从头到尾没有发生。

       后果：handler 继续往下执行，客户端收到一个**正常响应**。
       本该报错的地方静默成功了 —— 比报错难查一百倍。
       唯一的线索是控制台里的 RuntimeWarning:
       "coroutine 'abort' was never awaited"，
       而它很容易被日志淹没。

       验证方式：.venv/Lib/site-packages/grpc/aio/_base_server.py 第 196 行
       写的是 `async def abort(`。也可以看 grpc_health 官方实现，
       里面是 `await context.abort(...)`。
    """
    await context.abort(exc.code, exc.message)


def from_rpc_error(exc: grpc.aio.AioRpcError) -> BusinessError:
    """把客户端拿到的 gRPC 错误，还原成带语义的业务异常。

    这一步存在的意义：让"要不要重试"这个判断有统一依据，
    而不是在每个调用点散落 if code == ... 的判断。
    """
    code = exc.code()
    detail = exc.details() or code.name
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        return OutOfStock(detail)
    if code == grpc.StatusCode.NOT_FOUND:
        return OrderNotFound(detail)
    if is_retryable(code):
        return UpstreamUnavailable(f"{code.name}: {detail}")
    return BusinessError(f"{code.name}: {detail}")
