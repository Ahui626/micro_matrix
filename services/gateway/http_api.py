"""
[问题]   HTTP 入口要做四件事：限流、生成/透传 trace_id、调下游、记录指标。
         其中有两处"上下文作用域"的坑，写错了不会报错，只会静默失效。
[本文件] FastAPI 应用。用**纯 ASGI 中间件**，不用 @app.middleware("http")。
[面试]   - ★ 为什么这里用纯 ASGI 中间件，而不是常见的 @app.middleware("http")？
           （`@app.middleware("http")` 走的是 Starlette 的 BaseHTTPMiddleware，
             它会把下游应用跑在一个**新的 asyncio Task** 里。
             而 contextvars 在 Task 之间是隔离的 ——
             于是你在中间件里 set 的 trace_id，路由函数里读不到；
             路由函数里 set 的，中间件也读不到。
             症状是"trace_id 有时候有、有时候没有"，非常难查。
             纯 ASGI 中间件直接在同一个 Task 里调用下游，不存在这个问题。
             这和我写在 common/trace.py 里的 gRPC 拦截器坑是**同一类问题**：
             都是没想清楚"这段代码在哪个上下文作用域里跑"。）
         - 网关层的职责边界在哪？
           （横切关注点：限流、鉴权、trace 起点、协议转换。
             不该在这里做业务判断 —— 一旦开始做，
             业务逻辑就被分散到了网关和下游两处，日后必然不一致。）
         - ★ 用请求路径当指标的标签会有什么问题？
           （高基数（high cardinality）。/orders/123、/orders/124……
             每个 ID 都变成一个独立的时间序列，指标系统的内存会被打爆。
             正确做法是**用路由模板**（/orders/{id}）而不是实际路径。
             下面 _metric_name 做了这件事。这是运维里非常常见的事故。）
[取舍]   不用 Starlette 的中间件栈，手写两个简单 ASGI 中间件。
         理由：能看清"中间件本质就是一个 callable，拿到 scope/receive/send"。
         代价：没有现成的异常处理和请求体解析，得自己来。
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager

import grpc
import order_pb2
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from common import clients, discovery, errors, metrics, ratelimit, redisx, settings, trace

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 中间件
# ---------------------------------------------------------------------------
class TraceMiddleware:
    """给每个请求绑定 trace_id，并把它回写到响应头。

    回写响应头这一步很实用：出问题时用户可以把他看到的 trace_id 报给你，
    你直接 grep 日志就能还原整条链路。
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            # 非 HTTP 流量（lifespan 事件）直接透传
            await self.app(scope, receive, send)
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        # 调用方带了就用它的（链路延续），没带就新建一个（链路起点）
        trace_id = headers.get(trace.TRACE_HEADER) or trace.new_trace_id()
        token = trace.bind(trace_id)

        async def send_with_trace(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append(
                    (trace.TRACE_HEADER.encode(), trace_id.encode())
                )
            await send(message)

        try:
            await self.app(scope, receive, send_with_trace)
        finally:
            # 一定要 reset：这个 Task 之后可能被复用（比如 keep-alive 的下一个请求），
            # 不 reset 会让 trace_id 泄漏到下一个请求上。
            trace.unbind(token)


def _metric_name(scope) -> str:
    """给请求算一个指标名。

    ★ 用**路由模板**而不是实际路径。
    实际路径里带 ID（/orders/375001968860467200），
    每个 ID 都会变成一条独立的时间序列 —— 这就是高基数，
    会把指标系统打爆。路由模板只有有限几个，才是正确的标签。
    """
    route = scope.get("route")
    path = getattr(route, "path", None) or scope.get("path", "unknown")
    return f"http{path}"


class MetricsMiddleware:
    """记录每个请求的耗时和状态码。"""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        captured = {"status": 0}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                captured["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # 注意：这里是"中间件的 finally"，此时 scope["route"] 已经被路由填好了，
            # 所以 _metric_name 能拿到路由模板。
            name = _metric_name(scope)
            metrics.metrics.observe(name, time.perf_counter() - start)
            metrics.metrics.inc("http_requests_total")
            if captured["status"] >= 500:
                metrics.metrics.inc("http_5xx_total")
            elif captured["status"] >= 400:
                metrics.metrics.inc("http_4xx_total")


# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_app: FastAPI):
    """FastAPI 的启动/关闭钩子。

    为什么要用它而不是在模块顶层 connect：
    模块顶层会在 **import 时**执行，而 uvicorn 的 reload 模式会多次 import ——
    于是你会建出多份连接池。资源和导入必须分开。
    """
    from common import chaos, logging as mmlogging

    mmlogging.setup("gateway", settings.settings.log_level)
    log.info("=" * 64)
    log.info("gateway 启动中，监听 %s:%s", settings.settings.gateway_host, settings.settings.gateway_port)
    log.info("上游订单服务 → %s", settings.settings.order_addr)
    log.info("限流：每用户 %d 请求/秒", settings.settings.rate_limit_per_sec)
    log.info("故障注入状态：%s", chaos.describe())
    log.info("=" * 64)

    redisx.connect(settings.settings.redis_url)
    # gateway 是纯调用方（没人通过注册中心发现它），所以它不注册自己，
    # 只通过 discovery.resolve 解析下游 order 的地址。注册/心跳只属于
    # "会被别人调用"的服务 —— 见 inventory/order 的 main.py。
    yield
    log.info("gateway 停机中，关闭资源…")
    await clients.close_all()
    await discovery.close()
    await redisx.close()


app = FastAPI(title="micro_matrix gateway", lifespan=lifespan)

# 注意顺序：add_middleware 是**后加的在最外层**。
# 所以 Trace 在 Metrics 外面 —— trace_id 先绑定，再开始计时，符合直觉。
app.add_middleware(MetricsMiddleware)
app.add_middleware(TraceMiddleware)


# ---------------------------------------------------------------------------
# 请求/响应模型（pydantic 只在 HTTP 边界用）
# ---------------------------------------------------------------------------
class CreateOrderBody(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=64)
    sku: str = Field(..., min_length=1, max_length=64)
    quantity: int = Field(..., ge=1, le=1000)

    # 幂等键。可选 —— 不传的话网关生成一个。
    # 但**调用方应该自己传**：只有调用方知道自己"重试了同一个逻辑操作"。
    # 网关代生成的话，客户端每次重试都会拿到新的 key，幂等就失效了。
    request_id: str | None = Field(None, max_length=64)


class OrderView(BaseModel):
    id: int
    status: str
    sku: str
    quantity: int
    user_id: str
    deduplicated: bool = False


# ---------------------------------------------------------------------------
def _rpc_to_http(exc: grpc.aio.AioRpcError) -> HTTPException:
    """gRPC 状态码 → HTTP 状态码。

    ★ 映射错了会让客户端做出错误决定：
    把"服务暂时不可用"映射成 400，客户端会认为"请求有问题，重试没用"，
    于是放弃重试 —— 而这本来是该重试的。
    所以这里同时把"是否可重试"通过响应头告诉客户端。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 注意：这条路径处理的是**异常**。                                  │
    │                                                                  │
    │ 而"库存不足"在本项目里**不走这里** —— 它返回的是                    │
    │ 201 + `order.status = "CANCELLED"`。                              │
    │                                                                  │
    │ 为什么？这是 Saga 的一个设计决策：**失败应该表达为状态还是异常？**  │
    │   订单记录**确实被创建了**（数据库里有这一行，可以查到、可以对账）。│
    │   如果返回 409，等于告诉客户端"什么都没发生" —— 而事实上有一个    │
    │   处于 CANCELLED 的订单存在。客户端再去查会看到一个它以为不存在的单。│
    │   而且 Saga 的结果可能是**异步**确定的，异常语义根本承载不了。      │
    │                                                                  │
    │   所以这里选"状态"。代价是客户端必须检查响应体里的 status 字段，    │
    │   不能只看 HTTP 状态码。这是个真实的取舍，不是疏漏。               │
    │                                                                  │
    │ 下面 FAILED_PRECONDITION → 409 的映射依然是对的，                 │
    │ 它适用于"连订单记录都不该创建"的前置条件校验（比如账号被冻结）。    │
    │ 本项目当前的 Saga 没有这类校验，所以这条映射暂时用不到 ——           │
    │ 但它是通用规则的一部分，去掉反而会让映射表不完整。                 │
    └──────────────────────────────────────────────────────────────────┘
    """
    code = exc.code()
    mapping = {
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.FAILED_PRECONDITION: 409,  # 前置条件不满足（不是库存不足，见上）
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.DEADLINE_EXCEEDED: 504,    # 网关超时
        grpc.StatusCode.UNAVAILABLE: 503,          # 下游不可用
        grpc.StatusCode.RESOURCE_EXHAUSTED: 429,
    }
    status_code = mapping.get(code, 502)  # 默认：坏网关（下游返回了看不懂的东西）

    headers = {}
    if errors.is_retryable(code):
        # 告诉客户端"这个错误值得重试"。很多客户端库会读 Retry-After。
        headers["X-Retryable"] = "true"
        headers["Retry-After"] = "1"

    log.warning("上游返回 %s → HTTP %d（可重试=%s）", code.name, status_code, bool(headers))
    return HTTPException(status_code=status_code, detail=f"{code.name}: {exc.details()}", headers=headers or None)


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------
@app.post("/orders", status_code=201, response_model=OrderView)
async def create_order(body: CreateOrderBody):
    # ---- 1. 限流 ----
    # 放在最前面：被限流的请求不该消耗任何下游资源。
    result = await ratelimit.check_user(
        body.user_id,
        capacity=settings.settings.rate_limit_per_sec,
        rate_per_sec=settings.settings.rate_limit_per_sec,
    )
    if result.rejected:
        metrics.metrics.inc("rate_limited_total")
        log.warning("用户 %s 被限流，剩余令牌 %d", body.user_id, result.remaining)
        raise HTTPException(
            status_code=429,
            detail="请求过于频繁",
            headers={"Retry-After": "1", "X-RateLimit-Remaining": str(result.remaining)},
        )

    request_id = body.request_id or uuid.uuid4().hex
    stub = await clients.order_stub()

    try:
        # ---- 2. 调下游 ----
        # ★ deadline 从这里开始：gateway 是整条链路的预算源头，
        # 它会随请求一路传下去，每一跳自动扣减已消耗的时间。
        async with metrics.metrics.timer("order.create"):
            resp = await stub.CreateOrder(
                order_pb2.CreateOrderRequest(
                    request_id=request_id,
                    user_id=body.user_id,
                    sku=body.sku,
                    quantity=body.quantity,
                ),
                timeout=settings.settings.gateway_deadline_ms / 1000.0,
            )
    except grpc.aio.AioRpcError as exc:
        metrics.metrics.inc("order_create_failed_total")
        raise _rpc_to_http(exc) from exc

    if resp.deduplicated:
        metrics.metrics.inc("order_deduplicated_total")

    return OrderView(
        id=resp.order.id,
        status=resp.order.status,
        sku=resp.order.sku,
        quantity=resp.order.quantity,
        user_id=resp.order.user_id,
        deduplicated=resp.deduplicated,
    )


@app.get("/orders/{order_id}", response_model=OrderView)
async def get_order(order_id: int):
    stub = await clients.order_stub()
    try:
        async with metrics.metrics.timer("order.get"):
            row = await stub.GetOrder(
                order_pb2.GetOrderRequest(id=order_id),
                timeout=settings.settings.gateway_deadline_ms / 1000.0,
            )
    except grpc.aio.AioRpcError as exc:
        raise _rpc_to_http(exc) from exc

    return OrderView(
        id=row.id, status=row.status, sku=row.sku,
        quantity=row.quantity, user_id=row.user_id,
    )


@app.get("/healthz")
async def healthz():
    """健康检查。生产环境里 K8s 的 readiness 探针打这个接口。

    注意要**真的检查依赖**，不能只返回 200 ——
    一个"进程活着但连不上 Redis"的实例不该被放进负载均衡，
    否则它接到的每个请求都会失败。
    """
    redis_ok = await redisx.health()
    if not redis_ok:
        return JSONResponse(status_code=503, content={"status": "degraded", "redis": "down"})
    return {"status": "ok", "redis": "up"}


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics():
    """Prometheus 文本格式。可以直接接一个 Prometheus 来抓。"""
    return metrics.metrics.render_prometheus()


@app.get("/metrics/json")
async def metrics_json():
    """同样的数据，JSON 格式，给 labs 的断言用。

    注意这里**只有网关自己进程内**的指标。
    比如熔断器的状态在 order 进程里，网关这边读不到 ——
    进程内状态天然无法被其它进程观测。

    ★ 这正是为什么生产环境必须把指标**推送**到 Prometheus 这类中心化系统：
      不是"算不出来"的问题，是"聚合"的问题。
      多实例部署时，10 个实例各有各的 p99，
      你必须在一个地方把它们聚起来才能看到全局。
      下面这个端点给出的数字，只代表**这一个网关实例**。
    """
    return metrics.metrics.snapshot()
