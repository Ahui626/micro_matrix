"""
[问题]   服务地址写死会怎样？扩缩容、重启、故障都会让地址变，
         改配置 + 重启 = 停机。怎么让服务自己"报到"，让调用方"查到"？
[本文件] 基于 Consul 的服务注册与发现：注册（带 TTL 健康检查）、心跳续租、
         解析（带本地缓存 + 静态兜底）、注销。用 httpx 手写 Consul 的 HTTP API。
[面试]   - 服务注册中心的四个核心动作是什么？
           （注册 register / 健康检查 health check / 发现 resolve / 注销 deregister。
             这四件事在 Consul、etcd、Eureka、Nacos 里全都存在，只是叫法不同。）
         - TTL 健康检查和"注册中心主动探活"有什么区别？
           （TTL：服务自己定时报"我还活着"，超时没报就判死。好处是不用知道
             服务端口在哪、能否被注册中心访问到；代价是服务要写心跳逻辑。
             主动探活：注册中心定时去 ping 服务的端口。好处是服务无感知；
             代价是注册中心必须能连到服务的端口 —— 在容器/多网段拓扑下经常连不到。）
         - 服务挂了，注册中心怎么把它摘掉？
           （TTL 过期 → 标记 critical → 发现时用 passing=true 过滤掉 →
             DeregisterCriticalServiceAfter 到期后彻底删除。）
         - 注册中心是 CP 还是 AP？
           （Consul/etcd 用 Raft 保证强一致，是 CP；Eureka 牺牲一致性换可用，是 AP。
             分区发生时 CP 选一致性（可能拒绝写入）、AP 选可用性（可能返回旧数据）。）
         - K8s 里为什么通常不再需要独立注册中心？
           （K8s 的 Service + DNS 已经接管了发现：Service 有一个稳定的 DNS 名，
             解析出所有 Pod 的 IP。所以本文件在 K8s 里被 MM_DISCOVERY=static 跳过。）
[取舍]   用 Consul 的 TTL 健康检查 + 服务主动心跳，而不是让 Consul 主动探活。
         理由：心跳是"服务自己报告我还活着"，天然绕开"Consul 探不到容器/宿主机
         端口"的网络拓扑问题；而且这个"租约 + 续租"机制和 etcd 的 lease keepalive
         是同一种东西 —— 讲一次等于两个都讲。
         用 httpx 手写 HTTP 调用而不是 python-consul 客户端库：少一个依赖，
         而且注册/发现就这么四个端点，手写反而能把"注册中心到底存了什么"
         完全暴露出来。代价：没有服务端推送（watch），只能靠客户端缓存 TTL 定期刷新。
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from common import settings

log = logging.getLogger(__name__)

# 客户端解析结果的本地缓存：service -> (address, 过期时间戳)。
# 为什么要有缓存：如果每次调下游都去问一次注册中心，注册中心就成了新的单点
# 和延迟放大器。真实的服务发现客户端（比如 gRPC 的 resolver）都是
# "拉一次 + 本地缓存 + TTL 过期再刷新"。这里的 TTL 很短（默认 5s），
# 足够演示"地址变了客户端能跟上"。
_resolve_cache: dict[str, tuple[str, float]] = {}

# 所有注册/发现请求共用一个 AsyncClient。httpx 的 AsyncClient 内部维护连接池，
# 复用它才能避免每次请求都重新做 TCP 握手。
_client: httpx.AsyncClient | None = None


def _http() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=settings.settings.consul_url,
            # 注册中心是基础设施，不能因为它在抖就让业务请求陪着一起卡。
            # 2 秒超时，配合下面的异常回落，保证 Consul 挂了业务照常跑。
            timeout=httpx.Timeout(2.0),
        )
    return _client


async def close() -> None:
    """关闭到 Consul 的 HTTP 客户端。优雅停机时调用。"""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def split_addr(addr: str) -> tuple[str, int]:
    """把 "127.0.0.1:50052" 拆成 (host, port)，用于注册时填 Address/Port。

    注意：host 是"对外广播的地址"。本机开发时它等于监听地址（127.0.0.1）；
    多机部署时应换成该机的真实 IP，否则别的机器解析到 127.0.0.1 会连到自己身上去。
    这正是"监听地址"和"广播地址"的区别 —— K8s 里两者也常常不同（见 docs/7）。
    """
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def _check_id(service_id: str) -> str:
    """TTL 健康检查的 ID。约定用 service:<id>，和 Consul 默认保持一致。"""
    return f"service:{service_id}"


def _enabled() -> bool:
    """discovery_mode=consul 时启用注册中心；static 时全部动作退化为空。
    K8s 里设 static：发现交给 Service + DNS，本文件不再参与。"""
    return settings.settings.discovery_mode == "consul"


# ---------------------------------------------------------------------------
# 注册 / 心跳 / 注销 / 解析 —— 注册中心的四个核心动作
# ---------------------------------------------------------------------------
async def register(service_id: str, name: str, host: str, port: int) -> None:
    """把一个服务实例注册进 Consul，并给它挂一个 TTL 健康检查。

    注册**不是**健康检查：注册只是"我存在"，健康检查才是"我还活着"。
    这两件事分开，是因为一个进程可能注册了但已经不响应（死锁、假死）。
    """
    if not _enabled():
        return
    payload = {
        "ID": service_id,       # 实例唯一 ID。多实例部署时每个实例必须不同
        "Name": name,           # 服务名。发现时用这个名字查
        "Address": host,        # 客户端连接用的地址
        "Port": port,           # 客户端连接用的端口
        "Check": {
            "CheckID": _check_id(service_id),
            # TTL：超过这个时长没收到心跳，Consul 判为不健康（critical）。
            "TTL": f"{settings.settings.consul_ttl_s}s",
            # 长期不健康就自动删除，而不是留一个僵尸条目永远挂着。
            "DeregisterCriticalServiceAfter": "1m",
        },
    }
    try:
        await _http().put("/v1/agent/service/register", json=payload)
        log.info("已注册到 Consul：%s → %s:%d", service_id, host, port)
    except Exception as exc:  # noqa: BLE001 —— 注册失败不能拖垮服务
        log.warning("注册到 Consul 失败（%s）。服务继续运行，客户端将回落静态地址。",
                    type(exc).__name__)


async def heartbeat(service_id: str) -> None:
    """给某个实例的 TTL 检查续命。必须定期调用，间隔要显著小于 TTL。"""
    try:
        await _http().put(f"/v1/agent/check/pass/{_check_id(service_id)}")
    except Exception as exc:  # noqa: BLE001
        # 心跳失败不致命：Consul 恢复后下一次心跳又会把 TTL 续上。
        # 但连续失败超过 TTL 会被判 critical，所以这里打 warning 而不是 debug ——
        # 它是"接下来会被摘掉"的预警。
        log.warning("Consul 心跳失败：%s", type(exc).__name__)


async def deregister(service_id: str) -> None:
    """把实例从注册表删掉。优雅停机时调用，避免调用方解析到一个死地址。"""
    if not _enabled():
        return
    try:
        await _http().put(f"/v1/agent/service/deregister/{service_id}")
        log.info("已从 Consul 注销：%s", service_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("从 Consul 注销失败：%s", type(exc).__name__)


async def resolve(name: str, default: str) -> str:
    """解析某个服务的当前地址。返回 "host:port"。

    任何失败都回落到 default（配置里的静态地址）。这是刻意的：
    注册中心是基础设施，它出问题不该让业务链路断掉 —— 最多退化回
    "写死地址"的旧行为。
    """
    if not _enabled():
        return default

    now = time.monotonic()
    cached = _resolve_cache.get(name)
    if cached is not None and cached[1] > now:
        return cached[0]

    try:
        resp = await _http().get(
            f"/v1/health/service/{name}", params={"passing": "true"}
        )
        resp.raise_for_status()
        entries = resp.json()
    except Exception as exc:  # noqa: BLE001
        # 负缓存：失败也缓存回落结果。否则 Consul 一挂，每个请求都要白等一次
        # 解析超时 —— 等于把注册中心的故障放大到每个请求上。
        log.warning("从 Consul 解析 %s 失败（%s），回落静态地址 %s", name, type(exc).__name__, default)
        _resolve_cache[name] = (default, now + settings.settings.discovery_cache_s)
        return default

    if not entries:
        log.warning("Consul 里没有健康的 %s 实例，回落静态地址 %s", name, default)
        _resolve_cache[name] = (default, now + settings.settings.discovery_cache_s)
        return default

    # 取第一个健康实例。单实例场景下这就是唯一一个；多实例场景下这里应该
    # 做负载均衡（轮询/加权随机）。本项目只演示"发现"，不演示"均衡"——
    # 但"发现拿到的是一个列表，均衡是拿到列表之后的第二步"这个认知很重要。
    svc = entries[0]["Service"]
    addr = svc.get("Address") or entries[0]["Node"]["Address"]
    target = f"{addr}:{svc['Port']}"
    _resolve_cache[name] = (target, now + settings.settings.discovery_cache_s)
    return target


# ---------------------------------------------------------------------------
# 后台任务：注册 → 心跳循环 → 停机时注销
# ---------------------------------------------------------------------------
async def run_registrar(
    service_id: str,
    name: str,
    host: str,
    port: int,
    stop_event: asyncio.Event,
) -> None:
    """服务的"报到"后台任务，与 order 的 relay/consumer 同构。

    绑定 stop_event 而不是被直接 cancel：cancel 会在任意 await 点硬切，
    可能让 deregister 没执行 —— 服务虽然退了，注册表里还留着一个僵尸条目，
    调用方会持续解析到一个死地址。置位 stop_event 让任务自己走到注销再退出。

    注册时机由调用方决定：必须在服务**就绪之后**再调用本函数，
    这样注册表里永远不会出现"还没准备好接流量"的实例。
    """
    if not _enabled():
        # 静态模式：不注册也不心跳，但**必须等停机信号再返回**，而不是立刻退出。
        # 否则 order/main.py 的 FIRST_COMPLETED 会把这个任务当成"意外退出"，
        # 触发整个进程停机。这里让它像 relay/consumer 一样监听 stop_event，
        # 保持"后台任务"的语义。
        await stop_event.wait()
        return

    await register(service_id, name, host, port)
    # 注册后立刻心跳一次：TTL 检查的初始状态是 critical（不健康），
    # 要等第一次心跳才会翻成 passing。不立刻心跳的话，服务要等满一个心跳间隔
    # 才可被发现 —— 这段"注册了但还没被发现"的空窗会白白拖慢启动。
    await heartbeat(service_id)
    interval = settings.settings.consul_heartbeat_s
    try:
        while True:
            try:
                # 等待停机信号，超时就心跳一次。用 wait_for 而不是 sleep，
                # 是为了停机时能立刻响应，不用等满一个心跳间隔。
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break  # 收到停机信号，跳出循环去注销
            except asyncio.TimeoutError:
                await heartbeat(service_id)
    finally:
        # 无论正常停机还是被异常打断，都要注销 —— finally 保证这一点。
        await deregister(service_id)
