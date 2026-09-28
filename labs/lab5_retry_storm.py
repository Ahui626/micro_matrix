"""
[问题]   下游抖一下，重试就好了 —— 但如果重试没有抖动、没有预算，
         它会把「一个小抖动」放大成「一场雪崩」：
         下游本来就过载，你还把请求量乘 3 打过去，它永远好不了。
[本文件] 用真实的 common/retry.py，通过三个角度定量演示重试的危险：
         A. 放大倍数    —— 1 次逻辑调用变成几次实际 RPC，以及多层叠加的后果
         B. 预算约束    —— deadline 预算如何让重试提前放弃（确定性）
         C. 抖动的作用  —— 有抖动 vs 无抖动，重试在时间轴上的聚集程度
[面试]   - ★ 一次安全的重试必须满足哪三个条件？
           （1. 只重试**可重试**的错误 —— 参数错误重试一万次还是错
             2. 必须有**退避** —— 立即重试等于持续打压
             3. 必须有**抖动** —— 否则所有调用者会同步重试，形成尖峰
             缺任何一条，重试都会从"自愈机制"变成"故障放大器"。）
         - ★ 抖动的必要性，用数字说话是什么样？
           （假设 1000 个客户端同时失败、同时等 1 秒、同时重试 ——
             下游会在那一瞬间收到 1000 次调用，本来就过载的它再次被打垮，
             然后所有人再次失败、再次同步重试 —— 这就是**亚稳态故障**：
             即使原始故障已经消失，系统也回不到正常状态。
             加了抖动，重试被摊开在时间轴上，峰值立刻降一个量级。
             本 lab 的 C 部分就是这个现象的小规模复现。）
         - ★ 重试和 deadline 是什么关系？
           （重试必须活在**总预算**内。上游只给你 500ms，
             你重试 3 次、每次等 1 秒 —— 第 2 次还没发出去上游就超时了，
             剩下的重试全是浪费，而且是**对下游的额外伤害**。
             所以 retry 必须携带预算，并在"等完这次退避就超预算"时提前放弃。）
         - 重试该放在哪一层？和熔断怎么配合？
           （熔断在外、重试在内。熔断打开时直接拒绝，根本不给重试放大的机会。
             顺序反了的话，熔断永远拦不住重试产生的放大。
             见 services/order/service.py 的 _reserve_inventory。）
         - 多层重试叠加会怎样？
           （A→B→C 三层各重试 3 次，最坏情况下一个用户请求会变成 27 次 C 的调用。
             所以**只在最贴近故障的那一层重试**，或者让上层完全信任下层的重试。
             盲目层层重试是分布式系统里最经典的放大器。）
[取舍]   这个 lab 自己 spawn 一个带故障注入的 inventory 实例在**另一个端口**上，
         而不是去改正在运行的那个服务。
         理由：故障注入是环境变量控制的，改了要重启服务 ——
         那会破坏你正在观察的其它状态。
         自己起一个独立实例，既能精确控制注入参数，
         又不干扰 scripts/run_all.py 起的那套。
         代价：多一个进程要管理，别忘了最后关掉它（finally 里做了）。

运行前提：只需要中间件。
    docker compose up -d
    python labs/lab5_retry_storm.py
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
import os
import socket
import subprocess
import time

import grpc
import inventory_pb2
import inventory_pb2_grpc

from common import retry

ROOT = bootstrap.REPO_ROOT
CALLERS = 30
ATTEMPTS = 3
NO_JITTER_DELAY_S = 0.05
WINDOW_S = 0.015  # 用来衡量"聚集程度"的时间窗口


def _free_port() -> int:
    """让操作系统分配一个空闲端口。

    ★ 为什么不写死一个端口（比如 50062）：
      Windows 上 Hyper-V / WSL / Docker 会保留大段端口不给用。
      用 `netsh interface ipv4 show excludedportrange protocol=tcp` 能看到
      一堆保留范围（本机就有 50060-50159）。写死端口会随机地绑不上，
      而且报错信息是 grpc 底层的 "Failed to bind"，看不出真实原因。
      让 OS 分配（bind 到端口 0）能保证拿到的是真正可用的端口。

      注意这里有个理论上的竞态：我们释放端口到子进程真正绑定之间，
      别人可能抢走它。实际概率极低，而且真撞上了会立刻报错、不会静默出错。
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


CHAOS_PORT = _free_port()
CHAOS_ADDR = f"127.0.0.1:{CHAOS_PORT}"

# 全局的 channel 和 stub。复用一个 channel 而不是每次调用都新建 ——
# 这本身就是 common/clients.py 里讲的那件事：
# 每次新建 channel = 每次重新握手，高频下开销远超业务本身。
_channel: grpc.aio.Channel | None = None
_stub = None


def _open_channel() -> None:
    global _channel, _stub
    _channel = grpc.aio.insecure_channel(CHAOS_ADDR)
    _stub = inventory_pb2_grpc.InventoryServiceStub(_channel)


async def _close_channel() -> None:
    if _channel is not None:
        await _channel.close()


async def _wait_port(port: int, timeout_s: float = 25.0) -> bool:
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        await asyncio.sleep(0.2)
    return False


_chaos_log_handle = None


def _spawn_chaos_inventory() -> subprocess.Popen:
    """起一个"总是失败"的 inventory 实例，用来稳定触发重试。

    MM_CHAOS_INVENTORY_ERROR_RATE=1.0 让每次 Reserve 都返回 UNAVAILABLE。
    用 UNAVAILABLE 而不是业务错误 —— 它是"可重试"的状态码，
    这正是重试逻辑会去重试的那一类。

    ┌──────────────────────────────────────────────────────────────────┐
    │ 两个工程细节，都值得一提：                                        │
    │                                                                  │
    │ 1. 日志文件句柄必须被留住并显式关闭。                            │
    │    写成 `open(...)` 直接传给 Popen、不保留引用的话，             │
    │    父进程结束时这个句柄可能仍然开着 —— 表现是文件删不掉          │
    │    （Windows 报 "Device or resource busy"），而且日志可能没刷完。 │
    │                                                                  │
    │ 2. 如果这个 lab 被**强杀**（比如 kill -9、或者输出管道被提前关闭），│
    │    子进程会活下来变成孤儿，继续占着端口。                        │
    │    彻底解决要用 Windows Job Object（把子进程绑到父进程的生命周期），│
    │    那对一个教学 lab 来说太重了。                                  │
    │    实际影响很小：端口是动态分配的，孤儿不会和别的东西冲突；       │
    │    真遇到了用 `tasklist | findstr python` + `taskkill /F /PID` 清掉。│
    └──────────────────────────────────────────────────────────────────┘
    """
    global _chaos_log_handle

    env = {
        **os.environ,
        "MM_INVENTORY_ADDR": CHAOS_ADDR,
        "MM_CHAOS_INVENTORY_ERROR_RATE": "1.0",
    }
    log_path = ROOT / "logs" / "lab5-chaos-inventory.log"
    log_path.parent.mkdir(exist_ok=True)
    # 保留引用，好在 finally 里关闭
    _chaos_log_handle = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "services" / "inventory" / "main.py")],
        env=env, stdout=_chaos_log_handle, stderr=subprocess.STDOUT, cwd=str(ROOT),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    return proc


def _close_chaos_handle() -> None:
    global _chaos_log_handle
    if _chaos_log_handle is not None:
        _chaos_log_handle.close()
        _chaos_log_handle = None


# ---------------------------------------------------------------------------
async def _measure_attempts(
    use_jitter: bool, callers: int = CALLERS
) -> tuple[int, list[float]]:
    """并发发起 callers 个逻辑调用，记录**每一次实际尝试**的发送时刻。

    返回值：(总尝试次数, 所有重试尝试（不含第一次）的发送时刻)
    """
    attempt_times: list[float] = []
    retry_times: list[float] = []
    lock = asyncio.Lock()

    async def one_call(idx: int) -> None:
        # 每次逻辑调用用一个独立的 request_id，
        # 这样即使重试，幂等也不会掩盖"确实发了 3 次"这个事实
        # （我们的 inventory 在 chaos 注入下压根不会走到幂等检查）。
        request_id = f"lab5-{use_jitter}-{idx}"
        counter = {"n": 0}

        async def make_call():
            async with lock:
                counter["n"] += 1
                now = time.perf_counter()
                if counter["n"] > 1:
                    retry_times.append(now)
                attempt_times.append(now)
            return await _stub.Reserve(
                inventory_pb2.ReserveRequest(
                    request_id=request_id, sku="LAB5-SKU", quantity=1
                ),
                timeout=10.0,
            )

        try:
            if use_jitter:
                await retry.call_with_retry(make_call, attempts=ATTEMPTS, budget_s=10.0)
            else:
                await retry.call_with_retry_no_jitter(
                    make_call, attempts=ATTEMPTS, delay_s=NO_JITTER_DELAY_S
                )
        except Exception:
            # 全部失败是预期的 —— 我们关心的只是"尝试了几次、什么时候发的"
            pass

    await asyncio.gather(*(one_call(i) for i in range(callers)))
    return len(attempt_times), retry_times


def _dist(counts: list[int]) -> str:
    """把次数列表画成分布，比如 "2次×12, 3次×3"。

    为什么输出分布而不是平均值：平均值会掩盖"有时候走满、有时候提前放弃"
    这个事实，而后者恰恰是这段逻辑的真实行为。
    """
    from collections import Counter

    return ", ".join(f"{value}次×{count}" for value, count in sorted(Counter(counts).items()))


def _max_in_window(times: list[float], window_s: float = WINDOW_S) -> int:
    """在时间轴上找任意 window_s 窗口内最多有多少次尝试。

    这个数字就是"尖峰高度" —— 下游在这一瞬间要承受的并发量。
    抖动的作用就是把峰值削平。
    """
    if not times:
        return 0
    ordered = sorted(times)
    best = 0
    start = 0
    for end in range(len(ordered)):
        while ordered[end] - ordered[start] > window_s:
            start += 1
        best = max(best, end - start + 1)
    return best


# ---------------------------------------------------------------------------
async def case_a_amplification() -> int:
    """A. 放大倍数：无预算时，每次逻辑调用都会用满重试次数。"""
    attempts, _ = await _measure_attempts(use_jitter=True, callers=1)
    return attempts


async def _attempts_with_budget(budget_s: float | None, tag: str) -> int:
    """跑一次逻辑调用，返回实际发出的 RPC 次数。"""
    attempt_count = {"n": 0}
    async def make_call():
        attempt_count["n"] += 1
        return await _stub.Reserve(
            inventory_pb2.ReserveRequest(
                request_id=f"lab5-{tag}-{attempt_count['n']}", sku="LAB5-SKU", quantity=1
            ),
            timeout=10.0,
        )

    try:
        await retry.call_with_retry(make_call, attempts=ATTEMPTS, budget_s=budget_s, label=tag)
    except Exception:
        pass
    return attempt_count["n"]


async def case_b_budget(trials: int = 15) -> tuple[list[int], list[int]]:
    """B. 预算约束：同样的失败，加预算后重试更早放弃。

    ★ 注意这里必须看**分布**而不是单次结果：
      退避时间是 `random.uniform(0, ceiling)` —— 抽到很小的值时，
      即使预算只有 60ms 也够再试一次。
      所以"有预算 = 一定少试一次"是错的。

      这本身就是重试逻辑的一个真实特点：它的行为是**概率性**的。
      你必须按分布来推理，按单次运行下结论会得出错误的规律。

    返回 (无预算的尝试次数列表, 有预算的尝试次数列表)
    """
    no_budget: list[int] = []
    with_budget: list[int] = []
    for i in range(trials):
        no_budget.append(await _attempts_with_budget(None, f"nb{i}"))
        with_budget.append(await _attempts_with_budget(0.06, f"wb{i}"))
    return no_budget, with_budget


async def case_c_jitter() -> tuple[int, int]:
    """C. 抖动：对比重试在时间轴上的聚集程度。

    返回 (无抖动时的尖峰高度, 有抖动时的尖峰高度)
    """
    print(f"  无抖动：{CALLERS} 个调用者同时失败，都在固定 {NO_JITTER_DELAY_S*1000:.0f}ms 后重试")
    _, retry_no_jitter = await _measure_attempts(use_jitter=False)

    print(f"  有抖动：同样 {CALLERS} 个调用者，退避时间在 [0, {NO_JITTER_DELAY_S*1000:.0f}ms] 内随机")
    _, retry_jitter = await _measure_attempts(use_jitter=True)

    return _max_in_window(retry_no_jitter), _max_in_window(retry_jitter)


# ---------------------------------------------------------------------------
async def main() -> int:
    """整个 lab 的主流程。注意 finally 里一定要关掉那个 chaos 实例。"""
    # 这个 lab 故意制造成百上千次重试，common.retry 的 WARNING 日志
    # 会把真正的结果淹没。压到 ERROR —— 我们要看的是统计，不是每条重试记录。
    logging.getLogger("common.retry").setLevel(logging.ERROR)

    print("=" * 78)
    print(" lab5 —— 重试风暴：没有抖动和预算的重试会放大故障")
    print("=" * 78)

    proc = _spawn_chaos_inventory()
    print(f"  已启动故障注入的 inventory（pid={proc.pid}，端口 {CHAOS_PORT}）")
    print(f"  注入：MM_CHAOS_INVENTORY_ERROR_RATE=1.0（每次调用都失败，且返回可重试状态码）")

    try:
        if not await _wait_port(CHAOS_PORT):
            print(" ✗ 故障实例启动失败，看 logs/lab5-chaos-inventory.log")
            return 1
        _open_channel()
        print("  就绪")
        print()

        # --- A ---
        print("A. 放大倍数")
        attempts = await case_a_amplification()
        print(f"  1 次逻辑调用，在无预算限制下实际发出了 {attempts} 次 RPC")
        print(f"  ★ 放大 {attempts} 倍。而如果 A→B→C 三层各重试 3 次，")
        print(f"    最坏情况下一个用户请求会变成 {attempts ** 3} 次最底层的调用。")
        print(f"    这就是为什么**只在最贴近故障的那一层重试**。")
        print()

        # --- B ---
        print("B. 预算约束（看分布，不看单次）")
        no_budget, with_budget = await case_b_budget()
        avg_no = sum(no_budget) / len(no_budget)
        avg_with = sum(with_budget) / len(with_budget)
        early = sum(1 for c in with_budget if c < ATTEMPTS)
        print(f"  无预算     ：平均 {avg_no:.2f} 次  分布 {_dist(no_budget)}")
        print(f"  有预算 60ms：平均 {avg_with:.2f} 次  分布 {_dist(with_budget)}")
        print(f"  ★ {len(with_budget)} 次试验里有 {early} 次提前放弃（没走满 {ATTEMPTS} 次）。")
        print(f"    没提前放弃的那些，是因为抽到的退避恰好很小（退避是随机数）——")
        print(f"    所以这里必须看分布。重试逻辑的行为是**概率性**的，")
        print(f"    按单次运行下结论会得出错误的规律。")
        print()
        print(f"    为什么预算重要：不重试不代表请求会成功，")
        print(f"    它只是让失败**更快地暴露出来**，并且**停止对下游造成额外伤害**。")
        print()

        # --- C ---
        print("C. 抖动的作用（定量）")
        spike_no_jitter, spike_jitter = await case_c_jitter()
        print()
        print(f"  任意 {WINDOW_S*1000:.0f}ms 窗口内的最大重试次数：")
        print(f"    无抖动：{spike_no_jitter}  ← 所有调用者在同一瞬间重试")
        print(f"    有抖动：{spike_jitter}   ← 重试被摊开在时间轴上")
        if spike_jitter > 0:
            print(f"    ★ 峰值降低了约 {spike_no_jitter / max(1, spike_jitter):.1f} 倍")
        print()
        print(f"    把规模放大到 10000 个调用者，无抖动就是 10000 次调用同时到达 ——")
        print(f"    一个本来只是「有点慢」的下游会直接被这一波打垮，")
        print(f"    然后所有人再次失败、再次同步重试。这就是**亚稳态故障**：")
        print(f"    即使最初的故障已经消失，系统也回不到正常状态。")
        print()

        # --- 结论 ---
        print("=" * 78)
        print(" 结论：一次安全的重试必须同时满足三条")
        print("=" * 78)
        print(" 1. 只重试**可重试**的错误（网络类）。业务错误重试无意义。")
        print("    见 common/errors.py 的 RETRYABLE_STATUS 白名单 ——")
        print("    用白名单而不是黑名单，因为错误地重试比不重试更危险。")
        print()
        print(" 2. 必须**退避**（指数增长 + 封顶）。立即重试等于持续打压。")
        print()
        print(" 3. 必须**抖动**。否则所有调用者同步重试，把下游的负载乘 N。")
        print()
        print(" 外加一条：重试必须受**总预算**约束，不能无限重试下去。")
        print()
        print(" 最后，重试要放在**熔断器内侧**（见 services/order/service.py）：")
        print(" 熔断在外、重试在内。顺序反了，熔断永远拦不住重试产生的放大。")
        print()

        ok = (
            attempts == ATTEMPTS
            and avg_with < avg_no
            and spike_no_jitter > spike_jitter
        )
        if ok:
            print(" ✓ 三个实验全部符合预期。")
        else:
            print(f" ? 结果与预期不符：attempts={attempts} "
                  f"平均尝试 {avg_no:.2f}→{avg_with:.2f} "
                  f"尖峰 {spike_no_jitter} vs {spike_jitter}")
        return 0

    finally:
        await _close_channel()
        print("  关闭故障注入的 inventory 实例…")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
        # 子进程彻底退出后再关日志句柄 —— 反过来会丢掉最后几行日志
        _close_chaos_handle()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
