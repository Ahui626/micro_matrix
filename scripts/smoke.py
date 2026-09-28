"""
[问题]   在写任何业务代码之前，先证明这东西在这台机器上是活的。
         一旦 grpc.aio 或 asyncpg 在 Python 3.14 / Windows 上跑不起来，
         必须在写下 2000 行代码之前知道。
[本文件] 五项体检：Python 构建类型、事件循环类型、grpc.aio 往返调用、
         gRPC deadline 是否真的生效、asyncpg 连接、Redis 读写。
         全部跑在裸 asyncio.run() 下 —— 不碰事件循环策略。
[面试]   - Windows 上 asyncio 的默认事件循环是哪个？为什么不该手动改它？
         - 为什么 asyncpg 比 psycopg2 更适合 asyncio 服务？
         - free-threaded 构建（cp314t）和标准构建差在哪？为什么这里必须是标准构建？
         - gRPC 的 deadline 和 HTTP 的 timeout 有什么本质区别？
[取舍]   故意不用 pytest。这是"环境体检"，要让人一眼看到每一项的结论和原始
         报错，而不是一个 PASS —— 出问题时能直接知道该改哪里。

用法：
    docker compose up -d
    python scripts/smoke.py
"""

from __future__ import annotations

# 入口脚本固定前缀：向上找到 bootstrap.py 并导入。
# 这样不管从哪个目录、以哪种方式运行（python scripts/x.py / python -m ...）都成立。
# bootstrap 负责两件事：把仓库根和 gen/ 放进 sys.path、把输出编码强制成 UTF-8。
import pathlib as _pathlib
import sys

for _p in [_pathlib.Path(__file__).resolve(), *_pathlib.Path(__file__).resolve().parents]:
    if (_p / "bootstrap.py").exists():
        sys.path.insert(0, str(_p))
        break
import bootstrap  # noqa: F401  pylint: disable=unused-import

import asyncio
import os

import asyncpg
import grpc
import redis.asyncio as aioredis

PG_DSN = os.getenv("MM_PG_DSN", "postgresql://micro:micro@localhost:5432/micro_matrix")
REDIS_URL = os.getenv("MM_REDIS_URL", "redis://localhost:6379/0")

# (体检项, 是否通过, 说明)
RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))


# --------------------------------------------------------------------------
# 1. Python 构建类型
# --------------------------------------------------------------------------
# 为什么关心构建类型：grpcio 和 asyncpg 都是 C 扩展，只对标准构建发布了
# cp314 wheel。如果你不小心装的是 free-threaded 构建（ABI 是 cp314t），
# pip 找不到 wheel 就会退化成"从源码编译"，而这台机器没有 C 编译器 ——
# 安装会以一堆看不懂的报错失败。提前查出来，省半小时。
def check_build() -> None:
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    record(
        "Python 构建",
        gil_enabled,
        f"{sys.version.split()[0]}  标准构建（有预编译 wheel）"
        if gil_enabled
        else f"{sys.version.split()[0]}  free-threaded(cp314t)！无预编译 wheel，会编译失败",
    )


# --------------------------------------------------------------------------
# 2. 事件循环类型
# --------------------------------------------------------------------------
# Windows 上 asyncio 默认用 ProactorEventLoop，grpc.aio 在这个循环上最稳。
# 网上 3.8 时代的老教程会让你设 WindowsSelectorEventLoopPolicy —— 千万不要：
# Selector 循环在 Windows 上不支持 create_subprocess_*，而且这个策略 API
# 在 Python 3.14 已经废弃了。我们的答案是：什么都不设，用裸 asyncio.run()。
def check_loop() -> None:
    loop = asyncio.get_running_loop()
    record("事件循环", True, f"{type(loop).__name__}（Windows 默认，不手动干预）")


# --------------------------------------------------------------------------
# 3 & 4. gRPC 往返 + deadline 生效性
# --------------------------------------------------------------------------
# 这里刻意不用 .proto 生成代码，直接用通用 handler —— 体检脚本不该依赖
# 代码生成步骤是否已经跑过。
class _EchoHandler(grpc.GenericRpcHandler):
    def service(self, handler_call_details):
        return grpc.unary_unary_rpc_method_handler(lambda req, ctx: b"pong:" + req)


class _SlowHandler(grpc.GenericRpcHandler):
    def service(self, handler_call_details):
        async def _slow(req, ctx):
            await asyncio.sleep(5)
            return b"late"

        return grpc.unary_unary_rpc_method_handler(_slow)


async def check_grpc() -> None:
    # --- 往返 ---
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((_EchoHandler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = channel.unary_unary("/smoke.Ping/Ping")
            resp = await asyncio.wait_for(stub(b"ping"), timeout=5)
        record("grpc.aio 往返", resp == b"pong:ping", f"响应 {resp!r}")
    except Exception as exc:
        record("grpc.aio 往返", False, f"{type(exc).__name__}: {exc}")
    finally:
        await server.stop(0)

    # --- deadline ---
    # 这是整套系统赖以生存的机制：调用方设定截止时间，服务端超时就自动放弃。
    # 后面 gateway → order → inventory 的逐跳传播全靠它。
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((_SlowHandler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = channel.unary_unary("/smoke.Slow/Slow")
            try:
                await stub(b"x", timeout=0.3)
                record("gRPC deadline", False, "服务端睡了 5 秒，客户端 0.3s 超时居然没触发")
            except grpc.aio.AioRpcError as exc:
                ok = exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                record("gRPC deadline", ok, f"得状态码 {exc.code().name}")
    except Exception as exc:
        record("gRPC deadline", False, f"{type(exc).__name__}: {exc}")
    finally:
        await server.stop(0)


# --------------------------------------------------------------------------
# 5. asyncpg
# --------------------------------------------------------------------------
# asyncpg 是纯 asyncio 的 Postgres 驱动：它不做线程池包装，直接把 socket 交给
# 事件循环。psycopg2 是同步阻塞的，在 asyncio 服务里用它会堵死整个事件循环 ——
# 这是 Python 异步服务最常见的性能事故。
async def check_pg() -> None:
    try:
        conn = await asyncio.wait_for(asyncpg.connect(PG_DSN), timeout=5)
    except Exception as exc:
        record("asyncpg 连接", False, f"{type(exc).__name__}: {exc}   → 先跑 docker compose up -d")
        return
    try:
        one = await conn.fetchval("select 1")
        version = await conn.fetchval("select version()")
        record("asyncpg 连接", one == 1, version.split(",")[0])
    except Exception as exc:
        record("asyncpg 连接", False, f"{type(exc).__name__}: {exc}")
    finally:
        await conn.close()


# --------------------------------------------------------------------------
# 6. Redis
# --------------------------------------------------------------------------
# Redis 在本题里身兼四职：限流、分布式锁、消息队列（Streams）、缓存。
# 一个中间件覆盖四个面试知识点，这是把它选为唯一 NoSQL 的原因。
async def check_redis() -> None:
    client = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.set("mm:smoke", "ok", ex=10)
        value = await client.get("mm:smoke")
        pong = await client.ping()
        record("Redis 读写", value == "ok" and pong, f"GET mm:smoke → {value!r}")
    except Exception as exc:
        record("Redis 读写", False, f"{type(exc).__name__}: {exc}   → 先跑 docker compose up -d")
    finally:
        await client.aclose()


# --------------------------------------------------------------------------
async def main() -> int:
    check_build()
    check_loop()
    await check_grpc()
    await check_pg()
    await check_redis()

    print()
    print("=" * 68)
    print(" micro_matrix 环境体检")
    print("=" * 68)
    for name, ok, detail in RESULTS:
        mark = "通过" if ok else "失败"
        print(f" [{mark}] {name:<18} {detail}")
    print("=" * 68)

    failed = [name for name, ok, _ in RESULTS if not ok]
    if failed:
        print(f"\n{len(failed)} 项没过：{', '.join(failed)}")
        print("基础设施类失败请先执行：docker compose up -d")
        return 1
    print("\n全部通过 —— 可以开始写业务代码了。")
    return 0


if __name__ == "__main__":
    # 裸 asyncio.run()：不设策略，不取 loop，不碰 add_signal_handler。
    raise SystemExit(asyncio.run(main()))
