"""
[问题]   三个服务要分别启动、分别看日志。能不能一条命令搞定？
[本文件] 用 subprocess.Popen 启动三个服务，每个输出到自己的日志文件，
         等待就绪后打印访问地址，Ctrl+C 时优雅关闭全部。
[面试]   - ★ Windows 上为什么不能用 multiprocessing 启动这些服务？
           （Windows 没有 fork，multiprocessing 用的是 spawn：
             每个子进程会**重新 import __main__**，
             而且不共享父进程的任何全局状态 ——
             数据库连接池、事件循环、Redis 客户端全都不共享。
             更麻烦的是 spawn 要求 __main__ 有 if __name__ == "__main__" 保护，
             没有的话会无限递归启动子进程。
             跨平台的正解是 subprocess.Popen：每个服务是真正独立的进程，
             这也和"生产环境里它们是不同容器"这个事实一致。）
         - ★ Windows 上怎么"优雅地"通知另一个进程退出？
           （terminate() 是硬杀，不走任何清理逻辑。
             正确方式是给子进程建一个 process group
             （creationflags=CREATE_NEW_PROCESS_GROUP），
             然后用 os.kill(pid, signal.CTRL_BREAK_EVENT) 发 CTRL_BREAK ——
             这个信号 Python 侧对应 signal.SIGBREAK，
             能被 common/lifecycle.py 里的处理器接住，触发优雅停机。）
         - 为什么不能"启动后 sleep 3 秒就算就绪"？
           （慢机器上会失败，快机器上会白等。而且服务崩了你也看不出来。
             正确做法是**轮询就绪信号**（端口能连上 / 健康检查返回 200），
             并设置超时。这就是 K8s readiness 探针在做的事。）
[取舍]   每个服务独立的日志文件，而不是聚合到一个。
         理由：三个进程的输出交织在一个终端里基本没法读，
         而日志要有 trace 关联才有用 —— 交织之后你连"哪行属于哪个服务"都要猜。
         代价：要开三个窗口 tail（或者用下面的提示）。
         生产环境当然是用日志采集系统聚合（Loki/ELK），本地开发不必上那么重。
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

import codecs
import os
import signal
import socket
import subprocess
import time
from pathlib import Path

from common import settings

ROOT = bootstrap.REPO_ROOT
LOGS_DIR = ROOT / "logs"

# 启动顺序有讲究：被依赖的先起。
# inventory 不依赖别人；order 依赖 inventory；gateway 依赖 order。
SERVICES: list[tuple[str, str, int]] = [
    ("inventory", "services/inventory/main.py", 50052),
    ("order", "services/order/main.py", 50051),
]

HTTP_SERVICE = ("gateway", "services/gateway/main.py")
HTTP_PORT = 8000


def _wait_port(port: int, timeout_s: float = 20.0) -> tuple[bool, float]:
    """轮询等待端口可连接。返回 (是否成功, 等待秒数)。

    为什么不用 sleep：慢机器上 sleep 3 秒不够，快机器上白等 3 秒。
    而且如果服务启动失败，sleep 只会让你多等一会儿再发现而已。
    """
    start = time.perf_counter()
    deadline = start + timeout_s
    while time.perf_counter() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            # connect_ex 返回 0 表示成功；不抛异常，正好适合轮询
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True, time.perf_counter() - start
        time.sleep(0.2)
    return False, time.perf_counter() - start


def _spawn(name: str, script: str, log_files: list) -> subprocess.Popen:
    log_path = LOGS_DIR / f"{name}.log"

    # ┌──────────────────────────────────────────────────────────────────┐
    # │ 以**二进制**打开，并手写一个 UTF-8 BOM。这看起来多余，但要解决   │
    # │ 一个很烦人的 Windows 问题。                                      │
    # │                                                                  │
    # │ 症状：用 PowerShell 看日志，中文全是乱码                          │
    # │     order 鏈嶅姟鍚姩涓         ← 其实是 "order 服务启动中"        │
    # │                                                                  │
    # │ 原因：日志文件本身是正确的 UTF-8，但                                │
    # │   Windows PowerShell 5.1 的 Get-Content 在没有 BOM 时，           │
    # │   会按**系统 ANSI 代码页**（中文系统上是 GBK）去解码，            │
    # │   于是 UTF-8 字节被当成 GBK 解释 → 乱码。                        │
    # │                                                                  │
    # │ 解法：在文件开头写一个 BOM（EF BB BF）。                          │
    # │   PowerShell 5.1 和 7 看到 BOM 都会自动按 UTF-8 解码。            │
    # │                                                                  │
    # │ 为什么 Python 的 open(encoding="utf-8") 不够：                    │
    # │   它**不会**写 BOM（这是刻意的，BOM 在 Unix 世界不受欢迎）。      │
    # │   而且子进程是直接往这个文件描述符写字节的，                      │
    # │   所以只能由我们在文件开头显式写一次。                           │
    # │                                                                  │
    # │ 另一个办法是让用户每次加 -Encoding utf8，但那要靠人记住 ——        │
    # │   而人会忘。让文件自己带上身份标识更可靠。                        │
    # └──────────────────────────────────────────────────────────────────┘
    handle = open(log_path, "wb")
    handle.write(codecs.BOM_UTF8)
    handle.flush()
    log_files.append(handle)

    # CREATE_NEW_PROCESS_GROUP 是 Windows 上能发 CTRL_BREAK 的前提。
    # 在 Linux/macOS 上传 0 表示不设特殊标志。
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    proc = subprocess.Popen(
        [sys.executable, str(ROOT / script)],
        stdout=handle,
        stderr=subprocess.STDOUT,
        cwd=str(ROOT),
        creationflags=creationflags,
    )
    print(f"  启动 {name:<10} pid={proc.pid:<7} → {log_path.relative_to(ROOT)}")
    return proc


def _stop_all(procs: list[tuple[str, subprocess.Popen]], log_files: list) -> None:
    """优雅停止所有子进程。"""
    print("\n正在优雅停机…")
    for name, proc in procs:
        if proc.poll() is not None:
            continue
        try:
            if os.name == "nt":
                # CTRL_BREAK → 子进程的 signal.SIGBREAK → 我们的停机处理器
                os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
            else:
                proc.send_signal(signal.SIGTERM)
            print(f"  已通知 {name} 退出")
        except (OSError, ProcessLookupError) as exc:
            print(f"  通知 {name} 失败（{exc}），将强制终止")

    # 给它们时间走完停机流程（服务里设的是 3 秒 grace + 2 秒后台任务）
    deadline = time.perf_counter() + 8.0
    for name, proc in procs:
        remaining = max(0.1, deadline - time.perf_counter())
        try:
            proc.wait(timeout=remaining)
            print(f"  {name} 已退出（exit={proc.returncode}）")
        except subprocess.TimeoutExpired:
            print(f"  {name} 未在时限内退出，强制终止")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    for handle in log_files:
        handle.close()


def main() -> int:
    LOGS_DIR.mkdir(exist_ok=True)

    print("=" * 70)
    print(" micro_matrix 一键启动")
    print("=" * 70)
    print("请确认中间件已就绪：docker compose up -d\n")

    procs: list[tuple[str, subprocess.Popen]] = []
    log_files: list = []

    try:
        # --- 阶段一：被依赖的 gRPC 服务 ---
        for name, script, port in SERVICES:
            procs.append((name, _spawn(name, script, log_files)))

            ok, waited = _wait_port(port)
            if not ok:
                print(f"\n✗ {name} 在 20 秒内没有监听 {port} 端口，启动失败。")
                print(f"  请看 logs/{name}.log 的末尾：")
                log_path = LOGS_DIR / f"{name}.log"
                if log_path.exists():
                    tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-15:]
                    for line in tail:
                        print("    " + line)
                return 1
            print(f"         {name:<10} 就绪（{waited:.1f}s）")

        # --- 阶段二：HTTP 网关 ---
        name, script = HTTP_SERVICE
        procs.append((name, _spawn(name, script, log_files)))
        ok, waited = _wait_port(HTTP_PORT)
        if not ok:
            print(f"\n✗ {name} 在 20 秒内没有监听 {HTTP_PORT} 端口，启动失败。")
            print(f"  请看 logs/{name}.log 的末尾。")
            return 1
        print(f"         {name:<10} 就绪（{waited:.1f}s）")

        print()
        print("=" * 70)
        print(" 全部就绪")
        print("=" * 70)
        print(f"  下单        POST http://127.0.0.1:{HTTP_PORT}/orders")
        print(f"  查单        GET  http://127.0.0.1:{HTTP_PORT}/orders/{{id}}")
        print(f"  健康检查    GET  http://127.0.0.1:{HTTP_PORT}/healthz")
        print(f"  指标        GET  http://127.0.0.1:{HTTP_PORT}/metrics")
        print(f"  指标(JSON)  GET  http://127.0.0.1:{HTTP_PORT}/metrics/json")
        print()
        print("  日志：logs/inventory.log  logs/order.log  logs/gateway.log")
        print("  Windows 上看日志：")
        print("    powershell Get-Content logs\\order.log -Wait -Tail 30")
        print()
        print("  按 Ctrl+C 优雅停机")
        print("=" * 70)

        # --- 监控子进程 ---
        while True:
            time.sleep(0.5)
            for name, proc in procs:
                code = proc.poll()
                if code is not None:
                    print(f"\n✗ 服务 {name} 意外退出，exit={code}")
                    print(f"  请看 logs/{name}.log")
                    return 1

    except KeyboardInterrupt:
        print("\n收到 Ctrl+C")
        return 0
    finally:
        _stop_all(procs, log_files)

    return 0


if __name__ == "__main__":
    sys.exit(main())
