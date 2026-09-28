"""
[问题]   三个服务都需要"收到 Ctrl+C 后优雅停机"。要写三遍吗？
[本文件] 只抽出信号处理那一小段：装 SIGINT/SIGTERM → 返回一个 asyncio.Event。
         真正的停机动作（server.stop、关连接池）留在各自的 main.py 里。
[面试]   - Windows 上为什么不能用 `loop.add_signal_handler()`？
           （Windows 的事件循环不支持，直接抛 NotImplementedError。
             任何从 Linux 博客抄来的停机代码都会在启动时崩。）
         - 为什么信号处理器里不直接做异步操作？
           （信号处理器运行在受限上下文里 —— 它可能在任意字节码之间被触发，
             在它里面 await 是未定义行为。安全做法：只置位，动作交给主协程。）
         - 优雅停机的正确顺序是什么？
           （停止接受新请求 → 等在途请求做完 → 关闭下游连接和资源 → 退出。
             顺序错了的后果：先关连接池、后停监听，
             在途请求会在中途拿到"连接池已关闭"而失败。）
[取舍]   只抽信号这一段，不抽"完整停机流程"。
         理由：停机**顺序**是每个服务不同、且最值得读者看清的部分。
         抽掉了就藏起了"先停监听还是先关连接池"这个关键决策。
"""

from __future__ import annotations

import asyncio
import logging
import signal

log = logging.getLogger(__name__)


def install_signal_handlers(service_name: str) -> asyncio.Event:
    """装上信号处理器，返回一个"该停机了"的事件。

    调用方拿到事件后自己 await 它，然后执行自己的停机流程。
    """
    stop_event = asyncio.Event()

    def _on_signal(signum, _frame):
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = str(signum)
        # 只置位，不做任何其它事。
        log.info("[%s] 收到 %s，准备优雅停机", service_name, name)
        stop_event.set()

    # 要处理哪些信号：
    #   SIGINT       —— Ctrl+C，跨平台都有
    #   SIGTERM      —— Linux/macOS 的"请退出"，K8s 删除 Pod 时发的就是它
    #                   （Windows 上这个信号基本收不到，必须靠下面那个）
    #   SIGBREAK     —— ★ Windows 专有。Ctrl+Break 会触发它，
    #                   而且 `os.kill(pid, signal.CTRL_BREAK_EVENT)`
    #                   也是发这个信号 —— 这是 Windows 上
    #                   "优雅地通知另一个进程退出"的唯一可行方式。
    #                   scripts/run_all.py 就靠它来优雅停机。
    #                   不在 Windows 上的话 getattr 拿不到，自动跳过。
    signals_to_handle = [signal.SIGINT, signal.SIGTERM]
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        signals_to_handle.append(sigbreak)

    for sig in signals_to_handle:
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError, AttributeError):
            # 非主线程、或平台不支持该信号时忽略。
            # 不要因为装不上某个信号就让整个服务起不来 —— 那才是更大的问题。
            pass

    return stop_event
