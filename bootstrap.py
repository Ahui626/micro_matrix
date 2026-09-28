"""
[问题]   让 `import common.xxx`、`import order_pb2`、`import services.x` 在任何
         工作目录下都能工作。
[本文件] 把仓库根目录和 gen/ 插到 sys.path 最前面。所有入口脚本第一件事就是
         `import bootstrap`。
[面试]   - 为什么生成的 _pb2_grpc.py 里是 `import order_pb2` 而不是相对导入？
           （protoc 的 --python_out 生成的是绝对导入，且不支持改成相对导入。
             这是 protobuf 代码生成的一个长期痛点，必须靠 sys.path 解决）
         - Python 的 import 搜索顺序是怎样的？插到 sys.path[0] 有什么风险？
[取舍]   用 sys.path 注入而不是把 gen/ 做成一个正经的包。
         正经包需要改生成文件的导入语句，而生成文件每次都会重写 —— 不划算。
         代价：sys.path[0] 优先级最高，所以 gen/ 里不能有跟标准库重名的模块。
         当前 gen/ 里是 order_pb2 / inventory_pb2，安全。

         这本身是个可讲的权衡：微服务项目里共享代码目录 = 隐式耦合。
         教学项目收益大于成本，生产环境更倾向每个服务独立生成、独立打包。
"""

from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent

# 顺序重要：gen/ 放最前，仓库根放其后，保证两者都能被找到。
for _path in (str(ROOT / "gen"), str(ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

REPO_ROOT = ROOT


def force_utf8_stdout() -> None:
    """把 stdout/stderr 的编码强制成 UTF-8，并开启行缓冲。

    两件事都很关键：

    1. **UTF-8 编码**：Windows 控制台在输出被重定向或管道时
       （`python x.py | tee log`），Python 会退回系统 ANSI 代码页 ——
       中文系统上是 GBK。之后任何中文输出都变成乱码，而日志里恰恰全是中文。

    2. **行缓冲**：Python 的 stdout 在被重定向到文件/管道时是**块缓冲**的
       （默认 8KB 攒满才刷）。症状是：`python scripts/run_all.py > out.txt`
       之后 `cat out.txt` 是空的，程序看起来"卡住了"，
       实际上它跑得好好的，只是输出还在缓冲区里。
       对交互式脚本来说这是致命的 —— 用户会以为程序挂了。
       `line_buffering=True` 让每次换行就刷一次。

    为什么 catch 多个异常：reconfigure 只存在于 TextIOWrapper 上
    （某些环境下 sys.stdout 已被替换成别的对象），且某些流不允许重新配置。
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
        except (AttributeError, OSError, ValueError):
            pass


# 在 import 时就生效 —— 这违反"import 不应有副作用"的常规，但这里是刻意的：
# 每个入口脚本都 import bootstrap，放在这里就不用重复 6 遍，也不会漏。
force_utf8_stdout()
