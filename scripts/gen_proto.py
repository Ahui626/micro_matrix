"""
[问题]   把 proto/ 下的契约编译成 Python 代码。
[本文件] 调 grpc_tools.protoc，把 *_pb2.py / *_pb2_grpc.py 生成到 gen/，
         并给每个生成文件打上"自动生成，不要读"的头部。
[面试]   - 为什么生成代码必须和业务代码物理隔离？
         - 生成的 _pb2_grpc.py 里写的是 `import order_pb2`（绝对导入），
           这会导致什么问题？怎么解决？
         - .proto 改了之后，为什么服务端和客户端必须同时重新生成？
[取舍]   方案 A：把 gen/ 放进 sys.path，让绝对导入自然生效（本文件采用）。
           方案 B：生成到包内 + 用 sed 把绝对导入改成相对导入。
         选 A —— B 需要在生成后改文件，每次升级 protoc 都可能失效，更脆弱。
         代价是 gen/ 里的模块名是平铺的（order_pb2 而不是 gen.order_pb2），
         所以 __init__.py 里做了同名冲突提醒。

用法：
    python scripts/gen_proto.py
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

import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
GEN_DIR = ROOT / "gen"

# 放在每个生成文件最前面的横幅。
# 目的很直接：读代码的人必须能一眼分辨"这是我该读的"和"这是机器写的"。
# 分不清这两者，整个项目的可读性就崩了。
BANNER = '''"""自动生成 —— 不要读、不要改。

来源：proto/ 下的 .proto 文件
重新生成：python scripts/gen_proto.py

要改行为请改 .proto，改这里的代码会在下次生成时被覆盖。
"""
'''


def main() -> int:
    protos = sorted(PROTO_DIR.glob("*.proto"))
    if not protos:
        print(f"错误：{PROTO_DIR} 下没有 .proto 文件")
        return 1

    GEN_DIR.mkdir(exist_ok=True)

    # --proto_path 指定 import 根；--pyi_out 生成类型存根，让 IDE 有补全
    args = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_DIR}",
        f"--python_out={GEN_DIR}",
        f"--grpc_python_out={GEN_DIR}",
        f"--pyi_out={GEN_DIR}",
        *[str(p) for p in protos],
    ]
    print("正在生成：")
    print("  " + " ".join(args[3:]) + "\n")

    proc = subprocess.run(args)
    if proc.returncode != 0:
        print(f"\n生成失败，protoc 退出码 {proc.returncode}")
        return proc.returncode

    for path in sorted(GEN_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if text.startswith('"""自动生成'):
            continue  # 已经打过横幅，避免重复生成时叠加
        path.write_text(BANNER + text, encoding="utf-8")

    print(f"\n生成完毕 → {GEN_DIR}")
    for path in sorted(GEN_DIR.iterdir()):
        print(f"   {path.name}  ({path.stat().st_size} B)")
    print("\n注意：gen/ 里的模块是平铺的（order_pb2，不是 gen.order_pb2）。")
    print("入口脚本通过 bootstrap.py 把 gen/ 加进 sys.path，见 bootstrap.py。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
