"""
[问题]   K8s 的 readiness/liveness 探针怎么判断一个 gRPC 服务健不健康？
         HTTP 的 httpGet 探针发给 gRPC 端口会失败 —— 它发的是 HTTP/1.x 明文。
[本文件] 一个命令行探针：连到目标地址的 gRPC 健康检查服务，判断是否为 SERVING，
         是则退出 0，否则退出 1。供 K8s 的 exec 探针调用。
[面试]   - gRPC 服务为什么不能用 httpGet 探针？
           （httpGet 走 HTTP/1.x 明文，gRPC 走 HTTP/2 + protobuf 二进制，协议不通。
             要么退化成 tcpSocket 只测端口开没开，要么用真正的 gRPC 健康检查——
             比如 grpc-ecosystem 的 grpc-health-probe，或本文件这种 30 行实现。）
         - readiness 探针为什么要真的检查服务，而不是只测端口？
           （端口能连上只说明进程在监听，不代表它已就绪——可能还在连数据库、
             还在预热。readiness 失败会被摘除，liveness 失败会被重启，两者语义不同，
             见 docs/4-concepts.md §10。）
[取舍]   自己写而不是引入 grpc-health-probe 二进制：那个二进制要单独下载、
         塞进镜像还占体积；这里 30 行 + 项目里已有的 grpc-health-checking 就够。
         代价：没有 watch 模式、没有 TLS 选项，但教学场景用不到。

用法：
    python scripts/grpc_probe.py 127.0.0.1:50051
"""

from __future__ import annotations

import sys

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc


def main() -> int:
    if len(sys.argv) != 2:
        print("用法：python scripts/grpc_probe.py <host:port>", file=sys.stderr)
        return 2

    addr = sys.argv[1]
    try:
        with grpc.insecure_channel(addr) as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            # 空服务名表示"查整个 server 的总体状态"，对应服务里 set("", SERVING)。
            resp = stub.Check(health_pb2.HealthCheckRequest(), timeout=1)
            if resp.status == health_pb2.HealthCheckResponse.SERVING:
                return 0
            print(f"服务状态不是 SERVING：{resp.status}", file=sys.stderr)
            return 1
    except Exception as exc:  # noqa: BLE001 —— 连不上/超时/状态错都算不健康
        print(f"gRPC 健康检查失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
