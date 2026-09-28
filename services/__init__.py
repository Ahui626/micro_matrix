"""
三个服务。

    gateway/    FastAPI，对外 HTTP。系统的唯一边界，负责限流和 trace 起点。
    order/      gRPC，下单事务、幂等、Outbox、Saga 编排、消息中继与消费。
    inventory/  gRPC，库存预留与释放。

每个服务内部固定四层，依赖方向单向：

    main.py      进程入口：起服务、装拦截器、优雅停机
    grpc_api.py  传输层：解析 → 调 service → 映射错误（薄，无业务判断）
    service.py   业务层：编排、事务边界、幂等（不 import grpc）
    repo.py      数据层：SQL（不管事务边界，由 service 决定）

为什么要坚持这个分层：service 不 import grpc，意味着换掉传输协议
（gRPC → HTTP → 消息消费者）时业务逻辑一行都不用改。
这也是"业务逻辑可以被单元测试"的前提。
"""
