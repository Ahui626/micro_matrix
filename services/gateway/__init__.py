"""
网关服务。系统的唯一边界，也是唯一说 HTTP 的地方。

    http_api.py  中间件（trace / metrics）、限流、gRPC→HTTP 错误映射
    main.py      uvicorn 编程式启动

内部服务之间一律用 gRPC —— HTTP 只存在于边缘。
"""
