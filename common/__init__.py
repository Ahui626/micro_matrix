"""
共享基础库。

这里的每个模块都对应一类**分布式系统的共性问题**，而不是某个业务功能：

    settings.py   配置集中管理
    logging.py    跨进程日志关联（trace_id 注入）
    trace.py      trace_id 如何跨进程传递 ← 手写，不用 OTel
    ids.py        分布式唯一 ID（Snowflake）
    errors.py     业务失败 vs 系统失败，以及"该不该重试"
    db.py         Postgres 连接池 + 表结构（裸 SQL）
    redisx.py     Redis 客户端 + key 命名规约
    clients.py    gRPC channel、trace 透传、deadline 逐跳收紧
    discovery.py  Consul 服务注册与发现（注册/心跳/解析/注销）
    chaos.py      环境变量控制的故障注入（labs 的开关）
    retry.py      重试：什么时候该重试、退避与抖动
    breaker.py    熔断器
    ratelimit.py  Redis + Lua 令牌桶
    lock.py       分布式锁与 fencing token

[取舍] 微服务里共享一个 common/ 目录 = 隐式耦合，理论上每个服务应该独立演进。
       这个代价在教学项目里是划算的：读者能在一个地方看完所有横切关注点，
       而不是在三个服务里找三份几乎相同的实现。
       但要知道这笔账在生产环境里要重算——共享库会让服务的独立部署变困难。
"""
