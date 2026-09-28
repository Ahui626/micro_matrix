"""
订单服务。这个服务承担了最多知识点：

    service.py   Saga 编排（T1 建单 → T2 预留库存 → T3 支付 + 逆向补偿）
    repo.py      Outbox 与订单在同一事务、消费幂等与副作用在同一事务
    relay.py     Outbox → Redis Stream（at-least-once 的来源）
    consumer.py  消费组 + 幂等 + 重试计数 + 死信队列 + XAUTOCLAIM
    grpc_api.py  deadline 预算的计算与下发
"""
