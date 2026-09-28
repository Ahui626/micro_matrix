"""
反面案例实验。

每个 lab 都是**独立可运行**的脚本，而且**必须断言出错误行为** ——
日志里偶尔看到一次 bug 不算教学，一条失败的断言才是"原来如此"。

所有实验都通过 common/chaos.py 的故障注入开关，或通过显式的 barrier
来**确定性**地复现问题，绝不依赖"跑很多次总有一次会撞上"。
不可复现的失败既不能断言，也不能用来验证修复。

    lab1_oversell.py        并发扣库存超卖 —— await 就是竞态窗口
    lab2_dup_consume.py     重复消费与 ack 时机 —— at-least-once 的代价
    lab3_lock_fencing.py    分布式锁失效 —— 锁不是正确性保证
    lab4_outbox_twinwrite.py 双写不一致 —— 为什么需要本地消息表
    lab5_retry_storm.py     重试风暴 —— 没有抖动和预算的重试会放大故障

运行前提：
    docker compose up -d
    python scripts/run_all.py      （另开一个终端）
"""
