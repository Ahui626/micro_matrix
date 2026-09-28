# 6. 运行手册与 Windows 踩坑记录

> 📍 **参考手册**（第 6 份 / 共 6 份）。不需要顺序读 —— 出问题时来查。

这个项目跑在 **Windows 11 + Python 3.14.6 + 标准 GIL 构建**上。
下面每一条都是实际踩过的坑，不是从文档抄来的。

---

## 一、日常操作

> 命令都是 **PowerShell** 语法。Git Bash 的对应写法见
> [README 的「命令约定」](../README.md#命令约定)。

```powershell
# 启动中间件

docker compose up -d

# 看中间件状态

docker compose ps

# 播种库存（首次运行必须做一次）

python scripts\seed.py

# 启动全部服务（会轮询就绪，不是盲等）

python scripts\run_all.py

# Ctrl+C 优雅停机 —— 三个服务都会走完停机流程

```

单跑某个服务（调试时有用）：

```powershell
python services\inventory\main.py
python services\order\main.py
python services\gateway\main.py
```

看日志：

```powershell
Get-Content logs\order.log -Wait -Tail 30
```

> 日志文件带 UTF-8 BOM，所以 PowerShell 5.1 也能正确识别中文。
> 如果看到 `order 鏈嶅姟鍚姩涓` 这种乱码，说明 BOM 丢了 ——
> 见下面第 10 条。临时办法是加 `-Encoding utf8`。

按 trace_id 串全链路：

```powershell
Select-String -Path logs\*.log -Pattern "d4ce5835" -SimpleMatch |
    ForEach-Object { $_.Filename + "  " + $_.Line }
```

---

## 二、Windows + Python 3.14 的坑

按「会静默搞坏你」的概率排序。

### 1. ★ 绝不要调用 `asyncio.set_event_loop_policy()`

网上 3.8 时代的老教程会让你设 `WindowsSelectorEventLoopPolicy`。
**在 Python 3.14 上这是错且有害的**：

- 这个 API 已经被废弃
- `SelectorEventLoop` 在 Windows 上**不支持 `create_subprocess_*`**（直接抛 NotImplementedError）
- `grpc.aio` 在默认的 **ProactorEventLoop** 上最稳

而且 `asyncio.get_event_loop()` 在 3.14 里**直接抛 RuntimeError**（已实测）：

```
RuntimeError: There is no current event loop in thread 'MainThread'
```

**结论**：全部用裸 `asyncio.run()`。整个项目里没有一处碰事件循环策略。

### 2. ★ `loop.add_signal_handler()` 在 Windows 上抛 NotImplementedError

任何从 Linux 博客抄来的优雅停机代码都会在**启动时**崩。
正确做法见 [common/lifecycle.py](../common/lifecycle.py)：
用 `signal.signal()` + 一个 `asyncio.Event`。

另外 `SIGTERM` 在 Windows 上基本收不到。本项目的做法是额外注册
`signal.SIGBREAK`（Windows 专有），这样 `run_all.py` 才能用
`os.kill(pid, signal.CTRL_BREAK_EVENT)` 通知子进程优雅退出。

### 3. ★ 不要用 `multiprocessing` 起服务

Windows 没有 `fork`，`multiprocessing` 用 **spawn**：

- 每个子进程会**重新 import `__main__`**（没有 `if __name__ == "__main__"` 保护就无限递归）
- 不共享父进程的任何全局状态（连接池、事件循环、Redis 客户端全不共享）

用 `subprocess.Popen`，见 [scripts/run_all.py](../scripts/run_all.py)。

### 4. ★ 端口：Windows 保留大片端口段

```
$ netsh interface ipv4 show excludedportrange protocol=tcp

开始端口    结束端口
     50000       50059     *
     50060       50159          ← 本机实测：50062 在这里面，绑不上
     50175       50274
     ...
```

Hyper-V / WSL / Docker 会预留这些段。**写死端口会随机绑不上**，
而且报错是 grpc 底层的 `Failed to bind to address ...`，看不出真实原因。

本项目的做法：
- 服务用 50051 / 50052 / 8000（已验证可用）
- `labs/lab5` 需要额外端口，所以**让 OS 分配**（bind 到端口 0）

遇到绑定失败时，服务现在会打印人话版的排查提示（见
[inventory/main.py](../services/inventory/main.py) 的绑端口分支）。

### 5. pin 住 protobuf 运行时版本 == gencode 版本

`grpcio` / `grpcio-tools` / `protobuf` 必须一起升级。
否则会出现经典的：

```
Detected incompatible Protobuf Gencode/Runtime version
```

这是自伤，不是环境问题。[requirements.txt](../requirements.txt) 里已经锁死。

### 6. `uvloop` 在 Windows 上不存在

没有 wheel，`pip install uvloop` 会尝试源码编译然后失败。
本项目不用它，也不要从 Linux 配置里抄。

### 7. 确认没跑在 free-threaded 构建上

如果不小心装的是 free-threaded 构建（ABI 是 `cp314t`），
`grpcio` 和 `asyncpg` **没有对应的 wheel**，pip 会退化成源码编译，
而这台机器没有 C 编译器 —— 安装会以一堆看不懂的报错失败。

```powershell
python -c "import sys; print(sys._is_gil_enabled())"   # 必须是 True
```

`scripts/smoke.py` 里已经带了这项检查。

### 8. stdout 在重定向时是**块缓冲**的

```powershell
python scripts\run_all.py > out.txt   # 程序明明在跑，out.txt 却是空的
```

Python 的 stdout 被重定向到文件/管道时是块缓冲（攒 8KB 才刷）。
对一个交互式脚本来说是致命的 —— 用户会以为程序挂了。

而且 Windows 控制台在重定向时会退回 ANSI 代码页（中文系统上是 GBK），
中文直接变乱码。

两者都在 [bootstrap.py](../bootstrap.py) 里统一修掉：
`reconfigure(encoding="utf-8", line_buffering=True)`。

### 9. 中文 print 里嵌套半角双引号 = 语法错误

写 `print("不是"并发"，是..."` 会直接 SyntaxError。
本项目统一用全角引号 `「」`。

（这个坑在写这个项目的过程中踩了三次。）

### 10. ★ 日志文件必须带 UTF-8 BOM，否则 PowerShell 里是乱码

症状 —— 用 `Get-Content logs\order.log` 看到的是：

```
order 鏈嶅姟鍚姩涓    ← 其实是 "order 服务启动中"
```

**这不是日志写错了。** 日志文件是完全合法的 UTF-8，
问题在**读取方**：Windows PowerShell 5.1 的 `Get-Content`
在没有 BOM 时，会按**系统 ANSI 代码页**（中文系统上是 GBK）去解码，
于是 UTF-8 字节被当成 GBK 解释 → 乱码。

**解法**：在日志文件开头写一个 BOM（`EF BB BF`）。
PowerShell 5.1 和 7 看到 BOM 都会自动按 UTF-8 解码。

`scripts/run_all.py` 里的 `_spawn()` 就是这么做的（以二进制打开 + 手写 BOM）。
注意 Python 的 `open(encoding="utf-8")` **不会**写 BOM —— 这是刻意的，
BOM 在 Unix 世界不受欢迎；而子进程是直接往文件描述符写字节的，
所以只能由父进程在文件开头显式写一次。

**如果 BOM 丢了**（比如日志文件被别的工具重写），临时办法：

```powershell
Get-Content logs\order.log -Encoding utf8 -Wait -Tail 30
```

**为什么不让用户每次都加 `-Encoding utf8`**：那要靠人记住，而人会忘。
让文件自己带上身份标识更可靠 —— 这也是"把正确做成默认"的一个例子。

### 11. ★ 同一个坑：没有 BOM 的 `.ps1` 脚本会被 PowerShell 当 ANSI 读

**完全相同的根因，只是换了个文件类型。**

PowerShell 5.1 读取 `.ps1` 脚本文件时，如果文件没有 BOM，
也按系统 ANSI 代码页解码。所以一个**保存为 UTF-8 无 BOM、里面含中文的 .ps1**，
运行时中文会变成乱码 —— 如果是注释还好，如果出现在字符串或变量名里，
就会变成**语法错误**，而且报错信息本身也是乱码，根本看不懂：

```
表达式或语句中包含意外的标记"["。
字符串缺少终止符: "。
```

（写这份文档的过程中真的踩了：一个测试脚本因为中文注释被解码错乱而解析失败。）

**结论**：如果你要写 `.ps1` 脚本，**要么让编辑器保存为「UTF-8 with BOM」，
要么干脆只用 ASCII 字符**。VS Code 右下角的编码指示器可以切换。

**和日志那条的共同教训**：Windows 上任何要被别的工具读的文本文件，
带上 BOM 能省掉一整类"看起来是内容错了、其实是编码错了"的问题。

---

## 三、出问题怎么查

### 服务起不来

```powershell
# 1. 看日志末尾

Get-Content logs\order.log -Tail 30        # 把 order 换成出问题的服务

# 2. 端口是否被占

netstat -ano | findstr :50051

# 3. 是不是落在保留端口段里

netsh interface ipv4 show excludedportrange protocol=tcp

# 4. 中间件还在吗

docker compose ps
```

### 消息不被消费了

症状：订单创建成功，但积分一直不涨。

排查顺序：

1. `logs/order.log` 里有没有 relay 投递记录
2. 消费组还在吗：
   ```bash
   docker exec -it mm-redis redis-cli XPENDING mm:stream:order-events mm-order-consumers
   ```
3. 有没有进死信队列：
   ```bash
   docker exec -it mm-redis redis-cli XLEN mm:stream:order-events-dlq
   ```
4. **stream 被删过吗？**

   如果手动 `DEL` 了 stream，挂在它上面的**消费组会一起消失**，
   consumer 会一直报 `NOGROUP`。本项目在
   [consumer.py](../services/order/consumer.py) 里做了自愈（检测到 NOGROUP 就重建组），
   但如果哪天这段自愈被改坏了，症状就是"重启后消息不再被消费"。

### 查数据 / 验证一致性

> **psql 里每条 SQL 必须以分号结尾。** 如果你看到提示符从 `micro_matrix=#`
> 变成 `micro_matrix-#`，那个 `-` 表示 psql 认为上一条语句还没结束，
> 正在等你补完 —— 后面敲的所有内容都会被接成同一条语句。
> 清空缓冲区用 `\r`（**别**直接敲 `;`，那会把攒的垃圾一次性发出去）。
> `\` 开头的元命令（如 `\d orders`、`\q`）不需要分号。

懒得进交互模式的话，用 `-c` 一行式，不存在缓冲区状态问题：

```powershell
docker exec -it mm-postgres psql -U micro -d micro_matrix -c "select count(*) from orders;"
```

**一键交叉验证**（这两个等式成立就说明系统是一致的）：

```powershell
docker exec -it mm-postgres psql -U micro -d micro_matrix -c "
select
  (select coalesce(sum(quantity),0) from orders where status='CONFIRMED') * 10 as points_expected,
  (select coalesce(sum(total),0) from points)                                as points_actual,
  (select coalesce(sum(quantity),0) from orders where status='CONFIRMED')     as units_sold,
  (select 100000 - stock from inventory where sku='SKU-1')                    as units_deducted;
"
```

- `points_expected = points_actual` → 事件消费没重复也没漏
  （这是 lab2 那套幂等机制在起作用）
- `units_sold = units_deducted` → 没超卖也没少扣
  （这是 [inventory/repo.py](../services/inventory/repo.py) 那条原子 UPDATE 在起作用）

这两条对不上时，再看下面的分项排查。

### 数据不一致

```powershell
# 订单状态分布

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "SELECT status, count(*) FROM orders GROUP BY status;"

# 未发送的 outbox（长期 > 0 说明 relay 有问题）

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "SELECT count(*) FROM outbox WHERE published = FALSE;"

# 库存与预留记录是否对得上

docker exec -it mm-postgres psql -U micro -d micro_matrix -c "SELECT sku, stock FROM inventory;"
docker exec -it mm-postgres psql -U micro -d micro_matrix -c "SELECT sku, status, sum(quantity) FROM reservations GROUP BY 1,2;"
```

> 这里全部改成**单行**，不用多行续行符。原因是 bash 用 `\` 续行、
> PowerShell 用反引号 `` ` ``，写成多行就只能照顾一种 shell。
> 一行能放下的命令就别拆行 —— 少一个能出错的维度。

### 确认故障注入是不是开着的

很多"这怎么坏了"其实是自己开的注入没关：

```powershell
Select-String -Path logs\*.log -Pattern "故障注入" | Select-Object -Last 5
```

每个服务启动时都会打印 `chaos.describe()`，
激活的注入点会明确列出来。**先排除这一条再 debug。**

⚠️ 特别提醒 PowerShell 用户：`$env:X="1"` 设的是**当前窗口**的环境变量，
不是只对一条命令生效。所以**重启服务时它还在** ——
这正是"我明明没开注入，怎么行为不对"的常见原因。取消：

```powershell
Remove-Item Env:\MM_CHAOS_DISABLE_ATOMIC_DECR
```

---

## 四、重置环境

```powershell
# 清库 + 清 Redis（订单、outbox、消息、锁、限流桶），然后重新播种

python scripts\seed.py --reset
```

只想清掉 Redis 里的消息和锁、保留数据库：

```powershell
docker exec -it mm-redis redis-cli FLUSHDB
```

彻底重来（连数据卷一起删）：

```powershell
docker compose down -v; docker compose up -d
```

---

## 五、已知的、故意保留的行为

这些看起来像 bug，其实是设计决策：

| 现象 | 为什么 |
|---|---|
| 库存不足返回 **201 + `status=CANCELLED`**，不是 409 | 订单记录确实被创建了，返回异常等于说"什么都没发生"。Saga 的失败表达为**状态**而非异常。见 [gateway/http_api.py](../services/gateway/http_api.py) |
| 一个成功订单会产生 **2 条事件**（OrderCreated + OrderConfirmed） | 分别记录"事实发生了"和"结果确定了"。消费端只订阅终态事件 |
| 取消的订单也会发 OrderCreated | 同上。给它加积分才是错的（这个 bug 在开发时真实出现过，被 lab 的断言抓出来了） |
| relay 可能重复投递同一条事件 | at-least-once 的固有属性，不是 bug。消费端幂等负责吸收 |
| 分布式锁在服务代码里完全没用到 | 刻意如此。正确性来自原子 SQL 和唯一约束，不需要锁。锁只在 lab3 里演示 |

---

## 六、服务发现与部署（Consul / 容器 / K8s）

### 看注册中心里有什么

```powershell
# 服务启动后，Consul 里应看到 order 和 inventory（gateway 不注册，它是纯调用方）
docker exec mm-consul consul catalog services

# 看某个服务的**健康**实例（只列 passing 的）
docker exec mm-consul consul health service inventory --passing

# 直接用 HTTP API 看（和代码里 resolve() 打的是同一个接口）
curl http://127.0.0.1:8500/v1/health/service/inventory?passing=true
```

> `consul catalog services` 列出的是**注册了的**服务，不含健康状态；
> `health service --passing` 才是"健康且可被发现"的。两者的差就是 TTL 检查在起作用：
> 服务停掉后，前一个命令里它还会赖一段时间，后一个里立刻消失。

### 没起 Consul 会怎样

`MM_DISCOVERY` 默认是 `consul`，但解析失败会回落静态地址，所以**不起 Consul
系统也照常工作**（只是日志里会有 "从 Consul 解析失败" 的 warning）。
想彻底关掉注册中心（比如切到 K8s 的 DNS 式发现）：

```powershell
$env:MM_DISCOVERY = "static"
```

### 全栈容器化（不上 K8s 也能验证容器化没坏）

```powershell
cd deploy
docker compose up -d --build
```

把中间件 + 三个服务 + 一次性播种全部用容器跑起来，用 compose 的 DNS 做发现
（`MM_DISCOVERY=static`）。和根目录的 compose 分工不同，见
[7-deployment.md](7-deployment.md)。

### 上 K8s

见 [7-deployment.md](7-deployment.md) 第四部分。核心三步：改镜像名和中间件地址 →
`kubectl apply -f deploy/k8s/` → `kubectl -n micro-matrix get pods,svc,jobs`。

---

## 七、Docker Desktop K8s 与镜像源（踩坑速查）

### 本地私有仓库（Docker Desktop K8s 必须）

Docker Desktop 的 K8s **拉不到你 `docker build` 的镜像**，必须走本地仓库：

```powershell
docker pull docker.m.daocloud.io/library/registry:2
docker tag docker.m.daocloud.io/library/registry:2 registry:2
docker run -d --name mm-registry -p 5000:5000 --restart unless-stopped registry:2
docker tag micro-matrix:latest localhost:5000/micro-matrix:v1
docker push localhost:5000/micro-matrix:v1
```

改代码后重新发布（镜像 tag 不变，靠 `imagePullPolicy: Always` 拉最新）：

```powershell
docker build -f deploy/Dockerfile -t micro-matrix:latest .
docker push localhost:5000/micro-matrix:v1
kubectl -n micro-matrix rollout restart deployment/inventory deployment/order deployment/gateway
```

### 镜像源怎么配才生效

- **别手改 `daemon.json` 文件** —— Docker Desktop 重启会覆盖它。走
  Settings → Docker Engine（JSON 编辑框）→ Apply & Restart。
- 目前实测**唯一能拉 blob 的免费源是 `docker.m.daocloud.io`**（阿里云免费加速器
  已停服、USTC 已停、1ms 只能拉清单拉不动层）。
- 验证：`docker pull busybox` **能成功**才算配好（光 `docker info` 显示不算数）。

### Pod 起不来，按顺序查

```powershell
kubectl -n micro-matrix get pods                        # 看状态
kubectl -n micro-matrix logs deployment/order --tail=50 # 看日志
kubectl -n micro-matrix describe pod <名>                # 看事件
```

| 状态 | 原因 | 解法 |
|---|---|---|
| `ErrImageNeverPull` | 镜像不在 K8s 节点（它不共享 docker build 的镜像） | 走本地私有仓库 |
| `ImagePullBackOff` | 拉不到镜像 | 镜像名 / 仓库地址 / 网络 |
| `CrashLoopBackOff` | 起来就崩 | `logs` 看崩溃原因 |
