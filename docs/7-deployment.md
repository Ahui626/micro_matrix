# 7. 服务发现与部署编排

> 📍 第 7 份 / 共 7 份。前置 [1-architecture.md](1-architecture.md) · 想动手部署时读。

这份文档补齐了项目里最后两块拼图：**服务发现**（服务怎么互相找到）和
**部署编排**（这么多进程怎么跑起来、怎么扩容）。

读完你应该能回答：
- 服务发现的两种落地方式分别是什么？各用在什么场景？
- Consul 的注册 / 心跳 / 解析 / 注销四个动作分别干什么？
- 为什么「绑定地址」和「广播地址」会不一样？什么时候会不一样？
- 三个业务服务怎么打成镜像、怎么上 K8s？
- readiness 和 liveness 探针在这个项目里分别是怎么配的、为什么这么配？

---

## 第一部分：服务发现

### 1. 问题是什么

[1-architecture.md](1-architecture.md) 里讲过：服务地址不能永远写死。
这里把它落地。项目现在有**两套**发现方式，对应两种真实的生产方案：

| 方式 | 谁负责发现 | 用在 | 本项目 |
|---|---|---|---|
| **注册中心式** | 服务自己注册、客户端查注册中心 | 非 K8s、多机、跨集群 | 本地开发（Consul） |
| **DNS 式** | 平台提供（K8s Service / compose DNS） | K8s、容器编排 | compose 全栈 + K8s |

**关键认知**：这两者不是二选一的信仰，是「谁替你干活」的区别。
注册中心式要你写注册/心跳代码（本项目 [common/discovery.py](../common/discovery.py)）；
DNS 式平台直接给你一个稳定的名字（`order:50051`），你连注册代码都不用写。

> 所以你在 K8s 里几乎看不到 Consul —— 不是 Consul 不好，是 K8s 的
> Service + DNS 已经把「发现」这件事做掉了。Consul 的价值在 K8s 之外：
> 多机、混合云、虚拟机 + 容器的混合环境。

### 2. Consul 注册中心的四个动作

见 [common/discovery.py](../common/discovery.py)，逐行动作如下：

```
服务启动            register        PUT /v1/agent/service/register
                                     挂一个 TTL 健康检查（10s 没心跳就判死）

服务运行中          heartbeat       PUT /v1/agent/check/pass/service:<id>
                                    每隔 3s 报一次"我还活着"

调用方              resolve         GET /v1/health/service/<name>?passing=true
                                    取一个健康实例，本地缓存 5s

服务停机            deregister      PUT /v1/agent/service/deregister/<id>
                                    从注册表删掉自己
```

**几个必须能讲出来的点**：

- **注册 ≠ 健康检查**。注册只是"我存在"，健康检查才是"我还活着"。
  一个进程可能注册了但已经假死（死锁），所以两件事分开。
- **TTL 检查初始是 critical（不健康）**，要等第一次心跳才翻成 passing。
  所以 `run_registrar` 在注册后**立刻心跳一次**，否则服务要白等一个心跳间隔
  才可被发现 —— 这是实测踩出来的细节。
- **发现拿到的是一个列表**。本项目单实例只取第一个；多实例时「负载均衡」
  是拿到列表之后的第二步，和「发现」是两回事。

### 3. 客户端为什么要缓存解析结果

`resolve()` 的结果在客户端本地缓存 5 秒（`MM_DISCOVERY_CACHE_S`）。
如果每次调下游都去问注册中心，注册中心就成了新的单点和延迟放大器。
真实的 gRPC resolver 也是「拉一次 + 缓存 + TTL 过期再刷新」。

**缓存带来的一个可讲结论**：服务注销后，客户端最长还要用旧地址 `cache TTL` 秒。
所以「注销」不是立即生效的 —— 这是最终一致性，和 outbox 那套是一个道理。
那几秒里的失败由重试/熔断吸收（见 [1-architecture.md](1-architecture.md) 第 3.6 节）。

### 4. 注册中心挂了怎么办

`discovery.py` 的原则：**注册中心是基础设施，它挂不该拖垮业务**。

- `register`/`heartbeat`/`deregister` 失败 → 打 warning，继续跑。
- `resolve` 失败 → **回落静态地址**（`settings.py` 里写死的那个）。

所以即使本机没起 Consul，整套系统也照常工作，只是退化回「写死地址」的旧行为。
这本身是可讲的设计：**发现机制是一个可降级的增强，不是一个硬依赖**。

---

## 第二部分：绑定地址 vs 广播地址（★ 最容易踩的坑）

这是把服务搬进容器 / K8s 时**第一号坑**。同一个地址变量，两种含义：

| 含义 | 回答的问题 | 本机开发 | 容器 / K8s |
|---|---|---|---|
| **绑定地址** | 我监听哪个接口 | `127.0.0.1:50051` | `0.0.0.0:50051` |
| **广播/目标地址** | 别人怎么连到我 | `127.0.0.1:50051`（同一台机器，一样） | `order:50051`（Service 名，不一样了！） |

本机开发时两者恰好相同（都是 `127.0.0.1`），所以你在代码里一直用一个变量
没出过问题。一旦进容器：容器有自己的网络命名空间，`127.0.0.1` 只对它自己可见，
必须绑定 `0.0.0.0`；而别人连你时用的是 Service 名，不是 `0.0.0.0`。

所以 [common/settings.py](../common/settings.py) 里的 `order_addr` 是**一个变量两种用途**：
order 用它绑定，gateway 用它当目标。在本机没问题；在 K8s 里靠「不同 Pod 设不同的值」拆开：
- order Pod：`MM_ORDER_ADDR=0.0.0.0:50051`（绑定）
- gateway Pod：`MM_ORDER_ADDR=order:50051`（目标）

这也是为什么 [deploy/k8s/01-configmap.yaml](../deploy/k8s/01-configmap.yaml) 里
存目标地址、各 Deployment 里覆盖绑定地址。

> 想更严谨的话，生产项目会把这两个概念拆成两个变量（`bind_addr` / `advertise_addr`）。
> 本项目用一个变量，是为了让你**亲眼看到**这个坑是怎么来的。

---

## 第三部分：容器化

### Dockerfile 做了什么

[deploy/Dockerfile](../deploy/Dockerfile) 一个镜像装三个服务，运行时用 command 指定。

```
FROM python:3.14-slim          ← 和宿主机对齐，保证 cp314 wheel 能装上
COPY requirements.txt .        ← 先拷依赖清单
RUN pip install -r ...         ← 再装依赖（这层命中缓存，改代码不重装）
COPY . .                       ← 最后拷代码
RUN python scripts/gen_proto.py ← 重新生成 gRPC 代码
ENTRYPOINT ["python", "-u"]    ← -u 让日志不被缓冲
```

**两个能讲的点**：
- **COPY 顺序 = 缓存策略**。依赖清单不变时，`pip install` 这层命中缓存，
  改一行代码只重建最后一层。把「不常变的放前面」。
- **gen/ 在构建时重新生成**。`.dockerignore` 把 `gen/` 排除，进容器再生成，
  保证生成代码和镜像里的 grpcio-tools 版本严格一致（protobuf 版本匹配那套，
  见 [6-runbook.md](6-runbook.md) 第 5 条）。

### 全栈 compose：不用 K8s 也能本地验证

[deploy/docker-compose.yml](../deploy/docker-compose.yml) 把中间件 + 三个服务 + 一次性播种
全部容器化。它和根目录那个 compose 的区别：根目录只起中间件（业务跑宿主机，
方便下断点），这个是**全部进容器**，用来验证「容器化之后还能正常下单」。

> 注意它用 `MM_DISCOVERY=static` + compose 服务名（`order`/`inventory`），
> 因为 compose 的内建 DNS 已经能做发现 —— 和 K8s 的 Service 是同一套机制。
> 这也是为什么这里不再需要 Consul。

```powershell
cd deploy
docker compose up -d --build
# 下第一单
$body = @{ user_id="u1"; sku="SKU-1"; quantity=2; request_id="r-c" } | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:8000/orders -Method Post `
    -ContentType "application/json; charset=utf-8" -Body $body
```

### 本地镜像仓库：Docker Desktop K8s 拉不到你 build 的镜像

一个**关键坑**：Docker Desktop 内置的 K8s **不共享你 `docker build` 出来的镜像**。
它的节点用的是独立镜像存储 —— `docker images` 能看到、但 K8s 拉不到
（报 `ErrImageNeverPull`）。所以要把镜像先推到**本地私有仓库**，让 K8s 从仓库拉：

```powershell
# 1. 拉 registry（私有仓库）镜像 —— 走 daocloud 源（见 [8-ecosystem.md](8-ecosystem.md) 第二部分）
docker pull docker.m.daocloud.io/library/registry:2
docker tag docker.m.daocloud.io/library/registry:2 registry:2

# 2. 起本地仓库（监听 5000 端口）
docker run -d --name mm-registry -p 5000:5000 --restart unless-stopped registry:2

# 3. 把项目镜像推进去（用 v1 这种具体 tag，别用 latest）
docker tag micro-matrix:latest localhost:5000/micro-matrix:v1
docker push localhost:5000/micro-matrix:v1
```

之后 K8s 清单引用 `localhost:5000/micro-matrix:v1`，配 `imagePullPolicy: Always`
（本地仓库每次重启都拉最新，改代码重推后不用换 tag）。

---

## 第四部分：K8s

> 这一节假设你**没接触过 K8s**。先认识几个对象，再讲本项目怎么部署、怎么排查。

### 先认识 K8s 的 6 个对象（零基础）

K8s 的核心思想：**你声明「要什么」，它去把实际状态调整到那样**。
先认识这几个最常用的对象，后面每个命令、每个文件都是在操作它们：

| 对象 | 一句话 | 本项目里 |
|---|---|---|
| **Namespace** | 分组隔离，资源互不干扰 | `micro-matrix` 装下所有东西 |
| **Pod** | 最小调度单位，装一个或多个容器 | 每个服务实例 = 一个 Pod |
| **Deployment** | 声明「要 N 个副本 + 用哪个镜像」，负责滚动更新 | 三个服务各一个 |
| **Service** | 给一组 Pod 一个**稳定的 DNS 名** + 负载均衡 | `order` / `inventory` / `gateway` |
| **Job** | 跑一次就退出的任务 | 播种 `seed` |
| **ConfigMap** | 配置注入（环境变量） | 中间件地址、超时等 |

**它们的关系**：Deployment 创建 Pod → Service 把流量分给 Pod → Pod 从 ConfigMap 读配置。
你 `kubectl apply -f xxx.yaml` 就是在声明这些对象，K8s 的控制器不断对比「声明的」
和「实际的」，把实际往声明调。

### 清单总览

[deploy/k8s/](../deploy/k8s/) 下 6 份清单：

| 文件 | 对象 | 干什么 |
|---|---|---|
| `00-namespace.yaml` | Namespace | 建 `micro-matrix` 命名空间 |
| `01-configmap.yaml` | ConfigMap | 中间件地址（`host.docker.internal`）、`MM_DISCOVERY=static`、超时 |
| `02-inventory.yaml` | Deployment+Service | 绑定 `0.0.0.0:50052`，Service 名 `inventory` |
| `03-order.yaml` | Deployment+Service | 绑定 `0.0.0.0:50051`，Service 名 `order` |
| `04-gateway.yaml` | Deployment+Service(NodePort) | 对外 HTTP 入口 |
| `05-seed-job.yaml` | Job | 一次性播种库存 |

**中间件怎么连**：业务服务上 K8s，中间件（Postgres/Redis）仍在宿主机 Docker 里，
通过 `host.docker.internal` 访问（Docker Desktop 把宿主机端口转发进虚拟机）。
生产环境换成托管数据库 / 已有 Redis 的地址即可 —— 有状态的东西（数据库）
不该图便宜地塞进无状态的 Deployment。

### 部署步骤（真实跑通的流程）

前提：Docker Desktop 已开 Kubernetes（Settings → Kubernetes → Enable），
且已完成第三部分的「本地镜像仓库」搭建。

```powershell
# 1. 打镜像 + 推进本地仓库（改了代码就重跑这两步）
docker build -f deploy/Dockerfile -t micro-matrix:latest .
docker tag micro-matrix:latest localhost:5000/micro-matrix:v1
docker push localhost:5000/micro-matrix:v1

# 2. 一键部署
kubectl apply -f deploy/k8s/

# 3. 看状态（-w 持续 watch，Ctrl+C 退出）
kubectl -n micro-matrix get pods -w

# 4. 看 gateway 的对外端口，然后下单
kubectl -n micro-matrix get svc gateway          # 看 PORT(S) 里的 8000:3xxxx
$body = @{ user_id="u1"; sku="SKU-1"; quantity=2; request_id="r-k8s" } | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:3xxxx/orders -Method Post `
    -ContentType "application/json; charset=utf-8" -Body $body
```

> 嫌 NodePort 端口换来换去麻烦？用端口转发，固定用本机 8000：
> `kubectl -n micro-matrix port-forward svc/gateway 8000:8000`，然后直接打 `http://127.0.0.1:8000`。

### Docker Desktop K8s 特有的两个坑

这两个坑在「真集群」里大多不存在，是 Docker Desktop 单机环境的特性，但值得知道：

1. **不共享 `docker build` 的镜像**。节点用独立镜像存储，本地镜像它拉不到
   （报 `ErrImageNeverPull`）。解法就是第三部分的**本地私有仓库**。
2. **Service 名首次建连慢（约 2 秒）**。gvisor 网络 + kube-proxy 首次路由有开销，
   而项目默认给 inventory 的超时只有 1000ms，第一次调用必然超时。所以
   [01-configmap.yaml](../deploy/k8s/01-configmap.yaml) 放宽了
   `MM_GATEWAY_DEADLINE_MS=5000`、`MM_ORDER_RESERVE_TIMEOUT_MS=3000`。
   真集群网络更快，可以再收紧回去。

### 探针怎么配的、为什么

| 服务 | readiness | liveness |
|---|---|---|
| inventory / order | `exec` 跑 [grpc_probe.py](../scripts/grpc_probe.py) | 同左（gRPC 健康检查不查下游依赖，安全） |
| gateway | `httpGet /healthz`（真的查了 Redis） | `tcpSocket :8000`（只测进程活着） |

**两个能讲透的点**：

1. **gRPC 服务不能用 httpGet 探针**。httpGet 发的是 HTTP/1.x 明文，gRPC 走
   HTTP/2 + protobuf，协议不通。要么退化成 tcpSocket 只测端口，要么用真正的
   gRPC 健康检查 —— 项目里写了个 30 行的 [grpc_probe.py](../scripts/grpc_probe.py)，
   它连的是服务里已经注册的 `grpc-health-checking` 健康服务。

2. **liveness 不能查依赖，readiness 才可以**（详见 [4-concepts.md §10](4-concepts.md)）。
   所以 gateway 的 readiness 用 `/healthz`（查 Redis），liveness 却退化成 `tcpSocket`
   只测端口 —— 否则 Redis 一抖，全站 gateway Pod 被 liveness 重启，雪上加霜。

### 怎么排查问题

出问题时，按这个顺序查，基本能定位 90% 的问题：

```powershell
# 1. Pod 状态 —— Running 才正常；其它状态含义见下表
kubectl -n micro-matrix get pods

# 2. 看某个服务的日志（最重要的一步，能直接看到报错）
kubectl -n micro-matrix logs deployment/order --tail=50

# 3. 看某个 Pod 的详细状态和事件（为什么拉不到镜像、探针为什么失败）
kubectl -n micro-matrix describe pod <pod名>

# 4. 看整个命名空间发生了什么（启动失败、调度失败都在这）
kubectl -n micro-matrix get events --sort-by=.lastTimestamp
```

**常见状态 → 排查方向**：

| 状态 | 意思 | 先查什么 |
|---|---|---|
| `ContainerCreating` | 正在拉镜像/启动，正常 | 等一会，或 `describe` 看卡在拉镜像还是挂载 |
| `ImagePullBackOff` | 镜像拉不到 | 镜像名对不对？本地仓库起了吗？`imagePullPolicy`？ |
| `ErrImageNeverPull` | 镜像不在本地且策略是 `Never` | 走本地仓库，别用 `Never` |
| `CrashLoopBackOff` | 起来就崩、反复重启 | `logs` 看崩溃原因（连不上库？端口冲突？） |
| `Pending` | 没被调度到节点 | 资源不够？`describe` 看事件 |

### 滚动更新为什么不中断服务

三个 Deployment 默认就是滚动更新（RollingUpdate）。它的正确性依赖 readiness：
新 Pod 就绪之前不接流量，旧 Pod 等新 Pod 就绪后才被摘除。优雅停机
（[common/lifecycle.py](../common/lifecycle.py) 的 `server.stop(grace=3)`）保证
被摘除的旧 Pod 把在途请求做完再退。

> 一个还没做的点：滚动更新时 Snowflake 的 `worker_id` 要和实例一一对应，
> 否则两个副本可能生成重复 ID（见 [common/settings.py](../common/settings.py) 的注释）。
> 单副本演示没问题，扩副本前要解决（用 StatefulSet 的序号或 Downward API 的 Pod IP）。

### 在 K8s 里为什么不需要 Consul

回到第一部分的表：K8s 的 `Service` 给每个服务一个稳定的 DNS 名
（`inventory:50052`），kube-proxy 负责把流量转到健康的 Pod。**发现这件事已经被
平台做掉了**，所以 `MM_DISCOVERY=static`。Consul 那套注册/心跳代码，在 K8s 里
整个被跳过（`discovery.py` 里 `_enabled()` 返回 False）。

这个「在 K8s 里 Consul 失业了」的认知，面试时主动说出来很加分。

---

## 下一步

到这里，[1-architecture.md](1-architecture.md) 里那张「零件清单」的所有行
都不再是 ❌ 或 ⚠️ 了。回看它的小结表，或者去 [5-interview-index.md](5-interview-index.md)
扫一遍新增的「服务发现 / 部署编排」条目。

如果还想搞清楚 Docker / Docker Desktop / K8s / 镜像仓库之间的关系，
或者回顾一下「上 K8s 踩过的坑」，看 [8-ecosystem.md](8-ecosystem.md)。
