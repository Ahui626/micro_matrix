# 8. Docker / Docker Desktop / K8s / 镜像仓库 的关系（附本次踩坑复盘）

> 📍 第 8 份。**给还没分清这几个东西的人**。如果你在「把项目搬上 K8s」时
> 被一堆报错绕晕了，先读这一份建立心智模型，再回头看 [7-deployment.md](7-deployment.md)。

这份文档讲两件事：
1. **概念**：Docker、Docker Desktop、K8s、镜像仓库分别是什么、什么关系。
2. **复盘**：本项目从「本地能跑」到「K8s 跑通」踩过的每个坑，作为真实案例。

---

# 第一部分：四个东西各是什么

## 1. Docker：把程序打包成「集装箱」

**Docker 是一个引擎**，干一件事：把你的程序 + 依赖 + 运行环境打成一个**镜像**，
然后在一个隔离的环境（**容器**）里跑起来。

```
代码 + 依赖 + 系统库  ──打包──▶  镜像(image)  ──运行──▶  容器(container)
```

- **镜像**是「只读的模板」（像装机用的 ISO），里面装着 Python、你的代码、依赖。
- **容器**是「镜像跑起来后的实例」（像用 ISO 装出来的那台机器）。
- 一个镜像可以起很多个容器，互不干扰。

**关键概念**：镜像在哪、能不能拿到，是本文后半段所有坑的根源。

## 2. Docker Desktop：Windows/Mac 上跑 Docker 的工具

Docker 是 Linux 世界的技术。你的 Windows 机器要跑 Docker，需要一个**中间层**。
**Docker Desktop** 就是那个中间层 —— 它在后台起一个轻量 Linux 虚拟机（WSL2），
把 Linux 的 Docker 引擎装在里面，再让你在 Windows 命令行里用 `docker` 命令操作它。

> 所以「Docker 引擎」其实**跑在虚拟机里**，不在你的 Windows 里。这一点很重要：
> 后面很多「连不上」「看不到」的问题，都是因为你的程序、K8s、虚拟机、宿主机
> **各自在不同的网络/存储空间里**。

## 3. K8s（Kubernetes）：管理一大堆容器的「调度系统」

一个容器好管，**一百个容器**呢？谁该跑在哪台机器、挂了怎么办、怎么扩缩容、
怎么让它们互相找到对方 —— 这些问题 Docker 不解决。

**K8s 就是干这个的**：你告诉它「我要 3 个 order 实例 + 用这个镜像」，
它负责把 Pod 调度到节点、挂了自动重启、按需扩容。

一句话分清：**Docker 管「单个容器怎么跑」，K8s 管「一大堆容器怎么协作」。**

## 4. 镜像仓库（Registry）：镜像的「网盘」

镜像需要**分发**：你本机 build 出来的镜像，怎么让另一台机器 / K8s 节点也能用？

**镜像仓库就是存镜像、供人拉取的服务**。

| 仓库 | 说明 | 本项目 |
|---|---|---|
| **Docker Hub** | 官方公共仓库，存着全世界的公共镜像 | 拉 `python:3.14-slim`、`redis`、`postgres` |
| **镜像加速源（mirror）** | 代理 Docker Hub 的中转站，国内网络用的 | `docker.m.daocloud.io` |
| **私有仓库（registry）** | 你自己跑一个，存自己的镜像 | `localhost:5000` 存 `micro-matrix` |

**关键认知**：K8s 节点拉镜像时，只认**仓库地址**。它不会去你本机 `docker images`
里翻 —— 你本机 build 的镜像，除非推到一个 K8s 能访问的仓库，否则 K8s 拿不到。

---

# 第二部分：一张图看清它们的关系

```
你的 Windows 机器
│
├─ Docker Desktop（一个跑在 WSL2 虚拟机里的 Linux 环境）
│    │
│    ├─ Docker 引擎  ← 你敲 docker build / run 操作的就是它
│    │      │  产出镜像 → 存进 Docker 引擎自己的镜像存储
│    │      │
│    └─ K8s 集群（Docker Desktop 内置，可选开启）
│           │  节点用「独立镜像存储」← ★ 和上面的不共享！
│           │
│           └─ Pod（order / inventory / gateway）
│
├─ 中间件容器（mm-postgres / mm-redis / mm-consul）── 也在 Docker 引擎里
│      │
│      └─ 通过 host.docker.internal 被 Pod 访问（端口转发回宿主机）
│
└─ 本地私有仓库（mm-registry，端口 5000）── 一个普通的容器
       │
       └─ 你 push 镜像进去，K8s 从这里 pull
```

**三个「不共享」是万恶之源**（后面每个坑都能归结到它们）：

1. **Docker 引擎 ≠ K8s 节点**：镜像存储不共享。你 `docker build` 的镜像，K8s 看不到。
2. **宿主机 ≠ 虚拟机 ≠ Pod**：网络不共享。`localhost` 在容器里指的是容器自己。
3. **公共仓库 ≠ 你本机**：镜像不共享。K8s 拉镜像只能从仓库拉。

---

# 第三部分：本次修复复盘（真实案例）

目标：把项目从「宿主机本地跑」搬到「Docker Desktop 的 K8s 上跑」。
下面按踩坑顺序复盘，每个坑都标了**根因**和**解法**。

## 坑 1：Docker Desktop 启用 K8s 失败（"pulling images"）

- **现象**：设置里点 Enable Kubernetes，一直卡在 "pulling images" 然后 Failed to start。
- **根因**：启用 K8s 要拉一堆镜像（kube-apiserver、etcd 等），而 Docker Hub 在国内
  **连不通**（`registry-1.docker.io` 超时）。
- **解法**：配置**镜像加速源**。但下面立刻踩了坑 2。

## 坑 2：镜像加速源配了还是没用

- **现象**：`daemon.json` 里配了 `registry-mirrors`，但 `docker pull` 还是直连
  Docker Hub 超时。
- **根因**：两个问题叠在一起 ——
  1. 原配置里的源是**死的**：USTC 镜像 2024 年停服了，另一个还是阿里云控制台的
     **网页地址**（不是镜像地址）。
  2. 2025 年后**阿里云免费「镜像加速器」也停服了**（你那个 `xxx.mirror.aliyuncs.com`
     直接回 403 停服公告）。
- **解法**：实测出**唯一还能拉 blob 的免费源是 `docker.m.daocloud.io`**
  （其它源要么只能拉清单、拉不动文件层，要么被挤爆）。
- **教训**：`docker info | grep -iA5 "Registry Mirrors"` 能看配置**实际生效没有**；
  判断一个源能不能用，得看它能不能**拉 blob**（`docker pull` 一个真实镜像），
  光 `curl` 得到 401 只说明「服务活着」，不代表能拉。

## 坑 3：K8s 集群起来了，但 Pod 卡在 ImagePullBackOff

- **现象**：K8s 终于能跑，Pod 却一直 `ImagePullBackOff` / `ErrImageNeverPull`。
- **根因**：**Docker Desktop 的 K8s 不共享你 `docker build` 的镜像**。即使
  `docker images` 看得到 `micro-matrix:latest`，K8s 节点也拿不到，它只会去
  Docker Hub 拉 `micro-matrix:latest`（不存在 + 连不上）。
- **解法**：**本地私有仓库**。拉 `registry:2`（走 daocloud 源）→ 起仓库 →
  把 `micro-matrix` 推进去 → K8s 清单改指向 `localhost:5000/micro-matrix:v1`。
- **教训**：这是「镜像分发」的核心 —— **本机镜像 ≠ K8s 能用的镜像**，
  中间必须经过仓库。

## 坑 4：Pod 起来了但 order 秒退（后台任务"意外退出"）

- **现象**：order 服务一起就退，日志里一行
  `后台任务 registrar 意外正常退出（循环不该结束）`。
- **根因**：这是我写的一个 bug —— `MM_DISCOVERY=static` 时，注册中心的后台任务
  `run_registrar` **立刻返回**（不用注册），而 order 用「任何一个后台任务退出就
  整个进程停机」的逻辑，把这个"正常返回"当成了"异常退出"。
- **解法**：改成 static 模式下也**等停机信号再返回**，保持后台任务的语义。
- **教训**：后台任务的「退出」有两种含义 —— 正常收尾 vs 意外崩溃。监控逻辑必须
  能区分，否则一个"合法的空转"就会拖垮整个进程。

## 坑 5：下单超时（DEADLINE_EXCEEDED）

- **现象**：三服务都 Running，但下单返回 `DEADLINE_EXCEEDED`；order 调 inventory
  也超时。
- **根因**：Docker Desktop 的 K8s 用 gvisor 网络，**Service 名首次解析+建连要 2 秒**
  （`inventory:50052` 测出来 2.01s，直连 Pod IP 只要 0.00s），而项目默认给
  inventory 的超时只有 **1000ms** —— 第一次调用必然超时。
- **解法**：ConfigMap 里放宽 `MM_GATEWAY_DEADLINE_MS=5000`、`MM_ORDER_RESERVE_TIMEOUT_MS=3000`。
- **教训**：超时不是越短越好，要**覆盖冷启动/首次建连的开销**。这也是为什么
  生产系统要看「首次请求延迟」而不只是压测的稳态延迟。

## 复盘总结：一条主线

```
坑 1/2（镜像源）→ 坑 3（镜像分发）→ 坑 4（代码 bug）→ 坑 5（网络延迟）
    └── 拉不到镜像 ──┘      └── K8s 拿不到镜像 ──┘  └── 能拿到后 ──┘
```

前三个坑都是「**镜像从哪来**」的问题，最后两个是「**拿到镜像后怎么跑稳**」。
这也是真实上 K8s 的两道坎：**先解决分发，再解决运行时**。

---

# 附录：常用命令速查

**Docker（镜像）**

```powershell
docker images                          # 本机有哪些镜像
docker build -f deploy/Dockerfile -t 名字:tag .   # 打镜像
docker tag 旧名 新名                    # 改名字（含仓库前缀）
docker push 仓库/名字:tag               # 推送到仓库
docker pull 仓库/名字:tag               # 从仓库拉
docker info | grep -iA5 "Registry Mirrors"  # 看镜像源配置
```

**K8s（kubectl）**

```powershell
kubectl get pods -n micro-matrix          # 看 Pod 状态
kubectl logs deployment/order -n micro-matrix -f   # 看日志（-f 跟随）
kubectl describe pod <名> -n micro-matrix  # 看详情和事件
kubectl get events -n micro-matrix --sort-by=.lastTimestamp  # 看集群事件
kubectl apply -f deploy/k8s/              # 部署/更新
kubectl rollout restart deployment/order -n micro-matrix  # 重启（不重新打包）
kubectl port-forward svc/gateway 8000:8000 -n micro-matrix  # 端口转发到本机
kubectl delete -f deploy/k8s/             # 清掉全部
```

**三条「遇到报错先做」**：

1. `kubectl get pods` 看状态（Running 才正常）。
2. `kubectl logs` 看日志（90% 的答案在里面）。
3. `kubectl describe pod` 看事件（拉不到镜像/探针失败都在这）。
