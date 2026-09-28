# [问题]   把业务服务打成镜像，才能上 K8s。
# [本文件] 一个镜像装下全部三个服务，运行时用 command 指定起哪个。
# [面试]   - 为什么先 COPY requirements.txt 再 COPY 代码，而不是一次性 COPY？
#           （利用 Docker 层缓存：依赖清单不变时，pip install 这一层命中缓存，
#             改代码不会触发重装依赖，构建快很多。这是"把不常变的放前面"。
#             COPY . . 这一层在依赖之后，改一行代码只重建最后一层。）
#         - 为什么三个服务共用一个镜像，而不是各打一个？
#           （它们共享同一个代码库和依赖，共用一个镜像能保证版本一致。
#             代价是镜像偏大、部署粒度粗。真实项目常按服务拆镜像，这里教学优先。）
# [取舍]   python:3.14-slim 与宿主机版本对齐，保证 grpcio/asyncpg 的 cp314 wheel
#          能直接装上（见 docs/6-runbook.md 第 7 条）。
FROM python:3.14-slim

WORKDIR /app

# 先装依赖，后拷代码 —— 层缓存的关键顺序。
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝源码。gen/ 被 .dockerignore 排除，进容器后再生成，保证与镜像里的
# grpcio-tools 版本严格一致（protobuf 运行时版本必须匹配，见 runbook 第 5 条）。
COPY . .
RUN python scripts/gen_proto.py

# -u：stdout 不缓冲。容器里若缓冲，kubectl logs 看起来会"卡住"——
# 和 runbook 第 8 条讲的是同一件事，只是换到了容器场景。
ENTRYPOINT ["python", "-u"]
