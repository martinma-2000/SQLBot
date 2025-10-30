# Build sqlbot
FROM ghcr.io/1panel-dev/maxkb-vector-model:v1.0.1 AS vector-model
FROM registry.cn-qingdao.aliyuncs.com/dataease/sqlbot-base:latest AS sqlbot-builder

# Set build environment variables
ENV PYTHONUNBUFFERED=1
ENV SQLBOT_HOME=/opt/sqlbot
ENV APP_HOME=${SQLBOT_HOME}/app
ENV UI_HOME=${SQLBOT_HOME}/frontend
ENV PYTHONPATH=${SQLBOT_HOME}/app
ENV PATH="${APP_HOME}/.venv/bin:$PATH"
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV DEBIAN_FRONTEND=noninteractive

# 安装JDK
COPY jdk/jdk-8u471-linux-x64.tar.gz /tmp/jdk-8u471-linux-x64.tar.gz
RUN mkdir -p /usr/lib/jvm && \
    tar -zxvf /tmp/jdk-8u471-linux-x64.tar.gz -C /usr/lib/jvm && \
    mv /usr/lib/jvm/jdk-8u471-linux-x64 /usr/lib/jvm/jdk-8u471
ENV JAVA_HOME=/usr/lib/jvm/jdk-8u471
ENV JRE_HOME=$JAVA_HOME/jre
ENV CLASSPATH=$JAVA_HOME/lib:$JRE_HOME/lib
ENV PATH=$JAVA_HOME/bin:$PATH

# Create necessary directories
RUN mkdir -p ${APP_HOME} ${UI_HOME}

WORKDIR ${APP_HOME}

COPY frontend /tmp/frontend

RUN cd /tmp/frontend; npm install; npm run build; mv dist ${UI_HOME}/dist

# 安装依赖锁文件（如果存在）
RUN test -f "./uv.lock" && \
    --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=backend/uv.lock,target=uv.lock \
    --mount=type=bind,source=backend/pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project || echo "uv.lock file not found, skipping intermediate-layers"

# 复制 backend 代码
COPY ./backend ${APP_HOME}

# 复制离线 whl 文件到构建容器
COPY sqlbot_xpack-0.0.3.41-cp311-cp311-manylinux2014_x86_64.whl /tmp/

# 创建虚拟环境 -> 离线安装 sqlbot-xpack -> 安装剩余依赖（缓存挂载在 RUN 最前面）
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv && \
    uv pip install /tmp/sqlbot_xpack-0.0.3.41-cp311-cp311-manylinux2014_x86_64.whl && \
    UV_HTTP_TIMEOUT=300 uv sync --extra cpu --index-url https://mirrors.aliyun.com/pypi/simple

# Build g2-ssr
FROM registry.cn-qingdao.aliyuncs.com/dataease/sqlbot-base:latest AS ssr-builder

WORKDIR /app

COPY g2-ssr/app.js g2-ssr/package.json /app/
COPY g2-ssr/charts/* /app/charts/

RUN npm install

# Runtime stage
FROM registry.cn-qingdao.aliyuncs.com/dataease/sqlbot-python-pg:latest

RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

# 安装常用工具（vim/ping/curl/net-tools等）
RUN apt-get update && apt-get install -y \
    vim \
    iputils-ping \
    curl \
    net-tools \
    iproute2 \
    less \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set runtime environment variables
ENV PYTHONUNBUFFERED=1
ENV SQLBOT_HOME=/opt/sqlbot
ENV PYTHONPATH=${SQLBOT_HOME}/app
ENV PATH="${SQLBOT_HOME}/app/.venv/bin:$PATH"

ENV POSTGRES_DB=sqlbot
ENV POSTGRES_USER=root
ENV POSTGRES_PASSWORD=Password123@pg

# Copy necessary files from builder
COPY start.sh /opt/sqlbot/app/start.sh
COPY g2-ssr/*.ttf /usr/share/fonts/truetype/liberation/
COPY --from=sqlbot-builder ${SQLBOT_HOME} ${SQLBOT_HOME}
COPY --from=ssr-builder /app /opt/sqlbot/g2-ssr
COPY --from=vector-model /opt/maxkb/app/model /opt/sqlbot/models

WORKDIR ${SQLBOT_HOME}/app

RUN mkdir -p /opt/sqlbot/images /opt/sqlbot/g2-ssr

EXPOSE 3000 8000 8001 5432

# Add health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000 || exit 1

ENTRYPOINT ["sh", "start.sh"]
