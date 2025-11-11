SSR_PATH=/opt/sqlbot/g2-ssr
APP_PATH=/opt/sqlbot/app

/opt/sqlbot/app/.venv/bin/python -V || true

# 激活虚拟环境，确保安装在 venv 的依赖可用
if [ -f "/opt/sqlbot/app/.venv/bin/activate" ]; then
  . /opt/sqlbot/app/.venv/bin/activate
  export PATH="/opt/sqlbot/app/.venv/bin:$PATH"
  export PYTHONPATH="/opt/sqlbot/app"
else
  echo "\033[1;31m未找到 venv，继续尝试系统 python\033[0m"
fi

# 如缺失 sqlbot_xpack，尝试从镜像内离线安装（幂等）
if ! python -c "import sqlbot_xpack" >/dev/null 2>&1; then
  echo -e "\033[1;33m未检测到 sqlbot_xpack，尝试安装...\033[0m"
  if [ -f "/opt/sqlbot/app/sqlbot_xpack-0.0.3.41-cp311-cp311-manylinux2014_x86_64.whl" ]; then
    # 允许从镜像源拉取依赖（例如 cython 等）
    pip install -i https://mirrors.aliyun.com/pypi/simple --no-cache-dir /opt/sqlbot/app/sqlbot_xpack-0.0.3.41-cp311-cp311-manylinux2014_x86_64.whl || true
  else
    echo -e "\033[1;31m离线 whl 不存在，跳过\033[0m"
  fi
fi

/usr/local/bin/docker-entrypoint.sh postgres &
sleep 5
wait-for-it 127.0.0.1:5432 --timeout=120 --strict -- echo -e "\033[1;32mPostgreSQL started.\033[0m"

nohup node $SSR_PATH/app.js &

nohup python -m uvicorn main:mcp_app --host 0.0.0.0 --port 8001 &

cd $APP_PATH
python -m uvicorn main:app --host 0.0.0.0 --port 8010 --workers 1 --proxy-headers
