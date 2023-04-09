#!/bin/bash

# 自定义退出函数
function cleanup {
  echo "Stopping all server processes..."
  # 获取所有正在运行的 server.py 进程 ID
  pids=$(ps aux | grep '[s]erver.py' | awk '{print $2}')
  # 杀掉所有进程
  for pid in $pids; do
    kill -9 "$pid"
  done
  echo "All server processes have been terminated."
  exit
}

# 注册退出函数
trap cleanup SIGINT SIGTERM

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PRJ_ABS_PATH=$(realpath "${SCRIPT_DIR}/../../")

export PYTHONPATH=${PRJ_ABS_PATH}

python3 ./src/server/server.py --port 8001 --members 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 --store_path test_store1.db &
echo "Server on port 8001 is running in the background."

python3 ./src/server/server.py --port 8002 --members 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 --store_path test_store2.db &
echo "Server on port 8002 is running in the background."

#python3 ./src/server/server.py --port 8003 --members 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 --store_path test_store3.db &
#echo "Server on port 8003 is running in the background."

# 阻塞等待中断信号
while true; do
  sleep 1
done
