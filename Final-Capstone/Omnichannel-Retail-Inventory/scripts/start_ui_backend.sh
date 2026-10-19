#!/bin/bash

set -e

echo "========================================"
echo "OMNICHANNEL UI BACKEND STARTUP"
echo "========================================"

echo
echo "[1/4] Starting HDFS..."
start-dfs.sh >/dev/null 2>&1 || true
sleep 3
echo "HDFS start command completed."

echo
echo "[2/4] Starting HBase..."
start-hbase.sh >/dev/null 2>&1 || true
sleep 5
echo "HBase start command completed."

echo
echo "[3/4] Starting HBase Thrift..."
if ! ss -ltn | grep -q ":9090"; then
    nohup hbase thrift start \
        > ~/hbase-thrift-ui.log 2>&1 &
    sleep 12
fi

echo
echo "[4/4] Checking port 9090..."

if ss -ltn | grep -q ":9090"; then
    echo "HBase Thrift is listening on port 9090."
else
    echo "WARNING: HBase Thrift is not listening on port 9090."
    echo "Check: ~/hbase-thrift-ui.log"
fi

echo
echo "========================================"
echo "BACKEND STARTUP COMPLETE"
echo "========================================"

echo
echo "Java services:"
jps
