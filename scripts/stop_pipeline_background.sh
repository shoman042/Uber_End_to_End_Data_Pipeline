#!/bin/bash
# ============================================================
# Graceful shutdown for all uber_pipeline project components
# (producer / consumer / spark streaming / spark batch)
# ============================================================

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"

echo "== Stopping Spark Streaming (Kafka -> Silver) =="
if [ -f "$PROJECT_ROOT/pids/stream_etl.pid" ]; then
    kill -15 "$(cat "$PROJECT_ROOT/pids/stream_etl.pid")" 2>/dev/null
    sleep 3
    kill -9 "$(cat "$PROJECT_ROOT/pids/stream_etl.pid")" 2>/dev/null
    rm -f "$PROJECT_ROOT/pids/stream_etl.pid"
else
    pkill -15 -f "spark/streaming/stream_etl.py" 2>/dev/null
    sleep 3
    pkill -9 -f "spark/streaming/stream_etl.py" 2>/dev/null
fi

echo "== Stopping Spark Batch (Bronze/Batch -> Silver) =="
if [ -f "$PROJECT_ROOT/pids/batch_etl.pid" ]; then
    kill -15 "$(cat "$PROJECT_ROOT/pids/batch_etl.pid")" 2>/dev/null
    sleep 3
    kill -9 "$(cat "$PROJECT_ROOT/pids/batch_etl.pid")" 2>/dev/null
    rm -f "$PROJECT_ROOT/pids/batch_etl.pid"
else
    pkill -15 -f "spark/batch/batch_stream_etl.py" 2>/dev/null
    sleep 3
    pkill -9 -f "spark/batch/batch_stream_etl.py" 2>/dev/null
fi

echo "== Stopping the producer =="
pkill -15 -f "kafka/producer.py" 2>/dev/null

echo "== Stopping the consumer =="
pkill -15 -f "kafka/consumer_to_bronze.py" 2>/dev/null

echo "== Ensuring no leftover processes remain =="
ps -ef | grep -E "stream_etl.py|batch_stream_etl.py|producer.py|consumer_to_bronze.py" | grep -v grep

echo "== Stopped successfully =="
# Note: Zookeeper and Kafka themselves (as systemd services) remain running,
# If you want to stop them as well:
#   sudo systemctl stop kafka
#   sudo systemctl stop zookeeper
