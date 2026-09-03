#!/bin/bash
# ============================================================
# إيقاف نظيف لكل مكونات مشروع uber_pipeline
# (producer / consumer / spark streaming / spark batch)
# ============================================================

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"

echo "== إيقاف Spark Streaming (Kafka -> Silver) =="
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

echo "== إيقاف Spark Batch (Bronze/Batch -> Silver) =="
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

echo "== إيقاف الـ producer =="
pkill -15 -f "kafka/producer.py" 2>/dev/null

echo "== إيقاف الـ consumer =="
pkill -15 -f "kafka/consumer_to_bronze.py" 2>/dev/null

echo "== التأكد إن مفيش أي process متبقي =="
ps -ef | grep -E "stream_etl.py|batch_stream_etl.py|producer.py|consumer_to_bronze.py" | grep -v grep

echo "== تم الإيقاف =="
# ملحوظة: Zookeeper و Kafka نفسهم (كـ systemd services) بيفضلوا شغالين،
# لو عايز توقفهم كمان:
#   sudo systemctl stop kafka
#   sudo systemctl stop zookeeper

