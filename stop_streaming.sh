#!/bin/bash
# ============================================================
# stop_streaming.sh
# إيقاف نظيف لكل مكونات جزء الـ Streaming:
# Producer + Consumer + Spark Streaming
# (Zookeeper و Kafka نفسهم بيفضلوا شغالين كـ systemd services،
#  إلا لو حددت --with-kafka)
# ============================================================

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"

stop_by_pidfile() {
    local name="$1"
    local pidfile="$2"
    local pattern="$3"

    if [ -f "$pidfile" ]; then
        local pid
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            echo "== إيقاف $name (PID $pid) =="
            kill -15 "$pid" 2>/dev/null
            sleep 3
            kill -9 "$pid" 2>/dev/null
        fi
        rm -f "$pidfile"
    else
        echo "== إيقاف $name (fallback بالبحث عن الـ process) =="
        pkill -15 -f "$pattern" 2>/dev/null
        sleep 3
        pkill -9 -f "$pattern" 2>/dev/null
    fi
}

stop_by_pidfile "Spark Streaming ETL" "$PROJECT_ROOT/pids/stream_etl.pid" "spark/streaming/stream_etl.py"
stop_by_pidfile "Producer"            "$PROJECT_ROOT/pids/producer.pid"   "kafka/producer.py"
stop_by_pidfile "Consumer"            "$PROJECT_ROOT/pids/consumer.pid"   "kafka/consumer_to_bronze.py"

echo "== التأكد إن مفيش أي process متبقي =="
ps -ef | grep -E "stream_etl.py|producer.py|consumer_to_bronze.py" | grep -v grep

echo "=============================================="
echo " تم إيقاف جزء الـ Streaming."
echo " ملحوظة: Zookeeper و Kafka لسه شغالين (systemd services)."
echo " لو عايز توقفهم كمان:"
echo "   sudo systemctl stop kafka"
echo "   sudo systemctl stop zookeeper"
echo "=============================================="
