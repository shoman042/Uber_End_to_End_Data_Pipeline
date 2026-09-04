#!/bin/bash
# ============================================================
# stop_batch.sh
# Graceful shutdown for the Batch component (Spark Structured Streaming - File Source)
# ============================================================

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"

if [ -f "$PROJECT_ROOT/pids/batch_etl.pid" ]; then
    PID="$(cat "$PROJECT_ROOT/pids/batch_etl.pid")"
    if kill -0 "$PID" 2>/dev/null; then
        echo "== Stopping Spark Batch ETL (PID $PID) =="
        kill -15 "$PID" 2>/dev/null
        sleep 3
        kill -9 "$PID" 2>/dev/null
    fi
    rm -f "$PROJECT_ROOT/pids/batch_etl.pid"
else
    echo "== Stopping Spark Batch ETL (fallback process search) =="
    pkill -15 -f "spark/batch/batch_stream_etl.py" 2>/dev/null
    sleep 3
    pkill -9 -f "spark/batch/batch_stream_etl.py" 2>/dev/null
fi

echo "== Ensuring no leftover processes remain =="
ps -ef | grep "batch_stream_etl.py" | grep -v grep

echo "=============================================="
echo " Batch component stopped."
echo "=============================================="
