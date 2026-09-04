#!/bin/bash
# ============================================================
# start_batch.sh
# Complete automated startup for the Batch component:
# Spark Structured Streaming (File Source) monitors bronze/batch
# and writes to silver/batch_output, sharing the same dedup file with streaming.
# Runs once in the background without manual intervention.
# ============================================================
set -e

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"
mkdir -p "$PROJECT_ROOT/pids" "$PROJECT_ROOT/logs"

echo "=============================================="
echo " PROJECT_ROOT = $PROJECT_ROOT"
echo "=============================================="

# ---------- 1) Ensure required HDFS structure ----------
echo "[1/2] Verifying HDFS paths..."
hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/batch
hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/batch_output
hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/batch_etl

# ---------- 2) Run Spark Batch ETL ----------
echo "[2/2] Running Spark Batch ETL..."
PYSPARK_DRIVER_PYTHON=python3 PYSPARK_DRIVER_PYTHON_OPTS="" nohup spark-submit \
  --master local[*] \
  "$PROJECT_ROOT/spark/batch/batch_stream_etl.py" \
  --bronze-path "hdfs://localhost:9000/home/student/uber_pipeline/bronze/batch" \
  --silver-output "hdfs://localhost:9000/home/student/uber_pipeline/silver/batch_output" \
  --checkpoint "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/batch_etl" \
  --seen-ids-path "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/seen_trip_ids" \
  > "$PROJECT_ROOT/spark_batch.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/batch_etl.pid"
echo "Spark Batch PID: $(cat "$PROJECT_ROOT/pids/batch_etl.pid")"

echo "=============================================="
echo " Batch component started successfully."
echo " Monitor the logs:"
echo "   tail -f $PROJECT_ROOT/spark_batch.log"
echo " To stop: $PROJECT_ROOT/stop_batch.sh"
echo "=============================================="
