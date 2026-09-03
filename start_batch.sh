#!/bin/bash
# ============================================================
# start_batch.sh
# تشغيل تلقائي كامل لجزء الـ Batch:
# Spark Structured Streaming (File Source) بيراقب bronze/batch
# ويكتب في silver/batch_output، بمشاركة نفس ملف dedup مع الـ streaming.
# تشغيل واحد بس، بيشتغل في الخلفية من غير تدخل يدوي.
# ============================================================
set -e

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"
mkdir -p "$PROJECT_ROOT/pids" "$PROJECT_ROOT/logs"

echo "=============================================="
echo " PROJECT_ROOT = $PROJECT_ROOT"
echo "=============================================="

# ---------- 1) التأكد من هيكل HDFS المطلوب ----------
echo "[1/2] التأكد من مسارات HDFS..."
hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/batch
hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/batch_output
hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/batch_etl

# ---------- 2) تشغيل Spark Batch ETL ----------
echo "[2/2] تشغيل Spark Batch ETL..."
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
echo " تم تشغيل جزء الـ Batch بنجاح."
echo " تابع اللوج:"
echo "   tail -f $PROJECT_ROOT/spark_batch.log"
echo " للإيقاف: $PROJECT_ROOT/stop_batch.sh"
echo "=============================================="
