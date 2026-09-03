#!/bin/bash
# ============================================================
# start_streaming.sh
# تشغيل تلقائي كامل لجزء الـ Streaming:
# Zookeeper + Kafka + Producer + Consumer + Spark Streaming
# تشغيل واحد بس، وكل حاجة بتشتغل في الخلفية من غير تدخل يدوي.
# ============================================================
set -e

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"
mkdir -p "$PROJECT_ROOT/pids" "$PROJECT_ROOT/logs"

echo "=============================================="
echo " PROJECT_ROOT = $PROJECT_ROOT"
echo "=============================================="

# ---------- 1) Zookeeper + Kafka ----------
echo "[1/6] تشغيل Zookeeper..."
sudo systemctl start zookeeper
sleep 5

echo "[2/6] تشغيل Kafka..."
sudo systemctl start kafka
sleep 8

if ! ps -ef | grep -q "[Q]uorumPeerMain"; then
    echo "خطأ: Zookeeper مش شغال. راجع: sudo systemctl status zookeeper"
    exit 1
fi
if ! ps -ef | grep -q "[k]afka.Kafka"; then
    echo "خطأ: Kafka مش شغال. راجع: sudo systemctl status kafka"
    exit 1
fi
echo "Zookeeper و Kafka شغالين."

# ---------- 2) تأكيد وجود الـ topic ----------
echo "[3/6] التأكد من وجود الـ topic..."
if ! kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^uber_stream_dec$"; then
    echo "الـ topic مش موجود، بيتعمل دلوقتي..."
    kafka-topics --create --topic uber_stream_dec \
        --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1
fi
echo "الـ topic uber_stream_dec موجود."

# ---------- 3) التأكد من symlink مصدر الـ stream ----------
echo "[4/6] التأكد من symlink مصدر الداتا..."
if [ ! -e "$PROJECT_ROOT/kafka/stream_source.csv" ]; then
    ln -sf "$PROJECT_ROOT/data/raw/uber_raw_data_12_14.csv" "$PROJECT_ROOT/kafka/stream_source.csv"
fi
echo "symlink موجود: $(readlink -f "$PROJECT_ROOT/kafka/stream_source.csv")"

# ---------- 4) تشغيل الـ consumer (Kafka -> Bronze خام) ----------
echo "[5/6] تشغيل الـ consumer..."
nohup python3 "$PROJECT_ROOT/kafka/consumer_to_bronze.py" \
  --hdfs-dir "hdfs://localhost:9000/home/student/uber_pipeline/bronze/streaming" \
  --batch-size 20 --batch-interval 10 \
  > "$PROJECT_ROOT/logs/consumer.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/consumer.pid"
echo "Consumer PID: $(cat "$PROJECT_ROOT/pids/consumer.pid")"

sleep 2

# ---------- 5) تشغيل الـ producer (يبعت الداتا لـ Kafka) ----------
echo "[6/6] تشغيل الـ producer..."
nohup python3 "$PROJECT_ROOT/kafka/producer.py" \
  --file "$PROJECT_ROOT/kafka/stream_source.csv" \
  --interval 1.5 \
  > "$PROJECT_ROOT/logs/producer.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/producer.pid"
echo "Producer PID: $(cat "$PROJECT_ROOT/pids/producer.pid")"

sleep 2

# ---------- 6) تشغيل Spark Structured Streaming (Kafka -> Silver) ----------
echo "تشغيل Spark Streaming ETL..."
PYSPARK_DRIVER_PYTHON=python3 PYSPARK_DRIVER_PYTHON_OPTS="" nohup spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  --master local[*] \
  "$PROJECT_ROOT/spark/streaming/stream_etl.py" \
  --topic uber_stream_dec \
  --starting-offsets earliest \
  --silver-output "hdfs://localhost:9000/home/student/uber_pipeline/silver/stream_output" \
  --checkpoint "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/stream_etl" \
  --seen-ids-path "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/seen_trip_ids" \
  > "$PROJECT_ROOT/spark_stream.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/stream_etl.pid"
echo "Spark Streaming PID: $(cat "$PROJECT_ROOT/pids/stream_etl.pid")"

echo "=============================================="
echo " تم تشغيل كل مكونات الـ Streaming بنجاح."
echo " تابع اللوجات:"
echo "   tail -f $PROJECT_ROOT/logs/consumer.log"
echo "   tail -f $PROJECT_ROOT/logs/producer.log"
echo "   tail -f $PROJECT_ROOT/spark_stream.log"
echo " للإيقاف: $PROJECT_ROOT/stop_streaming.sh"
echo "=============================================="
