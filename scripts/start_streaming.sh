#!/bin/bash
# ============================================================
# start_streaming.sh
# Complete automated startup for the Streaming component:
# Zookeeper + Kafka + Producer + Consumer + Spark Streaming
# Runs once in the background without manual intervention.
# ============================================================
set -e

PROJECT_ROOT="${PROJECT_ROOT:-$HOME/uber_pipeline}"
mkdir -p "$PROJECT_ROOT/pids" "$PROJECT_ROOT/logs"

echo "=============================================="
echo " PROJECT_ROOT = $PROJECT_ROOT"
echo "=============================================="

# ---------- 1) Zookeeper + Kafka ----------
echo "[1/6] Starting Zookeeper..."
sudo systemctl start zookeeper
sleep 5

echo "[2/6] Starting Kafka..."
sudo systemctl start kafka
sleep 8

if ! ps -ef | grep -q "[Q]uorumPeerMain"; then
    echo "Error: Zookeeper is not running. Check: sudo systemctl status zookeeper"
    exit 1
fi
if ! ps -ef | grep -q "[k]afka.Kafka"; then
    echo "Error: Kafka is not running. Check: sudo systemctl status kafka"
    exit 1
fi
echo "Zookeeper and Kafka are running."

# ---------- 2) Ensure topic exists ----------
echo "[3/6] Verifying topic existence..."
if ! kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^uber_stream_dec$"; then
    echo "Topic does not exist, creating it now..."
    kafka-topics --create --topic uber_stream_dec \
        --bootstrap-server localhost:9092 \
        --partitions 1 --replication-factor 1
fi
echo "Topic uber_stream_dec exists."

# ---------- 3) Ensure stream source symlink exists ----------
echo "[4/6] Verifying data source symlink..."
if [ ! -e "$PROJECT_ROOT/kafka/stream_source.csv" ]; then
    ln -sf "$PROJECT_ROOT/data/raw/uber_raw_data_12_14.csv" "$PROJECT_ROOT/kafka/stream_source.csv"
fi
echo "Symlink exists: $(readlink -f "$PROJECT_ROOT/kafka/stream_source.csv")"

# ---------- 4) Run consumer (Kafka -> Raw Bronze) ----------
echo "[5/6] Starting consumer..."
nohup python3 "$PROJECT_ROOT/kafka/consumer_to_bronze.py" \
  --hdfs-dir "hdfs://localhost:9000/home/student/uber_pipeline/bronze/streaming" \
  --batch-size 20 --batch-interval 10 \
  > "$PROJECT_ROOT/logs/consumer.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/consumer.pid"
echo "Consumer PID: $(cat "$PROJECT_ROOT/pids/consumer.pid")"

sleep 2

# ---------- 5) Run producer (Sends data to Kafka) ----------
echo "[6/6] Starting producer..."
nohup python3 "$PROJECT_ROOT/kafka/producer.py" \
  --file "$PROJECT_ROOT/kafka/stream_source.csv" \
  --interval 1.5 \
  > "$PROJECT_ROOT/logs/producer.log" 2>&1 &
echo $! > "$PROJECT_ROOT/pids/producer.pid"
echo "Producer PID: $(cat "$PROJECT_ROOT/pids/producer.pid")"

sleep 2

# ---------- 6) Run Spark Structured Streaming (Kafka -> Silver) ----------
echo "Starting Spark Streaming ETL..."
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
echo " All streaming components started successfully."
echo " Monitor the logs:"
echo "   tail -f $PROJECT_ROOT/logs/consumer.log"
echo "   tail -f $PROJECT_ROOT/logs/producer.log"
echo "   tail -f $PROJECT_ROOT/spark_stream.log"
echo " To stop: $PROJECT_ROOT/stop_streaming.sh"
echo "=============================================="
