#!/bin/bash
# ============================================================
# uber_pipeline execution guide after any VM restart
# (Unified version: streaming + batch)
# Executed manually step-by-step rather than as a single automatic script,
# because each part (consumer/producer/spark stream/spark batch) must
# remain running in a separate terminal throughout usage.
# ============================================================

# ---------- 0) Ensure PROJECT_ROOT is defined ----------
# (If not present in .bashrc, add it:)
#   echo 'export PROJECT_ROOT=$HOME/uber_pipeline' >> ~/.bashrc
#   source ~/.bashrc
echo "PROJECT_ROOT = $PROJECT_ROOT"


# ---------- 1) Start Zookeeper and Kafka (if not running automatically) ----------
sudo systemctl start zookeeper
sleep 5
sudo systemctl status zookeeper 2>&1 | head -6

sudo systemctl start kafka
sleep 8
sudo systemctl status kafka 2>&1 | head -6

# Verify both are actually running
ps -ef | grep -E "QuorumPeerMain|kafka\.Kafka" | grep -v grep

# If Kafka fails due to "Failed to acquire lock on file .lock":
#   ps -ef | grep "kafka.Kafka" | grep -v grep   # verify no process is running
#   sudo rm -f /tmp/kafka-logs/.lock
#   sudo systemctl start kafka

# If Kafka fails due to "node already exists" (stale ephemeral node in Zookeeper):
#   zookeeper-shell.sh localhost:2181 <<< "rmr /brokers/ids/0"
#   sudo systemctl start kafka


# ---------- 2) Ensure the topic exists ----------
kafka-topics --list --bootstrap-server localhost:9092
# You should see: uber_stream_dec


# ---------- 3) Ensure HDFS is running and the layer structure exists ----------
hdfs dfs -ls /home/student/uber_pipeline

# If structure doesn't exist (first time only):
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/streaming
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/batch
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/stream_output
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/batch_output
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/gold
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/stream_etl
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/batch_etl


# ---------- 4) Ensure stream source symlink exists ----------
ls -la "$PROJECT_ROOT/kafka/stream_source.csv"
# If missing:
#   ln -sf "$PROJECT_ROOT/data/raw/uber_raw_data_12_14.csv" "$PROJECT_ROOT/kafka/stream_source.csv"


# ============================================================
# From here on, each step runs in its own dedicated terminal
# ============================================================

# ---------- TERMINAL 1: consumer (Kafka -> Raw Bronze on HDFS) ----------
: '
python3 "$PROJECT_ROOT/kafka/consumer_to_bronze.py" \
  --hdfs-dir "hdfs://localhost:9000/home/student/uber_pipeline/bronze/streaming" \
  --batch-size 20 --batch-interval 10
'

# ---------- TERMINAL 2: producer (Sends file row-by-row to Kafka) ----------
: '
python3 "$PROJECT_ROOT/kafka/producer.py" \
  --file "$PROJECT_ROOT/kafka/stream_source.csv" \
  --interval 1.5
'

# ---------- TERMINAL 3: Spark Structured Streaming (Kafka -> Silver with deduplication) ----------
: '
PYSPARK_DRIVER_PYTHON=python3 PYSPARK_DRIVER_PYTHON_OPTS="" spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  "$PROJECT_ROOT/spark/streaming/stream_etl.py" \
  --topic uber_stream_dec \
  --starting-offsets earliest \
  --silver-output "hdfs://localhost:9000/home/student/uber_pipeline/silver/stream_output" \
  --checkpoint "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/stream_etl" \
  --seen-ids-path "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/seen_trip_ids" \
  > "$PROJECT_ROOT/spark_stream.log" 2>&1 &
echo "Stream ETL PID: $!"
echo $! > "$PROJECT_ROOT/pids/stream_etl.pid"
'

# ---------- TERMINAL 4: Spark Structured Streaming (Bronze/Batch -> Silver with deduplication) ----------
# Monitors bronze/batch on HDFS; any new file placed there by NiFi is read and processed automatically.
# Shares the same seen_trip_ids file with streaming to keep deduplication unified across both sources.
: '
PYSPARK_DRIVER_PYTHON=python3 PYSPARK_DRIVER_PYTHON_OPTS="" spark-submit \
  --master local[*] \
  "$PROJECT_ROOT/spark/batch/batch_stream_etl.py" \
  --bronze-path "hdfs://localhost:9000/home/student/uber_pipeline/bronze/batch" \
  --silver-output "hdfs://localhost:9000/home/student/uber_pipeline/silver/batch_output" \
  --checkpoint "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/batch_etl" \
  --seen-ids-path "hdfs://localhost:9000/home/student/uber_pipeline/checkpoints/seen_trip_ids" \
  > "$PROJECT_ROOT/spark_batch.log" 2>&1 &
echo "Batch ETL PID: $!"
echo $! > "$PROJECT_ROOT/pids/batch_etl.pid"
'

# ---------- Periodic checks (anytime) ----------
: '
hdfs dfs -ls "hdfs://localhost:9000/home/student/uber_pipeline/bronze/streaming"
hdfs dfs -ls "hdfs://localhost:9000/home/student/uber_pipeline/silver/stream_output"
hdfs dfs -ls "hdfs://localhost:9000/home/student/uber_pipeline/bronze/batch"
hdfs dfs -ls "hdfs://localhost:9000/home/student/uber_pipeline/silver/batch_output"

grep -n "ERROR\|Traceback\|Exception" "$PROJECT_ROOT/spark_stream.log" | grep -v "KafkaDataConsumer"
grep "batch" "$PROJECT_ROOT/spark_stream.log" | tail -20

grep -n "ERROR\|Traceback\|Exception" "$PROJECT_ROOT/spark_batch.log"
grep "batch" "$PROJECT_ROOT/spark_batch.log" | tail -20
'

# ---------- Clean shutdown of everything ----------
# Use the provided stop_pipeline_background.sh script instead of manual Ctrl+C cuts
# to avoid EOFErrors in Python workers.
