#!/bin/bash
# ============================================================
# دليل تشغيل مشروع uber_pipeline بعد أي إعادة تشغيل للـ VM
# (نسخة موحّدة: streaming + batch)
# ينفَّذ يدويًا خطوة خطوة، مش كسكريبت واحد تلقائي،
# لأن كل جزء (consumer/producer/spark stream/spark batch) لازم
# يفضل شغال في terminal منفصل طول فترة الاستخدام.
# ============================================================

# ---------- 0) تأكد إن PROJECT_ROOT متعرف ----------
# (لو مش موجود في .bashrc، ضيفه:)
#   echo 'export PROJECT_ROOT=$HOME/uber_pipeline' >> ~/.bashrc
#   source ~/.bashrc
echo "PROJECT_ROOT = $PROJECT_ROOT"


# ---------- 1) شغّل Zookeeper و Kafka (لو مش شغالين تلقائيًا) ----------
sudo systemctl start zookeeper
sleep 5
sudo systemctl status zookeeper 2>&1 | head -6

sudo systemctl start kafka
sleep 8
sudo systemctl status kafka 2>&1 | head -6

# تأكد إن الاتنين شغالين فعليًا
ps -ef | grep -E "QuorumPeerMain|kafka\.Kafka" | grep -v grep

# لو Kafka فشل بسبب "Failed to acquire lock on file .lock":
#   ps -ef | grep "kafka.Kafka" | grep -v grep   # تأكد مفيش process شغال
#   sudo rm -f /tmp/kafka-logs/.lock
#   sudo systemctl start kafka

# لو Kafka فشل بسبب "node already exists" (ephemeral node قديم في Zookeeper):
#   zookeeper-shell.sh localhost:2181 <<< "rmr /brokers/ids/0"
#   sudo systemctl start kafka


# ---------- 2) تأكد إن الـ topic موجود ----------
kafka-topics --list --bootstrap-server localhost:9092
# المفروض تشوف: uber_stream_dec


# ---------- 3) تأكد إن HDFS شغال وهيكل الطبقات موجود ----------
hdfs dfs -ls /home/student/uber_pipeline

# لو الهيكل مش موجود (أول مرة فقط):
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/streaming
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/bronze/batch
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/stream_output
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/silver/batch_output
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/gold
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/stream_etl
#   hdfs dfs -mkdir -p /home/student/uber_pipeline/checkpoints/batch_etl


# ---------- 4) تأكد إن symlink مصدر الـ stream موجود ----------
ls -la "$PROJECT_ROOT/kafka/stream_source.csv"
# لو مفقود:
#   ln -sf "$PROJECT_ROOT/data/raw/uber_raw_data_12_14.csv" "$PROJECT_ROOT/kafka/stream_source.csv"


# ============================================================
# من هنا فصاعدًا، كل خطوة في terminal منفصل خاص بيها
# ============================================================

# ---------- TERMINAL 1: consumer (Kafka -> Bronze خام على HDFS) ----------
: '
python3 "$PROJECT_ROOT/kafka/consumer_to_bronze.py" \
  --hdfs-dir "hdfs://localhost:9000/home/student/uber_pipeline/bronze/streaming" \
  --batch-size 20 --batch-interval 10
'

# ---------- TERMINAL 2: producer (يبعت الملف صف صف لـ Kafka) ----------
: '
python3 "$PROJECT_ROOT/kafka/producer.py" \
  --file "$PROJECT_ROOT/kafka/stream_source.csv" \
  --interval 1.5
'

# ---------- TERMINAL 3: Spark Structured Streaming (Kafka -> Silver مع منع التكرار) ----------
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

# ---------- TERMINAL 4: Spark Structured Streaming (Bronze/Batch -> Silver مع منع التكرار) ----------
# بيراقب مجلد bronze/batch على HDFS، وأي ملف جديد يحطه NiFi هناك بيتقرأ ويتعالج أوتوماتيك
# بيشارك نفس ملف seen_trip_ids مع الـ streaming عشان الـ dedup يبقى موحّد بين المصدرين
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

# ---------- تحقق دوري (أي وقت) ----------
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

# ---------- إيقاف كل حاجة نظيف ----------
# استخدم سكريبت stop_pipeline_background.sh المرفق بدل القطع اليدوي بـ Ctrl+C
# عشان تتجنب الـ EOFError في الـ Python workers.
