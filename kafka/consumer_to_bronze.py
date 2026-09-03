import argparse
import os
import subprocess
import tempfile
from datetime import datetime
from kafka import KafkaConsumer

HEADER = "trip_id,start_lat,start_lon,end_lat,end_lon,start_time,end_time,distance"

def flush_to_hdfs(buffer, hdfs_dir):
    if not buffer:
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"stream_batch_{ts}.csv"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
        tmp.write(HEADER + "\n")
        tmp.write("\n".join(buffer) + "\n")
        tmp_path = tmp.name

    hdfs_target = f"{hdfs_dir.rstrip('/')}/{filename}"
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_target],
        capture_output=True, text=True
    )
    os.remove(tmp_path)

    if result.returncode != 0:
        print(f"[consumer] FAILED to write {hdfs_target}: {result.stderr}")
    else:
        print(f"[consumer] flushed {len(buffer)} rows -> {hdfs_target}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="uber_stream_dec")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--hdfs-dir", required=True,
                         help="Full bronze/streaming path on HDFS")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--batch-interval", type=float, default=10.0)
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )

    buffer = []
    import time
    last_flush = time.time()

    print(f"[consumer] streaming to HDFS: {args.hdfs_dir} ... Press Ctrl+C to stop")
    try:
        while True:
            for msg in consumer:
                buffer.append(msg.value)
                if len(buffer) >= args.batch_size:
                    flush_to_hdfs(buffer, args.hdfs_dir)
                    buffer = []
                    last_flush = time.time()

            if buffer and (time.time() - last_flush) >= args.batch_interval:
                flush_to_hdfs(buffer, args.hdfs_dir)
                buffer = []
                last_flush = time.time()
    except KeyboardInterrupt:
        flush_to_hdfs(buffer, args.hdfs_dir)
        print("\n[consumer] stopped by user.")

if __name__ == "__main__":
    main()