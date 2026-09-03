import argparse
import time
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--topic", default="uber_stream_dec")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=2.0,
                         help="Seconds between each message")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    sent = 0
    print("[producer] running... Press Ctrl+C to stop")
    try:
        while True:  # Re-read the file from the beginning each time it finishes, until stopped
            with open(args.file, "r", encoding="utf-8") as f:
                next(f)  # Skip the header
                for line in f:
                    line = line.rstrip("\n").rstrip("\r")
                    if not line:
                        continue
                    producer.send(args.topic, value=line)
                    sent += 1
                    if sent % 100 == 0:
                        print(f"[producer] sent {sent} rows...")
                    time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n[producer] stopped by user. total sent: {sent}")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()