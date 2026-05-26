# ============================================================
# stream_producer.py
# Goal: Send hotel booking events to Fabric Eventstream via Kafka
# Author: Esra Demirturk Duman
# ============================================================

import csv
import json
import time
import os
from datetime import datetime
from confluent_kafka import Producer

# --------------------------------------------------
# 1. Kafka Configuration
# --------------------------------------------------
# Neden Kafka protokolü?
# Fabric Eventstream Kafka protokolünü destekler
# Bu sayede Docker direkt Fabric'e event gönderebilir
# Araya ayrı bir Kafka kurulumuna gerek yok

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER", "")
TOPIC = os.getenv("TOPIC", "")
CONNECTION_STR = os.getenv("CONNECTION_STR", "")
STREAM_FILE = os.getenv("STREAM_FILE", "booking_dirty.csv")
DELAY_SECONDS = float(os.getenv("DELAY_SECONDS", "0.1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# Kafka producer config
conf = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": CONNECTION_STR,
    "client.id": "happybooking-producer"
}

producer = Producer(conf)

print("=" * 60)
print("🚀 HappyBooking Stream Producer Starting...")
print(f"   Server: {BOOTSTRAP_SERVER}")
print(f"   Topic: {TOPIC}")
print(f"   File: {STREAM_FILE}")
print(f"   Delay: {DELAY_SECONDS}s")
print("=" * 60)

# --------------------------------------------------
# 2. Delivery callback
# --------------------------------------------------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

# --------------------------------------------------
# 3. Send events
# --------------------------------------------------
def process_stream():
    if not os.path.exists(STREAM_FILE):
        print(f"❌ File not found: {STREAM_FILE}")
        return

    total_sent = 0
    start_time = datetime.now()

    with open(STREAM_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row_num, row in enumerate(reader, 1):
            event = {
                **row,
                "event_id": f"EVT_{row_num:08d}",
                "event_timestamp": datetime.now().isoformat(),
                "stream_source": "docker_kafka_producer",
                "sequence_number": row_num
            }

            producer.produce(
                topic=TOPIC,
                value=json.dumps(event, default=str).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            total_sent += 1

            if row_num % 100 == 0:
                producer.flush()
                elapsed = (datetime.now() - start_time).seconds
                print(f"📊 Sent: {total_sent} events | Elapsed: {elapsed}s")

            time.sleep(DELAY_SECONDS)

    producer.flush()
    elapsed = (datetime.now() - start_time).seconds
    print("\n" + "=" * 60)
    print("✅ Stream Producer Complete!")
    print(f"   Total sent: {total_sent}")
    print(f"   Total time: {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    process_stream()
