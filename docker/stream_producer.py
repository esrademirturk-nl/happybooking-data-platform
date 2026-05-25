# ============================================================
# stream_producer.py
# Goal: Simulate real-time hotel booking events
#       by sending CSV rows one-by-one to Fabric Eventstream
# Author: Esra Demirturk Duman
# ============================================================

# Neden Docker + stream_producer?
# Gerçek hayatta rezervasyon sistemleri anlık event gönderir.
# Biz bunu simüle ediyoruz:
# - CSV'den satır satır okuyoruz
# - Her satırı JSON event olarak gönderiyoruz
# - time.sleep ile gerçek zamanlı gibi davranıyoruz

import csv
import json
import time
import os
import requests
from datetime import datetime

# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------
# Neden env_var: Hassas bilgileri kod içine yazmıyoruz
# Docker run sırasında -e ile geçilir

STREAM_FILE = os.getenv("STREAM_FILE", "hotel_raw_stream.csv")
EVENTSTREAM_URL = os.getenv("EVENTSTREAM_URL", "")
DELAY_SECONDS = float(os.getenv("DELAY_SECONDS", "0.5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))

print("=" * 60)
print("🚀 HappyBooking Stream Producer Starting...")
print(f"   File: {STREAM_FILE}")
print(f"   Delay: {DELAY_SECONDS}s between events")
print(f"   Batch size: {BATCH_SIZE}")
print("=" * 60)

# --------------------------------------------------
# 2. Read CSV and send events
# --------------------------------------------------

def send_event(event: dict, url: str) -> bool:
    """
    Send a single event to Fabric Eventstream.
    
    Neden requests: Eventstream HTTP endpoint ile çalışır.
    Her event JSON formatında POST edilir.
    """
    if not url:
        # URL yoksa sadece print et (test modu)
        print(f"[TEST MODE] Event: {json.dumps(event, default=str)[:100]}...")
        return True
    
    try:
        response = requests.post(
            url,
            json=event,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        print(f"❌ Error sending event: {e}")
        return False


def process_stream():
    """
    Main streaming loop.
    Reads CSV row by row, sends as JSON events.
    """
    if not os.path.exists(STREAM_FILE):
        print(f"❌ Stream file not found: {STREAM_FILE}")
        return

    total_sent = 0
    total_failed = 0
    start_time = datetime.now()

    with open(STREAM_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        
        batch = []
        
        for row_num, row in enumerate(reader, 1):
            # Add streaming metadata
            # Neden: Her event'in ne zaman geldiğini bilmek istiyoruz
            event = {
                **row,
                "event_id": f"EVT_{row_num:08d}",
                "event_timestamp": datetime.now().isoformat(),
                "stream_source": "docker_producer",
                "sequence_number": row_num
            }
            
            batch.append(event)
            
            # Send batch
            if len(batch) >= BATCH_SIZE:
                for e in batch:
                    success = send_event(e, EVENTSTREAM_URL)
                    if success:
                        total_sent += 1
                    else:
                        total_failed += 1
                
                batch = []
                
                # Progress report every 100 events
                if row_num % 100 == 0:
                    elapsed = (datetime.now() - start_time).seconds
                    print(f"📊 Progress: {row_num} rows | "
                          f"Sent: {total_sent} | "
                          f"Failed: {total_failed} | "
                          f"Elapsed: {elapsed}s")
                
                # Simulate real-time delay
                # Neden: Gerçek streaming'de eventler anlık gelir
                time.sleep(DELAY_SECONDS)
        
        # Send remaining events in batch
        for e in batch:
            success = send_event(e, EVENTSTREAM_URL)
            if success:
                total_sent += 1
            else:
                total_failed += 1

    # Final summary
    elapsed = (datetime.now() - start_time).seconds
    print("\n" + "=" * 60)
    print("✅ Stream Producer Complete!")
    print(f"   Total sent:   {total_sent}")
    print(f"   Total failed: {total_failed}")
    print(f"   Total time:   {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    process_stream()
