# ============================================================
# 02_bronze_stream_simulator.py
# Goal: Simulate real-time streaming by reading stream CSV
#       row by row and writing to Bronze as Delta
# Layer: Bronze
# ============================================================

# Neden bu notebook var?
# Gerçek hayatta Docker + Kafka/Eventstream ile streaming yapılır.
# Biz trial ortamında bunu Python ile simüle ediyoruz.
# Her satır = bir otel rezervasyon eventi gibi işleniyor.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType
import time

# --------------------------------------------------
# 1. Read stream simulation data from Bronze batch
# --------------------------------------------------
# Neden: raw_bookings_stream tablosu zaten 01'de oluşturuldu
# Biz onu sanki gerçek zamanlı geliyormuş gibi işleyeceğiz

df_stream = spark.read \
    .format("delta") \
    .table("raw_bookings_stream")

print(f"✅ Stream simulation rows: {df_stream.count()}")

# --------------------------------------------------
# 2. Add streaming metadata columns
# --------------------------------------------------
# Neden: Streaming verisini batch'ten ayırt etmek için
# event_time = verinin "geldiği" zaman
# stream_source = nereden geldiğini belirtir

df_stream = df_stream \
    .withColumn("event_time", current_timestamp()) \
    .withColumn("stream_source", lit("docker_stream_simulator")) \
    .withColumn("processing_type", lit("streaming"))

# --------------------------------------------------
# 3. Write to Bronze streaming table
# --------------------------------------------------
# append mode: streaming veri üstüne yazılır, silinmez
# Neden append: gerçek streaming'de her event eklenir, overwrite olmaz

df_stream.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("bronze_stream_events")

print("✅ Bronze streaming table written: bronze_stream_events")
print(f"✅ Total events processed: {df_stream.count()}")

# --------------------------------------------------
# 4. Verify
# --------------------------------------------------
df_verify = spark.read.format("delta").table("bronze_stream_events")
print(f"✅ Total records in bronze_stream_events: {df_verify.count()}")
df_verify.show(5)
