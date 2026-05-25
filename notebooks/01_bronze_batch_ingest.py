# ============================================================
# 01_bronze_batch_ingest.py
# Goal: Read raw CSV, split into batch/stream, write to Bronze
# Layer: Bronze
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# --------------------------------------------------
# 1. Read raw CSV from Lakehouse Files
# --------------------------------------------------
# Neden: Ham veriyi olduğu gibi okuyoruz, hiçbir şeye dokunmuyoruz
# Bronze'un altın kuralı: veriyi olduğu gibi sakla

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/booking_dirty.csv")

print(f"✅ Total rows loaded: {df_raw.count()}")
print(f"✅ Total columns: {len(df_raw.columns)}")
df_raw.printSchema()

# --------------------------------------------------
# 2. Add audit columns
# --------------------------------------------------
# Neden: Her Bronze kaydında ne zaman ve nereden geldiğini bilmek istiyoruz
# Gerçek hayatta compliance ve debugging için şart

df_raw = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("booking_dirty.csv")) \
    .withColumn("source_system", lit("kaggle_batch"))

# --------------------------------------------------
# 3. Split into batch (70%) and stream (30%)
# --------------------------------------------------
# Neden: Proje hem batch hem streaming simüle ediyor
# Batch = geçmiş veri, Stream = gerçek zamanlı gelen yeni rezervasyonlar

df_batch, df_stream = df_raw.randomSplit([0.7, 0.3], seed=42)

print(f"✅ Batch rows: {df_batch.count()}")
print(f"✅ Stream rows: {df_stream.count()}")

# --------------------------------------------------
# 4. Write to Bronze Lakehouse as Delta
# --------------------------------------------------
# Neden Delta: Versiyonlama, ACID transaction, büyük veri için optimize
# overwrite mode: her çalıştırmada temiz başlar (idempotent)

df_batch.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("raw_bookings_batch")

df_stream.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("raw_bookings_stream")

print("✅ Bronze batch table written: raw_bookings_batch")
print("✅ Bronze stream table written: raw_bookings_stream")
