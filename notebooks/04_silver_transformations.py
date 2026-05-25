# ============================================================
# 04_silver_transformations.py
# Goal: Clean and normalize Bronze data, split into entities,
#       write to Silver Lakehouse
# Layer: Silver
# Author: Esra Demirturk Duman
# ============================================================

# Neden Silver katman?
# Bronze'da veri ham ve kirli geldi.
# Silver'da:
# - NULL değerler temizlenir
# - Duplicate'ler kaldırılır
# - Veri tipleri düzeltilir
# - Tarih formatları standardize edilir
# - 4 entity'e ayrılır: hotels, customers, bookings, reviews

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, to_timestamp,
    when, regexp_replace, current_timestamp, lit,
    round as spark_round, abs as spark_abs
)
from pyspark.sql.types import IntegerType, DoubleType, DateType

# --------------------------------------------------
# 1. Read from Bronze
# --------------------------------------------------
# Neden: Silver her zaman Bronze'dan okur, CSV'den değil
# Bu sayede Bronze → Silver pipeline temiz kalır

df_bronze = spark.read.format("delta").table("raw_bookings_batch")
print(f"✅ Bronze rows loaded: {df_bronze.count()}")

# --------------------------------------------------
# 2. Initial Cleaning (tüm veri için)
# --------------------------------------------------

# 2a. Remove exact duplicates
# Neden: Aynı booking_id ile birden fazla kayıt olabilir
df_clean = df_bronze.dropDuplicates(["booking_id"])
print(f"✅ After dedup: {df_clean.count()} rows")

# 2b. Trim whitespace from string columns
# Neden: "Amsterdam " ve "Amsterdam" farklı değer sayılır
string_cols = [f.name for f in df_clean.schema.fields 
               if str(f.dataType) == "StringType()"]

for c in string_cols:
    df_clean = df_clean.withColumn(c, trim(col(c)))

# 2c. Replace empty strings with NULL
# Neden: "" ve NULL aynı şey ama analitik araçlar farklı işler
for c in string_cols:
    df_clean = df_clean.withColumn(
        c, when(col(c) == "", None).otherwise(col(c))
    )

print("✅ String cleaning done")

# --------------------------------------------------
# 3. Silver Hotels Table
# --------------------------------------------------
# Neden ayrı tablo: Otel bilgisi değişmez, tekrar tekrar yazma

hotel_cols = [
    "hotel_id", "hotel_name", "country", "city",
    "hotel_type", "star_rating", "total_rooms",
    "hotel_facilities", "hotel_description",
    "nearby_attractions", "latitude", "longitude", "website"
]

df_hotels = df_clean.select(hotel_cols) \
    .dropDuplicates(["hotel_id"]) \
    .filter(col("hotel_id").isNotNull()) \
    .withColumn("hotel_name", 
                when(col("hotel_name").isNull(), "Unknown")
                .otherwise(col("hotel_name"))) \
    .withColumn("star_rating",
                when((col("star_rating") < 1) | (col("star_rating") > 5), None)
                .otherwise(col("star_rating"))) \
    .withColumn("country", upper(col("country"))) \
    .withColumn("silver_updated_at", current_timestamp())

print(f"✅ Hotels: {df_hotels.count()} rows")

# Silver Lakehouse'a yaz
# Neden farklı Lakehouse: Her katman izole, bağımsız
spark.conf.set(
    "spark.hadoop.fs.defaultFS",
    "abfss://happybooking_silver_lh@onelake.dfs.fabric.microsoft.com"
)

df_hotels.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("happybooking_silver_lh.silver_hotels")

print("✅ Silver hotels written")

# --------------------------------------------------
# 4. Silver Customers Table
# --------------------------------------------------
# Neden ayrı tablo: GDPR — kişisel veri ayrı yönetilmeli

customer_cols = [
    "customer_id", "first_name", "last_name", "full_name",
    "email", "phone", "birth_date", "gender",
    "country_customer", "city_customer", "address",
    "postal_code", "language_preference", "loyalty_level_customer",
    "registration_date", "marketing_consent"
]

df_customers = df_clean.select(customer_cols) \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("email").isNotNull()) \
    .withColumn("email", lower(col("email"))) \
    .withColumn("gender",
                when(col("gender").isin("M", "Male", "male"), "Male")
                .when(col("gender").isin("F", "Female", "female"), "Female")
                .otherwise("Other")) \
    .withColumn("birth_date", to_date(col("birth_date"))) \
    .withColumn("registration_date", to_date(col("registration_date"))) \
    .withColumn("loyalty_level_customer",
                when(col("loyalty_level_customer").isNull(), "Bronze")
                .otherwise(col("loyalty_level_customer"))) \
    .withColumn("silver_updated_at", current_timestamp())

print(f"✅ Customers: {df_customers.count()} rows")

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("happybooking_silver_lh.silver_customers")

print("✅ Silver customers written")

# --------------------------------------------------
# 5. Silver Bookings Table
# --------------------------------------------------
# Ana iş verisi — en kritik tablo

booking_cols = [
    "booking_id", "hotel_id", "customer_id",
    "booking_date", "checkin_date", "checkout_date",
    "nights", "adults", "children", "infants",
    "room_type", "rooms_booked", "booking_channel",
    "special_requests", "is_cancelled", "cancellation_date",
    "cancellation_reason", "total_price", "room_price",
    "tax_amount", "service_fee", "paid_amount",
    "payment_status", "payment_method", "booking_source",
    "promotion_code", "discount_amount", "booking_status"
]

df_bookings = df_clean.select(booking_cols) \
    .dropDuplicates(["booking_id"]) \
    .filter(col("booking_id").isNotNull()) \
    .filter(col("hotel_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("booking_date", to_date(col("booking_date"))) \
    .withColumn("checkin_date", to_date(col("checkin_date"))) \
    .withColumn("checkout_date", to_date(col("checkout_date"))) \
    .withColumn("cancellation_date", to_date(col("cancellation_date"))) \
    .withColumn("nights",
                when(col("nights") <= 0, None)
                .otherwise(col("nights").cast(IntegerType()))) \
    .withColumn("total_price",
                when(col("total_price") < 0, spark_abs(col("total_price")))
                .otherwise(col("total_price").cast(DoubleType()))) \
    .withColumn("is_cancelled",
                when(col("is_cancelled").isin("1", "true", "True", "YES"), True)
                .otherwise(False)) \
    .withColumn("booking_status",
                when(col("booking_status").isNull(), "Unknown")
                .otherwise(col("booking_status"))) \
    .withColumn("silver_updated_at", current_timestamp())

print(f"✅ Bookings: {df_bookings.count()} rows")

df_bookings.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("happybooking_silver_lh.silver_bookings")

print("✅ Silver bookings written")

# --------------------------------------------------
# 6. Silver Reviews Table
# --------------------------------------------------

review_cols = [
    "review_id", "booking_id", "hotel_id", "customer_id",
    "review_rating", "review_title", "review_text",
    "review_date", "helpful_votes", "is_verified_review",
    "reviewer_location", "stay_date", "trip_type",
    "room_type_reviewed"
]

df_reviews = df_clean.select(review_cols) \
    .dropDuplicates(["review_id"]) \
    .filter(col("review_id").isNotNull()) \
    .filter(col("review_rating").isNotNull()) \
    .withColumn("review_date", to_date(col("review_date"))) \
    .withColumn("stay_date", to_date(col("stay_date"))) \
    .withColumn("review_rating",
                when((col("review_rating") < 1) | (col("review_rating") > 10), None)
                .otherwise(col("review_rating").cast(DoubleType()))) \
    .withColumn("is_verified_review",
                when(col("is_verified_review").isin("1", "true", "True"), True)
                .otherwise(False)) \
    .withColumn("silver_updated_at", current_timestamp())

print(f"✅ Reviews: {df_reviews.count()} rows")

df_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("happybooking_silver_lh.silver_reviews")

print("✅ Silver reviews written")

# --------------------------------------------------
# 7. Summary
# --------------------------------------------------
print("\n🎉 Silver transformations complete!")
print("=" * 50)
print(f"  Hotels:    {df_hotels.count()} rows")
print(f"  Customers: {df_customers.count()} rows")
print(f"  Bookings:  {df_bookings.count()} rows")
print(f"  Reviews:   {df_reviews.count()} rows")
print("=" * 50)
