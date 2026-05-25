# ============================================================
# 05_quality_checks_ge.py
# Goal: Validate Silver data quality using Great Expectations
# Layer: Silver (validation)
# Author: Esra Demirturk Duman
# ============================================================

# Neden Great Expectations (GE)?
# Silver'a geçen veri temiz mi? Bunu otomatik test etmek için.
# GE ile "expectation" tanımlarsın: "bu kolon NULL olamaz" gibi.
# Her çalıştırmada rapor üretir — CI/CD'de de kullanılır.

# --------------------------------------------------
# 1. Install Great Expectations
# --------------------------------------------------
%pip install great-expectations==0.18.19 --quiet

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import json
from datetime import datetime
from pyspark.sql.functions import current_timestamp

print("✅ Great Expectations loaded:", gx.__version__)

# --------------------------------------------------
# 2. Load Silver Tables
# --------------------------------------------------
# Neden: Her Silver tablosunu ayrı ayrı test edeceğiz

df_bookings = spark.read.format("delta") \
    .table("happybooking_silver_lh.silver_bookings")

df_customers = spark.read.format("delta") \
    .table("happybooking_silver_lh.silver_customers")

df_hotels = spark.read.format("delta") \
    .table("happybooking_silver_lh.silver_hotels")

df_reviews = spark.read.format("delta") \
    .table("happybooking_silver_lh.silver_reviews")

print(f"✅ Bookings: {df_bookings.count()} rows")
print(f"✅ Customers: {df_customers.count()} rows")
print(f"✅ Hotels: {df_hotels.count()} rows")
print(f"✅ Reviews: {df_reviews.count()} rows")

# --------------------------------------------------
# 3. Convert to Pandas for GE
# --------------------------------------------------
# Neden Pandas: GE Spark'ı doğrudan destekler ama
# Pandas ile daha basit ve hızlı çalışır küçük/orta veri için

pdf_bookings = df_bookings.toPandas()
pdf_customers = df_customers.toPandas()
pdf_hotels = df_hotels.toPandas()
pdf_reviews = df_reviews.toPandas()

# --------------------------------------------------
# 4. Initialize GE Context
# --------------------------------------------------
context = gx.get_context()

# --------------------------------------------------
# 5. Quality Tests — Bookings
# --------------------------------------------------
print("\n🔍 Testing: silver_bookings")

ds_bookings = context.sources.add_pandas("bookings_source")
da_bookings = ds_bookings.add_dataframe_asset("bookings_asset")
batch_bookings = da_bookings.build_batch_request(dataframe=pdf_bookings)
suite_bookings = context.add_or_update_expectation_suite("bookings_suite")
validator_bookings = context.get_validator(
    batch_request=batch_bookings,
    expectation_suite=suite_bookings
)

# Not null checks
# Neden: Bu kolonlar olmadan booking analizi yapılamaz
validator_bookings.expect_column_values_to_not_be_null("booking_id")
validator_bookings.expect_column_values_to_not_be_null("hotel_id")
validator_bookings.expect_column_values_to_not_be_null("customer_id")
validator_bookings.expect_column_values_to_not_be_null("checkin_date")
validator_bookings.expect_column_values_to_not_be_null("checkout_date")
validator_bookings.expect_column_values_to_not_be_null("total_price")

# Unique checks
# Neden: Her booking_id benzersiz olmalı
validator_bookings.expect_column_values_to_be_unique("booking_id")

# Value range checks
# Neden: Negatif fiyat veya 0 geceleme iş mantığına aykırı
validator_bookings.expect_column_values_to_be_between(
    "total_price", min_value=0
)
validator_bookings.expect_column_values_to_be_between(
    "nights", min_value=1, max_value=365
)

# Accepted values
validator_bookings.expect_column_values_to_be_in_set(
    "is_cancelled", [True, False]
)

results_bookings = validator_bookings.validate()
print(f"✅ Bookings tests: {results_bookings['statistics']['successful_expectations']}"
      f"/{results_bookings['statistics']['evaluated_expectations']} passed")

# --------------------------------------------------
# 6. Quality Tests — Customers
# --------------------------------------------------
print("\n🔍 Testing: silver_customers")

ds_customers = context.sources.add_pandas("customers_source")
da_customers = ds_customers.add_dataframe_asset("customers_asset")
batch_customers = da_customers.build_batch_request(dataframe=pdf_customers)
suite_customers = context.add_or_update_expectation_suite("customers_suite")
validator_customers = context.get_validator(
    batch_request=batch_customers,
    expectation_suite=suite_customers
)

validator_customers.expect_column_values_to_not_be_null("customer_id")
validator_customers.expect_column_values_to_not_be_null("email")
validator_customers.expect_column_values_to_be_unique("customer_id")
validator_customers.expect_column_values_to_be_unique("email")
validator_customers.expect_column_values_to_be_in_set(
    "gender", ["Male", "Female", "Other"]
)
validator_customers.expect_column_values_to_match_regex(
    "email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)

results_customers = validator_customers.validate()
print(f"✅ Customers tests: {results_customers['statistics']['successful_expectations']}"
      f"/{results_customers['statistics']['evaluated_expectations']} passed")

# --------------------------------------------------
# 7. Quality Tests — Hotels
# --------------------------------------------------
print("\n🔍 Testing: silver_hotels")

ds_hotels = context.sources.add_pandas("hotels_source")
da_hotels = ds_hotels.add_dataframe_asset("hotels_asset")
batch_hotels = da_hotels.build_batch_request(dataframe=pdf_hotels)
suite_hotels = context.add_or_update_expectation_suite("hotels_suite")
validator_hotels = context.get_validator(
    batch_request=batch_hotels,
    expectation_suite=suite_hotels
)

validator_hotels.expect_column_values_to_not_be_null("hotel_id")
validator_hotels.expect_column_values_to_not_be_null("hotel_name")
validator_hotels.expect_column_values_to_be_unique("hotel_id")
validator_hotels.expect_column_values_to_be_between(
    "star_rating", min_value=1, max_value=5
)
validator_hotels.expect_column_values_to_be_between(
    "total_rooms", min_value=1, max_value=10000
)

results_hotels = validator_hotels.validate()
print(f"✅ Hotels tests: {results_hotels['statistics']['successful_expectations']}"
      f"/{results_hotels['statistics']['evaluated_expectations']} passed")

# --------------------------------------------------
# 8. Quality Tests — Reviews
# --------------------------------------------------
print("\n🔍 Testing: silver_reviews")

ds_reviews = context.sources.add_pandas("reviews_source")
da_reviews = ds_reviews.add_dataframe_asset("reviews_asset")
batch_reviews = da_reviews.build_batch_request(dataframe=pdf_reviews)
suite_reviews = context.add_or_update_expectation_suite("reviews_suite")
validator_reviews = context.get_validator(
    batch_request=batch_reviews,
    expectation_suite=suite_reviews
)

validator_reviews.expect_column_values_to_not_be_null("review_id")
validator_reviews.expect_column_values_to_not_be_null("review_rating")
validator_reviews.expect_column_values_to_be_unique("review_id")
validator_reviews.expect_column_values_to_be_between(
    "review_rating", min_value=1, max_value=10
)
validator_reviews.expect_column_values_to_be_in_set(
    "is_verified_review", [True, False]
)

results_reviews = validator_reviews.validate()
print(f"✅ Reviews tests: {results_reviews['statistics']['successful_expectations']}"
      f"/{results_reviews['statistics']['evaluated_exceptions']} passed")

# --------------------------------------------------
# 9. Generate Summary Report
# --------------------------------------------------
# Neden: CI/CD'de artifact olarak saklanır, proje tesliminde gösterilir

print("\n" + "=" * 60)
print("📊 GREAT EXPECTATIONS QUALITY REPORT")
print("=" * 60)
print(f"  Run date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Bookings:  {results_bookings['statistics']['successful_expectations']}"
      f"/{results_bookings['statistics']['evaluated_expectations']} ✅")
print(f"  Customers: {results_customers['statistics']['successful_expectations']}"
      f"/{results_customers['statistics']['evaluated_expectations']} ✅")
print(f"  Hotels:    {results_hotels['statistics']['successful_expectations']}"
      f"/{results_hotels['statistics']['evaluated_expectations']} ✅")
print(f"  Reviews:   {results_reviews['statistics']['successful_expectations']}"
      f"/{results_reviews['statistics']['evaluated_expectations']} ✅")
print("=" * 60)

# Save report as JSON to Lakehouse
# Neden: CI/CD artifact olarak saklanır
report = {
    "run_date": datetime.now().isoformat(),
    "bookings": results_bookings["statistics"],
    "customers": results_customers["statistics"],
    "hotels": results_hotels["statistics"],
    "reviews": results_reviews["statistics"]
}

report_json = json.dumps(report, indent=2)
dbutils.fs.put(
    "Files/ge_reports/quality_report.json",
    report_json,
    overwrite=True
)

print("\n✅ Quality report saved: Files/ge_reports/quality_report.json")
print("\n🎉 All quality checks complete!")
