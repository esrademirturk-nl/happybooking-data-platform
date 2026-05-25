# ============================================================
# 03_bronze_api_ingest.py
# Goal: Fetch Weather + Currency API data, write to Bronze
# Layer: Bronze
# Author: Esra Demirturk Duman
# ============================================================

# Neden API verisi?
# HappyBooking için hava durumu ve döviz kuru önemli:
# - Hava durumu: hangi şehirlerde rezervasyon artıyor?
# - Döviz kuru: farklı para birimlerinde fiyat analizi

import requests
import json
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime, timedelta

# --------------------------------------------------
# 1. Get unique cities from Bronze batch data
# --------------------------------------------------
# Neden: Hangi şehirlerin hava durumunu çekeceğimizi
# Bronze'daki gerçek veriden alıyoruz

df_bronze = spark.read.format("delta").table("raw_bookings_batch")
cities = df_bronze.select("city").distinct().limit(10).collect()
city_list = [row["city"] for row in cities if row["city"] is not None]
print(f"✅ Cities found: {city_list}")

# --------------------------------------------------
# 2. Fetch Weather Data (Open-Meteo API)
# --------------------------------------------------
# Neden Open-Meteo: Ücretsiz, API key gerektirmez
# Gerçek hayatta paid API kullanılır (WeatherAPI, OpenWeather)

# City coordinates (en yaygın şehirler için sabit koordinatlar)
city_coords = {
    "Amsterdam": (52.3676, 4.9041),
    "Paris": (48.8566, 2.3522),
    "London": (51.5074, -0.1278),
    "Berlin": (52.5200, 13.4050),
    "Rome": (41.9028, 12.4964),
    "Madrid": (40.4168, -3.7038),
    "Barcelona": (41.3851, 2.1734),
    "Vienna": (48.2082, 16.3738),
    "Prague": (50.0755, 14.4378),
    "Istanbul": (41.0082, 28.9784)
}

weather_records = []
today = datetime.now().strftime("%Y-%m-%d")
week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

for city in city_list[:5]:  # İlk 5 şehir (API limitini korumak için)
    coords = city_coords.get(city)
    if not coords:
        print(f"⚠️ No coordinates for {city}, skipping...")
        continue
    
    lat, lon = coords
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=Europe/Amsterdam"
        f"&past_days=7"
    )
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        
        for i, date in enumerate(dates):
            weather_records.append({
                "city": city,
                "date": date,
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "precipitation": daily.get("precipitation_sum", [None])[i],
                "source": "open-meteo",
                "ingestion_timestamp": datetime.now().isoformat()
            })
        print(f"✅ Weather fetched for {city}: {len(dates)} days")
    except Exception as e:
        print(f"❌ Weather API error for {city}: {e}")

print(f"✅ Total weather records: {len(weather_records)}")

# --------------------------------------------------
# 3. Fetch Currency Data (ExchangeRate API)
# --------------------------------------------------
# Neden: Farklı para birimlerinde fiyat analizi için
# ECB (European Central Bank) API — ücretsiz

currency_records = []

try:
    url = "https://api.exchangerate-api.com/v4/latest/EUR"
    response = requests.get(url, timeout=10)
    data = response.json()
    
    target_currencies = ["USD", "GBP", "TRY", "JPY", "AUD", "CAD", "CHF"]
    
    for currency in target_currencies:
        rate = data.get("rates", {}).get(currency)
        if rate:
            currency_records.append({
                "base_currency": "EUR",
                "target_currency": currency,
                "rate": rate,
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source": "exchangerate-api",
                "ingestion_timestamp": datetime.now().isoformat()
            })
    print(f"✅ Currency records fetched: {len(currency_records)}")
except Exception as e:
    print(f"❌ Currency API error: {e}")

# --------------------------------------------------
# 4. Write Weather Data to Bronze
# --------------------------------------------------
# Neden overwrite: Her gün taze veri çekiyoruz
# Partition by date: Büyük veri setlerinde sorgu hızı için

if weather_records:
    df_weather = spark.createDataFrame(weather_records)
    df_weather = df_weather.withColumn("ingestion_timestamp", current_timestamp())
    
    df_weather.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_weather_data")
    
    print("✅ Bronze weather table written: bronze_weather_data")
    df_weather.show(5)
else:
    print("⚠️ No weather data to write")

# --------------------------------------------------
# 5. Write Currency Data to Bronze
# --------------------------------------------------
if currency_records:
    df_currency = spark.createDataFrame(currency_records)
    df_currency = df_currency.withColumn("ingestion_timestamp", current_timestamp())
    
    df_currency.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_currency_data")
    
    print("✅ Bronze currency table written: bronze_currency_data")
    df_currency.show()
else:
    print("⚠️ No currency data to write")

print("\n🎉 Bronze API ingestion complete!")
print("Tables created: bronze_weather_data, bronze_currency_data")
