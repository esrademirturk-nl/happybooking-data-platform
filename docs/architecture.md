# 🏗️ HappyBooking Data Platform — Architecture

## Overview

HappyBooking Data Platform is a production-grade Modern Data Engineering
solution built on Microsoft Fabric, implementing the Medallion Architecture
(Bronze → Silver → Gold) with batch, streaming, and API data sources.

---

## Architecture Diagram
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
├─────────────────┬───────────────────┬───────────────────────┤
│   BATCH         │   STREAMING       │   API                 │
│ booking_dirty   │ Docker Producer   │ Open-Meteo (Weather)  │
│ .csv (Kaggle)   │ stream_producer   │ ExchangeRate (FX)     │
└────────┬────────┴────────┬──────────┴──────────┬────────────┘
```
│                 │                      │
▼                 ▼                      ▼
```
┌─────────────────────────────────────────────────────────────┐
│              BRONZE LAYER (happybooking_bronze_lh)          │
│                                                             │
│  raw_bookings_batch    bronze_stream_events                 │
│  raw_bookings_stream   bronze_weather_data                  │
│                        bronze_currency_data                 │
│                                                             │
│  ✓ Raw data — no transformations                           │
│  ✓ Audit columns: ingestion_timestamp, source_file         │
│  ✓ Delta format — versioned, ACID compliant                │
└─────────────────────────┬───────────────────────────────────┘
```
│
▼ PySpark (04_silver_transformations)
```
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER (happybooking_silver_lh)          │
│                                                             │
│  silver_hotels       silver_customers                       │
│  silver_bookings     silver_reviews                         │
│                                                             │
│  ✓ Nulls removed, duplicates dropped                       │
│  ✓ Data types fixed, dates standardized                    │
│  ✓ Entities separated (Hotel, Customer, Booking, Review)   │
│  ✓ Great Expectations quality validation                   │
└─────────────────────────┬───────────────────────────────────┘
```
│
▼ DBT (dbt_project/)
```
┌─────────────────────────────────────────────────────────────┐
│              GOLD LAYER (happybooking_warehouse)            │
│                                                             │
│  Staging (Views)         Marts (Tables)                     │
│  ─────────────────       ────────────────────              │
│  stg_bookings            fact_booking                       │
│  stg_hotels              dim_hotel                          │
│  stg_customers           dim_customer                       │
│  stg_reviews             kpi_revenue                        │
│                                                             │
│  ✓ Star schema — optimized for analytics                   │
│  ✓ Pre-calculated KPIs and revenue metrics                 │
│  ✓ DBT tests: unique, not_null, relationships              │
└─────────────────────────┬───────────────────────────────────┘
```
│
▼
┌─────────────────────────────────────────────────────────────┐
│                    POWER BI DASHBOARD                       │
│                                                             │
│  📊 Booking Trends        💰 Revenue KPIs                  │
│  🏨 Hotel Performance     👥 Customer Segments             │
│  🌍 City Analysis         ⭐ Review Analytics              │
└─────────────────────────────────────────────────────────────┘
```
---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | PySpark (Fabric Notebook) | Batch + API ingestion |
| Streaming | Docker + Python | Stream simulation |
| Storage | Microsoft Fabric Lakehouse | Delta Lake storage |
| Transformation | PySpark + DBT | Silver + Gold models |
| Quality | Great Expectations + Pytest | Data validation |
| Orchestration | Fabric Workflow | Pipeline automation |
| BI | Power BI (Direct Lake) | Analytics dashboard |
| CI/CD | GitHub Actions | Automated testing + deploy |
| Version Control | GitHub | Code management |

---

## Data Flow

### Batch Flow
booking_dirty.csv
```
→ 01_bronze_batch_ingest.py    (Bronze: raw_bookings_batch)
→ 04_silver_transformations.py (Silver: 4 entity tables)
→ DBT staging models           (Gold: views)
→ DBT mart models              (Gold: fact + dim tables)
→ Power BI Dashboard
```

### Streaming Flow
hotel_raw_stream.csv
```
→ Docker stream_producer.py    (sends events)
→ 02_bronze_stream_simulator.py(Bronze: bronze_stream_events)
→ 04_silver_transformations.py (Silver: merged with batch)
→ DBT models                   (Gold: included in fact_booking)
```
### API Flow
Open-Meteo API + ExchangeRate API
```
→ 03_bronze_api_ingest.py      (Bronze: weather + currency)
→ Used in Silver enrichment
→ Available for Power BI analysis
```
---

## Repository Structure
```
happybooking-data-platform/
├── data/                          # Raw data files
│   └── booking_dirty.csv
├── docker/                        # Stream simulator
│   ├── Dockerfile
│   ├── stream_producer.py
│   └── requirements.txt
├── notebooks/                     # Fabric PySpark notebooks
│   ├── 01_bronze_batch_ingest.py
│   ├── 02_bronze_stream_simulator.py
│   ├── 03_bronze_api_ingest.py
│   ├── 04_silver_transformations.py
│   └── 05_quality_checks_ge.py
├── dbt_project/                   # DBT Gold models
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_bookings.sql
│   │   │   ├── stg_hotels.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_reviews.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── fact_booking.sql
│   │       ├── dim_hotel.sql
│   │       ├── dim_customer.sql
│   │       └── kpi_revenue.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/                         # Pytest unit tests
│   └── test_quality.py
├── .github/
│   └── workflows/
│       └── ci.yml                 # CI/CD pipeline
├── docs/
│   └── architecture.md
└── README.md
```
---

## Branch Strategy
```
main     → Production (merge only via PR)
dev      → Development (active work branch)
feature/ → Individual features
```
## CI/CD Pipeline
```
PR opened to main
↓
┌─────────────────────────────┐
│  Job 1: DBT Tests           │ → dbt test
│  Job 2: GE Tests            │ → pytest tests/
│  Job 3: Pytest              │ → unit tests
└─────────────────────────────┘
```
↓ (all pass)
```
Merge to main
↓
┌─────────────────────────────┐
│  Job 4: DBT Build Prod      │ → dbt build --target prod
└─────────────────────────────┘
```
---

## Author

**Esra Demirturk Duman**
Data Engineer | Microsoft Fabric | Azure Data Engineering
Rotterdam, Netherlands
