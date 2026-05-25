# 🏨 HappyBooking Data Platform

A production-grade Modern Data Engineering project built on Microsoft Fabric.

## 🏗️ Architecture

Bronze (Raw) → Silver (Cleaned) → Gold (Business Models) → Power BI

## 🛠️ Tech Stack

- **Microsoft Fabric** — Lakehouse, Warehouse, Eventstream, Notebook, Workflow
- **PySpark** — Data transformations
- **DBT** — Gold layer business models
- **Great Expectations** — Data quality checks
- **Docker** — Stream simulator
- **GitHub Actions** — CI/CD pipeline

## 📦 Data Sources

- **Batch** — Hotel booking CSV (Kaggle)
- **Streaming** — Docker stream simulator → Fabric Eventstream
- **API** — Open-Meteo (weather) + ExchangeRate (currency)

## 🗂️ Repository Structure
```
happybooking-data-platform/
├── data/               # Raw data files
├── docker/             # Stream simulator
├── notebooks/          # Fabric PySpark notebooks
├── dbt_project/        # DBT Gold models
├── tests/              # Data quality tests
├── .github/workflows/  # CI/CD pipelines
└── docs/               # Architecture diagrams
```
## 👩‍💻 Author

Esra Demirturk Duman — Data Engineer
