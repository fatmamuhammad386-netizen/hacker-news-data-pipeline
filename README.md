# 🚀 Hacker News Analytics Data Lakehouse & Executive Dashboard

An end-to-end Data Engineering pipeline built to ingest, process, and analyze Hacker News data. The pipeline aggregates trends, top tech keywords, author dynamics, and domain engagement metrics into a multi-layered Data Lake, surfaced through a minimalist Executive Power BI Dashboard.

---

## 📌 Architecture & Data Pipeline Flow
[ Hacker News Firebase API ]
│
▼ (Daily Python Ingestion)
[ Data Lake: Raw Layer (JSON) ]
│
▼ (PySpark Transformation & Schema Validation)
[ Data Lake: Processed Layer (Parquet Partitioned by Date) ]
│
▼ (PySpark Analytics & Aggregations)
[ Data Lake: Analytics Layer (Aggregated Parquet Tables) ]
│
▼ (Direct Import / Orchestration)
[ Executive Power BI Dashboard ]

1. **Ingestion Layer:** Daily extraction of top stories and nested comment metadata.
2. **Raw Layer:** JSON format stored in `/opt/airflow/data-lake/raw/dt=YYYY-MM-DD/`.
3. **Processed Layer:** Parquet format partitioned by execution date (`dt=YYYY-MM-DD`) after cleaning and schema mapping.
4. **Analytics Layer:** Aggregated tables (`domain_scatter`, `stories_by_hour`, `author_analytics`, etc.) ready for BI consumption.
5. **Orchestration:** Scheduled and managed via Apache Airflow in Docker containers.

---

## 🔌 APIs & Data Sources Used

This project consumes the official **Hacker News Firebase REST API**:

* 📌 **Top Stories API:**  
  `https://hacker-news.firebaseio.com/v0/topstories.json`  
  *Retrieves the list of current top story IDs.*

* 📌 **Item Details API (Stories & Comments):**  
  `https://hacker-news.firebaseio.com/v0/item/{item_id}.json`  
  *Fetches individual item metadata including title, author, URL, score, time, and comment count.*

* 📄 **Official API Documentation:**  
  [Hacker News API Documentation on GitHub](https://github.com/HackerNews/API)

---

## 🛠️ Tech Stack & Tools

* **Orchestration:** Apache Airflow
* **Batch Processing & ETL:** PySpark (Apache Spark 3.x)
* **Containerization:** Docker & Docker Compose
* **Storage Layer:** Parquet Data Lake (Partitioned Architecture)
* **Visualization:** Microsoft Power BI
* **Language:** Python 3.x

---

## 📊 Analytics & BI Features

* **Domain Engagement (Scatter Plot):** Correlation between `total_score` and `total_comments` across publishing domains (e.g., `github.com`, `nytimes.com`).
* **Hourly Engagement Trends:** Identifying peak submission and discussion windows using `stories_by_hour`.
* **Keyword & Topic Categorization:** Tech trend analysis based on story title tokenization.
* **Top Authors Performance:** Author contribution and engagement benchmarking.

---

## ⚙️ Setup & Execution

### 1. Prerequisites
* Docker & Docker Compose
* Power BI Desktop

### 2. Running Airflow Pipeline
```bash
# Clone the repository
git clone [https://github.com/YOUR_USERNAME/hacker-news-data-pipeline.git](https://github.com/YOUR_USERNAME/hacker-news-data-pipeline.git)
cd hacker-news-data-pipeline

# Start Docker containers
docker-compose up -d
