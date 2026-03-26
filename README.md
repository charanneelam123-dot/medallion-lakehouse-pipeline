# medallion-lakehouse-pipeline

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-00ADD8?logo=databricks&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Unity_Catalog-FF3621?logo=databricks&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apacheairflow&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-ff69b4)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?logo=microsoftazure&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazonaws&logoColor=white)
![CI](https://img.shields.io/github/actions/workflow/status/your-org/medallion-lakehouse-pipeline/ci.yml?label=CI)

Production-grade **Medallion Lakehouse** pipeline for NYC Taxi trip data built on **Databricks + Delta Lake**, orchestrated with **Apache Airflow**, and quality-gated with **Great Expectations**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                     │
│   NYC TLC Yellow Taxi CSV (ADLS Gen2 / S3 / DBFS)                      │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  Auto Loader / COPY INTO
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (main.nyc_taxi.bronze_trips)                             │
│  • Raw schema preserved — no transformations                            │
│  • Auto Loader cloudFiles for incremental ingestion                     │
│  • COPY INTO for idempotent full reloads                                │
│  • Audit cols: _ingestion_timestamp, _source_file, _bronze_batch_id    │
│  • OPTIMIZE ZORDER BY pickup_datetime                                   │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  MERGE INTO (upsert on natural key)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (main.nyc_taxi.silver_trips)                             │
│  • Type casting: string → timestamp, correct numerics                   │
│  • Business-rule filtering (passenger_count, fare, distance, datetime)  │
│  • Window-based deduplication (vendor+pickup+pu_loc+do_loc)             │
│  • Derived columns: trip_duration_minutes, speed_mph, fare_per_mile,   │
│    tip_percentage, pickup_hour/dow/month/year, payment_type_desc        │
│  • Partitioned by (pickup_year, pickup_month)                           │
│  • Rejection-rate guard (fails pipeline if > 20%)                       │
│           │                                                             │
│           ▼  Great Expectations checkpoint                              │
│    ┌──────────────────────────────────┐                                 │
│    │  silver_trips_suite              │                                 │
│    │  • Row count gate                │                                 │
│    │  • Schema contract (34 columns)  │                                 │
│    │  • 10 null checks                │                                 │
│    │  • Type assertions               │                                 │
│    │  • Range / set validations       │                                 │
│    │  • Cross-column pair checks      │                                 │
│    └──────────────────────────────────┘                                 │
└─────────────────────────────┬───────────────────────────────────────────┘
                              │  MERGE INTO (per grain)
                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER                                                             │
│                                                                         │
│  gold_daily_summary        │ pickup_date × zone × payment_type         │
│  • 20+ KPIs per grain      │ revenue, trips, tips, speed, fare/mile     │
│                             │                                           │
│  gold_hourly_demand        │ pickup_date × hour × zone                 │
│  • Demand category         │ High / Medium / Low trip-count bands       │
│                             │                                           │
│  gold_driver_performance   │ vendor_id × pickup_date                   │
│  • Revenue/minute          │ credit-card %, zone diversity              │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  ORCHESTRATION  (Apache Airflow — daily @ 03:00 UTC)                   │
│                                                                         │
│  source_data_check ──▶ bronze_ingestion ──▶ bronze_validation           │
│                  └──▶ skip_pipeline ──┐        │                        │
│                                       │        ▼                        │
│                                       │  silver_transform               │
│                                       │        │                        │
│                                       │  silver_validation              │
│                                       │        │                        │
│                                       │  great_expectations_check       │
│                                       │        │                        │
│                                       │  gold_aggregate                 │
│                                       │        │                        │
│                                       │  pipeline_success               │
│                                       │        │                        │
│                                       └───────▶ end                     │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  CI / CD  (GitHub Actions on every PR → main / develop)                │
│                                                                         │
│  lint ──▶ unit-tests ──▶ great-expectations ──▶ ci-gate                │
│       └──▶ dag-lint ───────────────────────────────┘                   │
│       └──▶ security ───────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Databricks (DBR 14.x / Photon) |
| Storage format | Delta Lake 3.1 |
| Catalog | Unity Catalog |
| Language | Python 3.11 / PySpark 3.5 |
| Orchestration | Apache Airflow 2.8 + Databricks provider |
| Data quality | Great Expectations 0.18 |
| Transformation | dbt-databricks 1.7 (optional Gold layer) |
| Cloud storage | Azure ADLS Gen2 / AWS S3 |
| CI/CD | GitHub Actions |
| Linting | ruff + black + isort |
| Security scan | bandit |

---

## Project Structure

```
medallion-lakehouse-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py     # Auto Loader + COPY INTO ingestion
│   ├── 02_silver_transform.py     # Clean, cast, deduplicate, MERGE
│   └── 03_gold_aggregate.py       # Business aggregations, 3 Gold tables
├── dags/
│   └── medallion_pipeline_dag.py  # Airflow DAG (Bronze→Silver→GE→Gold)
├── great_expectations/
│   ├── great_expectations.yml     # GE context (Spark datasource)
│   ├── expectations/
│   │   └── silver_trips_suite.json  # 25+ expectations for Silver
│   └── checkpoints/
│       └── silver_trips_checkpoint.yml
├── tests/
│   └── test_silver_transform.py   # 25+ pytest unit tests (local Spark)
├── .github/
│   └── workflows/
│       └── ci.yml                 # Full CI pipeline
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### Prerequisites

- Python 3.11+
- Java 11+ (for local PySpark)
- Databricks workspace with Unity Catalog enabled
- Apache Airflow 2.8+ deployment
- Git

### 1. Clone & Install

```bash
git clone https://github.com/your-org/medallion-lakehouse-pipeline.git
cd medallion-lakehouse-pipeline

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Airflow Variables

In your Airflow UI (Admin → Variables), set:

| Key | Example Value | Description |
|---|---|---|
| `nyc_taxi_catalog` | `main` | Unity Catalog name |
| `nyc_taxi_database` | `nyc_taxi` | Schema / database |
| `bronze_job_id` | `1001` | Databricks Job ID for Bronze notebook |
| `silver_job_id` | `1002` | Databricks Job ID for Silver notebook |
| `gold_job_id` | `1003` | Databricks Job ID for Gold notebook |
| `ge_job_id` | `1004` | Databricks Job ID for GE checkpoint |
| `alert_emails` | `you@company.com` | Comma-separated failure alert recipients |

### 3. Configure Airflow Connection

```bash
# Databricks connection (via Airflow CLI or UI)
airflow connections add databricks_default \
  --conn-type databricks \
  --conn-host https://<workspace>.azuredatabricks.net \
  --conn-password <personal-access-token>
```

### 4. Register Databricks Jobs

Upload the three notebooks to your Databricks workspace and create Jobs
(Workflows → Create Job) pointing to each notebook. Note the Job IDs and
set them as Airflow Variables (step 2).

### 5. Configure Data Source

Upload NYC TLC Yellow Taxi CSVs to your cloud storage:

```
# ADLS Gen2 example
abfss://raw@<storage>.dfs.core.windows.net/nyc_taxi/

# DBFS example
dbfs:/FileStore/nyc_taxi/raw/
```

Update the `source_path` widget default in `01_bronze_ingestion.py`, or
pass it as a Databricks Job parameter.

### 6. Run Tests Locally

```bash
# Run all unit tests with coverage
pytest tests/ -v --cov=tests --cov-report=term-missing

# Run a specific test class
pytest tests/test_silver_transform.py::TestFiltering -v
```

### 7. Validate GE Suite

```bash
# From the project root
cd great_expectations
great_expectations suite list
great_expectations checkpoint run silver_trips_checkpoint \
  --batch-request '{"runtime_parameters": {"batch_date": "2024-01-15"}}'
```

### 8. Trigger the Pipeline

```bash
# Manual Airflow trigger
airflow dags trigger medallion_lakehouse_pipeline --conf '{"batch_date": "2024-01-15"}'
```

---

## NYC Taxi Dataset Schema

Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

| Column | Type | Description |
|---|---|---|
| `pickup_datetime` | TIMESTAMP | Trip start time |
| `dropoff_datetime` | TIMESTAMP | Trip end time |
| `passenger_count` | INT | Number of passengers (1–9) |
| `trip_distance` | DOUBLE | Miles traveled |
| `fare_amount` | DOUBLE | Base fare (min $2.50) |
| `tip_amount` | DOUBLE | Tip (credit card only) |
| `total_amount` | DOUBLE | Total charged to passenger |
| `payment_type` | INT | 1=Credit, 2=Cash, 3–6=Other |
| `pu_location_id` | INT | TLC pickup zone ID |
| `do_location_id` | INT | TLC dropoff zone ID |

**Derived (Silver):** `trip_duration_minutes`, `speed_mph`, `fare_per_mile`, `tip_percentage`, `pickup_hour/dow/month/year`, `payment_type_desc`

---

## Data Quality Rules

| Check | Rule | Action on Failure |
|---|---|---|
| Row count | ≥ 1,000 rows per batch | Pipeline abort |
| Rejection rate | < 20% filtered rows | Pipeline abort |
| Null checks | 10 critical columns | GE expectation failure |
| Passenger count | 1–9 | Row filtered |
| Trip distance | 0.01–500 miles | Row filtered |
| Fare amount | $2.50–$1,000 | Row filtered |
| Trip duration | 0.5–480 minutes | GE expectation |
| Dropoff > Pickup | Always true | Row filtered |
| Payment type | TLC codes 1–6 | Row filtered |

---

## Contributing

1. Create a feature branch: `git checkout -b feat/your-feature`
2. Make changes and run tests: `pytest tests/ -v`
3. Open a PR → CI runs automatically (lint, tests, GE validation, DAG check, security scan)
4. All CI checks must pass before merge
                            
