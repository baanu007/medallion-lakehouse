# 🏅 Medallion Lakehouse Architecture

A complete data lakehouse implementation using the **Medallion Architecture** (Bronze/Silver/Gold) with **Delta Lake**, **PySpark**, and **Snowflake**.

![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

![Architecture](screenshots/architecture.png)

> Diagram lives at `screenshots/architecture.png` (see `screenshots/architecture.md` for what it depicts; export from draw.io / Excalidraw to that path).

## 📋 Overview

This project implements a production-grade Medallion Architecture that provides:

- **Bronze Layer**: Raw data ingestion with schema evolution
- **Silver Layer**: Cleaned, deduplicated, conformed data
- **Gold Layer**: Business-level aggregates and analytics
- **ACID Transactions**: Guaranteed data consistency
- **Time Travel**: Query historical data versions
- **Schema Enforcement**: Prevent bad data from entering

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │   APIs   │  │  Files   │  │  Kafka   │  │ Database │  │   IoT    │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
└───────┼─────────────┼─────────────┼─────────────┼─────────────┼───────────┘
        │             │             │             │             │
        └─────────────┴─────────────┴─────────────┴─────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (Raw)                                                       │
│  ────────────────────────────────────────────────────────────────────────── │
│  • Raw data in original format                                               │
│  • Append-only writes                                                        │
│  • Schema evolution enabled                                                  │
│  • Full history preserved                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  delta/bronze/orders/     delta/bronze/customers/    ...            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ ETL (Clean, Dedupe, Conform)
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (Cleaned)                                                   │
│  ────────────────────────────────────────────────────────────────────────── │
│  • Cleaned and validated data                                                │
│  • Deduplication applied                                                     │
│  • Schema enforced                                                           │
│  • Standardized formats                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  delta/silver/orders/     delta/silver/customers/    ...            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ ETL (Aggregate, Join, Enrich)
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (Business)                                                    │
│  ────────────────────────────────────────────────────────────────────────── │
│  • Business-level aggregations                                               │
│  • Dimensional models (Star Schema)                                          │
│  • Optimized for analytics                                                   │
│  • Ready for BI tools                                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  delta/gold/dim_customers/  delta/gold/fct_sales/    ...            │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
            │  Snowflake  │ │  Power BI   │ │   Jupyter   │
            │  Warehouse  │ │  Dashboards │ │  Notebooks  │
            └─────────────┘ └─────────────┘ └─────────────┘
```

## 📁 Project Structure

```
medallion-lakehouse/
├── src/
│   ├── bronze/
│   │   └── ingest_orders.py        # CSV/JSON -> Bronze Delta + audit cols
│   ├── silver/
│   │   ├── clean_orders.py         # Dedup, clean, MERGE upsert
│   │   └── clean_customers.py
│   ├── gold/
│   │   ├── dim_customer.py         # SCD Type 2 customer dimension
│   │   └── fact_orders.py          # Order-grain fact + daily aggregates
│   ├── common/
│   │   ├── spark_session.py        # Delta-enabled SparkSession builder
│   │   └── io_utils.py             # read / write / merge Delta helpers
│   └── snowflake/
│       └── load_to_snowflake.py    # Gold -> Snowflake loader
├── sql/
│   └── snowflake_ddl.sql           # DIM_CUSTOMER, FACT_ORDERS, AGG_DAILY_SALES
├── data/
│   └── sample/                     # ~500 rows of synthetic orders/customers
├── tests/                          # pytest suite (local Spark, no AWS/SF)
├── screenshots/                    # architecture.png lives here
├── .github/workflows/ci.yml        # lint + pytest on every PR
├── requirements.txt
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Apache Spark 3.4+
- Delta Lake 2.4+
- (Optional) Databricks Runtime

### Installation

```bash
# Clone repository
git clone https://github.com/baanu007/medallion-lakehouse.git
cd medallion-lakehouse

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run Pipeline

```bash
# Bronze: Ingest raw orders
python -m src.bronze.ingest_orders \
    --source-path data/sample/orders.csv \
    --bronze-path ./_lake/bronze/orders \
    --format csv

# Silver: Clean orders + customers
python -m src.silver.clean_orders
python -m src.silver.clean_customers \
    --bronze-path ./_lake/bronze/customers \
    --silver-path ./_lake/silver/customers

# Gold: Build the SCD2 dim and the fact + daily agg
python -m src.gold.dim_customer \
    --silver-path ./_lake/silver/customers \
    --gold-path ./_lake/gold/dim_customer
python -m src.gold.fact_orders \
    --silver-orders-path ./_lake/silver/orders \
    --dim-customer-path ./_lake/gold/dim_customer \
    --gold-fact-path ./_lake/gold/fact_orders \
    --gold-agg-path ./_lake/gold/agg_daily_sales

# Snowflake: load a Gold table (creds via env vars)
python -m src.snowflake.load_to_snowflake \
    --gold-path ./_lake/gold/dim_customer \
    --table DIM_CUSTOMER
```

## 📦 Data Source

This repo ships with deterministic synthetic data under `data/sample/` so the
pipeline is runnable locally without any cloud credentials:

| File                          | Rows | Description                              |
| ----------------------------- | ---- | ---------------------------------------- |
| `data/sample/orders.csv`      | 500  | Bronze-shaped orders with audit columns  |
| `data/sample/customers.csv`   | 100  | Bronze-shaped customers                  |

Regenerate or scale up via `python data/sample/generate_sample_data.py`.

For a realistic-scale public dataset, point the Bronze ingestor at the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
on Kaggle - schemas align closely with the orders/customers model used here.

## 🧪 Tests

```bash
pip install -r requirements.txt
pytest -ra tests/
```

Tests run against a local Spark session with Delta enabled - no AWS or
Snowflake access is required. CI runs the same suite on every PR via
`.github/workflows/ci.yml`.

## 📊 Delta Lake Features Used

### ACID Transactions
```python
# Write with ACID guarantees
df.write.format("delta").mode("append").save(bronze_path)
```

### Schema Evolution
```python
# Enable automatic schema evolution
df.write.option("mergeSchema", "true").format("delta").save(path)
```

### Time Travel
```python
# Query historical version
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(path)

# Query by timestamp
df_yesterday = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```

### MERGE (Upsert)
```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, silver_path)

delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Optimize & Z-Order
```python
from delta.tables import DeltaTable

# Compact small files
delta_table.optimize().executeCompaction()

# Z-Order for query optimization
delta_table.optimize().executeZOrderBy("customer_id", "order_date")
```

## 🔧 Configuration

```yaml
# config/pipeline_config.yaml
storage:
  base_path: "s3://data-lake"
  bronze_path: "${base_path}/bronze"
  silver_path: "${base_path}/silver"
  gold_path: "${base_path}/gold"

delta:
  optimize_frequency: "daily"
  vacuum_retention_hours: 168  # 7 days
  enable_change_data_feed: true

quality:
  null_threshold: 0.05  # 5% max nulls
  duplicate_threshold: 0.0  # No duplicates in silver
  
snowflake:
  account: ${SNOWFLAKE_ACCOUNT}
  database: LAKEHOUSE
  schema: GOLD
```

## 📈 Sample Queries

### Time Travel Query
```sql
-- View data as of specific version
SELECT * FROM delta.`/path/to/table` VERSION AS OF 5;

-- View table history
DESCRIBE HISTORY delta.`/path/to/table`;
```

### Incremental Processing
```sql
-- Read only new changes
SELECT * FROM table_changes('delta.`/path/to/table`', 10);
```

## 🛠️ Technologies

| Layer | Technology | Purpose |
|-------|------------|---------|
| Storage | Delta Lake 3.1 | ACID transactions, versioning, time travel |
| Processing | PySpark 3.5 | Distributed transformations |
| Warehouse | Snowflake | Analytics queries (Gold tables) |
| Testing | pytest + local Spark | Unit + integration tests |
| CI | GitHub Actions | Lint (flake8) + tests on every PR |

Orchestration (Airflow/Dagster) is intentionally **not** included in this
repo - the jobs are designed as standalone entry points so any scheduler can
drive them.

## 📄 License

MIT License

---

*Building reliable, scalable data lakehouses with Delta Lake*
