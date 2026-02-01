# 🏅 Medallion Lakehouse Architecture

A complete data lakehouse implementation using the **Medallion Architecture** (Bronze/Silver/Gold) with **Delta Lake**, **PySpark**, and **Snowflake**.

![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

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
│   │   ├── ingest_orders.py
│   │   ├── ingest_customers.py
│   │   └── ingest_products.py
│   ├── silver/
│   │   ├── clean_orders.py
│   │   ├── clean_customers.py
│   │   └── clean_products.py
│   ├── gold/
│   │   ├── dim_customers.py
│   │   ├── dim_products.py
│   │   ├── dim_date.py
│   │   ├── fct_sales.py
│   │   └── agg_daily_sales.py
│   └── utils/
│       ├── delta_utils.py
│       ├── quality_checks.py
│       └── config.py
├── notebooks/
│   ├── 01_bronze_exploration.ipynb
│   ├── 02_silver_transformations.ipynb
│   └── 03_gold_analytics.ipynb
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── config/
│   └── pipeline_config.yaml
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
# Bronze: Ingest raw data
python src/bronze/ingest_orders.py

# Silver: Clean and transform
python src/silver/clean_orders.py

# Gold: Create aggregates
python src/gold/fct_sales.py
```

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
| Storage | Delta Lake | ACID transactions, versioning |
| Processing | PySpark | Distributed transformations |
| Orchestration | Airflow/Dagster | Pipeline scheduling |
| Warehouse | Snowflake | Analytics queries |
| Quality | Great Expectations | Data validation |

## 📄 License

MIT License

---

*Building reliable, scalable data lakehouses with Delta Lake*
