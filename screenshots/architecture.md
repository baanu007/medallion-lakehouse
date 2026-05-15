# Architecture Diagram (placeholder)

`architecture.png` should live alongside this file and depict the end-to-end
Medallion Lakehouse pipeline:

1. **Sources** (left): files (CSV/JSON), APIs, operational databases.
2. **Bronze** (top of lakehouse): raw, append-only Delta tables in S3
   (`s3://<bucket>/bronze/orders/`, `.../customers/`). Schema evolution is
   enabled; audit columns `_ingestion_timestamp` and `_source_file` are added.
3. **Silver** (middle): cleansed, deduplicated Delta tables. MERGE upserts
   keyed on natural keys (`order_id`, `customer_id`).
4. **Gold** (bottom): dimensional model.
   - `dim_customer` - SCD Type 2 with `effective_from` / `effective_to` /
     `is_current`.
   - `fact_orders` - order-grain fact joined to `customer_sk`.
   - `agg_daily_sales` - daily aggregates at `(order_date, country)` grain.
5. **Consumers** (right): Snowflake (Gold tables loaded via
   `src/snowflake/load_to_snowflake.py`), Power BI, and notebooks.

Suggested tools to author the diagram: draw.io, Excalidraw, or Lucidchart.
Export to `screenshots/architecture.png` once finalized.
