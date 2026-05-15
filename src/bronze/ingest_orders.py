"""
Bronze Layer: Ingest Orders.

Reads raw orders from a configurable source path (CSV or JSON) and lands
them as Delta in the Bronze zone. The Bronze layer is intentionally
append-only and preserves the source data verbatim plus a small set of
audit columns:

* ``_ingestion_timestamp`` - when the row was ingested
* ``_source_file``        - originating file path (useful for lineage)
* ``_source_format``      - csv/json/etc.

Usage:
    python -m src.bronze.ingest_orders \\
        --source-path data/sample/orders.csv \\
        --bronze-path s3://my-bucket/bronze/orders/ \\
        --format csv
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.io_utils import write_delta
from src.common.spark_session import build_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


# Fallback schema applied when ``infer_schema=False`` (or inference fails).
# Keeping a hard-coded schema means we don't pay the cost of a second pass
# over the data and we get consistent types across runs.
ORDERS_FALLBACK_SCHEMA: StructType = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_date", StringType(), nullable=True),
        StructField("order_timestamp", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(10, 2), nullable=True),
        StructField("discount_amount", DecimalType(10, 2), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("_ingestion_timestamp", TimestampType(), nullable=True),
    ]
)


class OrdersIngestor:
    """Read raw orders from a source path and land them in Bronze."""

    def __init__(
        self,
        spark: SparkSession,
        source_path: str,
        bronze_path: str,
        source_format: str = "csv",
        infer_schema: bool = True,
    ) -> None:
        self.spark = spark
        self.source_path = source_path
        self.bronze_path = bronze_path
        self.source_format = source_format.lower()
        self.infer_schema = infer_schema

    def read_source(self) -> DataFrame:
        """Read the raw source file(s).

        Tries schema inference first when enabled, otherwise falls back to
        the explicit schema above. Errors during inference are caught and
        we retry with the explicit schema (defensive against malformed
        rows on first ingestion).
        """
        logger.info(
            "Reading source %s (format=%s, infer_schema=%s)",
            self.source_path,
            self.source_format,
            self.infer_schema,
        )

        reader = self.spark.read.format(self.source_format)

        if self.source_format == "csv":
            reader = reader.option("header", "true").option("mode", "PERMISSIVE")

        if self.infer_schema:
            try:
                return reader.option("inferSchema", "true").load(self.source_path)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "Schema inference failed (%s); falling back to explicit schema",
                    exc,
                )

        return reader.schema(ORDERS_FALLBACK_SCHEMA).load(self.source_path)

    @staticmethod
    def add_audit_columns(df: DataFrame, source_format: str) -> DataFrame:
        """Append lineage / audit columns required by the Bronze contract."""
        return (
            df.withColumn(
                "_ingestion_timestamp",
                F.coalesce(F.col("_ingestion_timestamp"), F.current_timestamp())
                if "_ingestion_timestamp" in df.columns
                else F.current_timestamp(),
            )
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_source_format", F.lit(source_format))
        )

    def write_bronze(self, df: DataFrame) -> None:
        """Append the ingested batch to the Bronze Delta table."""
        write_delta(
            df,
            self.bronze_path,
            mode="append",
            partition_by=["order_date"] if "order_date" in df.columns else None,
            merge_schema=True,
        )

    def run(self) -> int:
        """Execute the ingestion. Returns the number of rows written."""
        logger.info("=" * 60)
        logger.info("Bronze ingestion: orders")
        logger.info("=" * 60)

        raw = self.read_source()
        audited = self.add_audit_columns(raw, self.source_format)
        count = audited.count()
        logger.info("Ingesting %d rows into %s", count, self.bronze_path)
        self.write_bronze(audited)
        logger.info("Bronze ingestion complete.")
        return count


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw orders into the Bronze layer.")
    parser.add_argument("--source-path", required=True, help="Path to raw orders (file or dir).")
    parser.add_argument(
        "--bronze-path",
        required=True,
        help="Target Delta path for Bronze orders (e.g. s3://bucket/bronze/orders/).",
    )
    parser.add_argument("--format", default="csv", choices=["csv", "json", "parquet"])
    parser.add_argument(
        "--no-infer-schema",
        action="store_true",
        help="Disable schema inference and use the explicit fallback schema.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    spark = build_spark_session(app_name="bronze-ingest-orders")
    try:
        OrdersIngestor(
            spark=spark,
            source_path=args.source_path,
            bronze_path=args.bronze_path,
            source_format=args.format,
            infer_schema=not args.no_infer_schema,
        ).run()
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
