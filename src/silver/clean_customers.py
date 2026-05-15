"""
Silver Layer: Clean Customers.

Takes raw customer records ingested into Bronze and produces a cleaned,
deduplicated Silver table suitable for downstream dimension building.

Cleaning rules:
* Trim whitespace and normalize casing on text fields.
* Lowercase + validate email; rows with malformed emails are flagged
  rather than dropped (kept for analytics on data quality).
* Cast date columns and drop rows with invalid ``customer_id``.
* Deduplicate by ``customer_id``, keeping the latest by
  ``_ingestion_timestamp``.
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.io_utils import merge_delta
from src.common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Same regex used by most email validators - good enough for cleansing,
# not a substitute for sending a confirmation email.
EMAIL_REGEX = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


class CustomersCleaner:
    """Clean Bronze customers and write the result to Silver."""

    def __init__(
        self,
        spark: SparkSession,
        bronze_path: str,
        silver_path: str,
    ) -> None:
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self) -> DataFrame:
        logger.info("Reading Bronze customers from %s", self.bronze_path)
        return self.spark.read.format("delta").load(self.bronze_path)

    @staticmethod
    def clean(df: DataFrame) -> DataFrame:
        """Apply column-level cleaning rules."""
        return (
            df.withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
            .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
            .withColumn("email", F.lower(F.trim(F.col("email"))))
            .withColumn(
                "email_is_valid",
                F.col("email").rlike(EMAIL_REGEX),
            )
            .withColumn("country", F.upper(F.trim(F.col("country"))))
            .withColumn("signup_date", F.to_date(F.col("signup_date")))
            .filter(F.col("customer_id").isNotNull())
        )

    @staticmethod
    def deduplicate(df: DataFrame) -> DataFrame:
        """Keep the most recent record per ``customer_id``."""
        window = Window.partitionBy("customer_id").orderBy(
            F.desc("_ingestion_timestamp")
        )
        return (
            df.withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    @staticmethod
    def add_audit(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_silver_timestamp", F.current_timestamp())
            .withColumn("_source_layer", F.lit("bronze"))
        )

    def write_silver(self, df: DataFrame) -> None:
        """Upsert into Silver using MERGE on ``customer_id``."""
        merge_delta(
            spark=self.spark,
            updates=df,
            target_path=self.silver_path,
            merge_keys=["customer_id"],
        )

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("Bronze -> Silver pipeline: customers")
        logger.info("=" * 60)

        bronze = self.read_bronze()
        cleaned = self.clean(bronze)
        deduped = self.deduplicate(cleaned)
        final = self.add_audit(deduped)

        count = final.count()
        logger.info("Writing %d customers to Silver at %s", count, self.silver_path)
        self.write_silver(final)
        logger.info("Silver customers complete.")


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Bronze customers into Silver.")
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    spark = build_spark_session(app_name="silver-clean-customers")
    try:
        CustomersCleaner(
            spark=spark,
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
        ).run()
    finally:
        spark.stop()


__all__ = ["CustomersCleaner"]


if __name__ == "__main__":  # pragma: no cover
    main()
