"""
Gold Layer: fact_orders.

Joins cleansed Silver orders to the current row of ``dim_customer`` to
attach the customer surrogate key, then produces:

1. ``fact_orders`` - one row per order, partitioned by ``order_date``.
2. ``agg_daily_sales`` - daily aggregations (gross / net / order_count) at
   the ``(order_date, country)`` grain, suitable for BI dashboards.

The fact is upserted via Delta MERGE keyed on ``order_id`` so reruns are
idempotent.
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io_utils import merge_delta, write_delta
from src.common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FactOrdersBuilder:
    """Construct the ``fact_orders`` table and a daily aggregate."""

    def __init__(
        self,
        spark: SparkSession,
        silver_orders_path: str,
        dim_customer_path: str,
        gold_fact_path: str,
        gold_agg_path: str,
    ) -> None:
        self.spark = spark
        self.silver_orders_path = silver_orders_path
        self.dim_customer_path = dim_customer_path
        self.gold_fact_path = gold_fact_path
        self.gold_agg_path = gold_agg_path

    def _read(self, path: str) -> DataFrame:
        return self.spark.read.format("delta").load(path)

    def build_fact(self) -> DataFrame:
        """Join Silver orders to the current customer dimension row."""
        orders = self._read(self.silver_orders_path)
        dim = (
            self._read(self.dim_customer_path)
            .filter(F.col("is_current") == True)  # noqa: E712
            .select("customer_id", "customer_sk", "country")
        )

        # Left join so we never lose an order if the dimension is late.
        fact = (
            orders.alias("o")
            .join(dim.alias("c"), on="customer_id", how="left")
            .select(
                F.col("o.order_id"),
                F.col("o.customer_id"),
                F.col("c.customer_sk"),
                F.col("c.country"),
                F.col("o.product_id"),
                F.col("o.order_date"),
                F.col("o.order_timestamp"),
                F.col("o.order_status"),
                F.col("o.payment_method"),
                F.col("o.quantity"),
                F.col("o.unit_price"),
                F.col("o.discount_amount"),
                F.col("o.gross_amount"),
                F.col("o.net_amount"),
            )
            .withColumn("_gold_timestamp", F.current_timestamp())
        )
        return fact

    def write_fact(self, fact: DataFrame) -> None:
        merge_delta(
            spark=self.spark,
            updates=fact,
            target_path=self.gold_fact_path,
            merge_keys=["order_id"],
            partition_by=["order_date"],
        )

    def build_and_write_daily_agg(self, fact: DataFrame) -> None:
        """Daily sales aggregation at (order_date, country) grain."""
        agg = (
            fact.groupBy("order_date", "country")
            .agg(
                F.count("order_id").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("quantity").alias("total_units"),
                F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
                F.round(F.sum("net_amount"), 2).alias("net_revenue"),
                F.round(F.avg("net_amount"), 2).alias("avg_order_value"),
            )
            .withColumn("_gold_timestamp", F.current_timestamp())
        )
        # Aggregations are cheaper to recompute than to merge, so overwrite
        # partition-by-partition would be a future optimization. For now,
        # full overwrite keeps things simple and correct.
        write_delta(
            agg,
            self.gold_agg_path,
            mode="overwrite",
            partition_by=["order_date"],
            overwrite_schema=True,
        )

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("Silver -> Gold pipeline: fact_orders + agg_daily_sales")
        logger.info("=" * 60)
        fact = self.build_fact().cache()
        try:
            count = fact.count()
            logger.info("Writing %d rows to fact_orders at %s", count, self.gold_fact_path)
            self.write_fact(fact)
            self.build_and_write_daily_agg(fact)
        finally:
            fact.unpersist()
        logger.info("fact_orders + agg_daily_sales complete.")


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold fact_orders + daily agg.")
    parser.add_argument("--silver-orders-path", required=True)
    parser.add_argument("--dim-customer-path", required=True)
    parser.add_argument("--gold-fact-path", required=True)
    parser.add_argument("--gold-agg-path", required=True)
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    spark = build_spark_session(app_name="gold-fact-orders")
    try:
        FactOrdersBuilder(
            spark=spark,
            silver_orders_path=args.silver_orders_path,
            dim_customer_path=args.dim_customer_path,
            gold_fact_path=args.gold_fact_path,
            gold_agg_path=args.gold_agg_path,
        ).run()
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
