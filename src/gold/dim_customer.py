"""
Gold Layer: dim_customer (SCD Type 2).

Builds and maintains a Type 2 slowly-changing customer dimension from the
cleansed Silver customers table.

Tracked attributes (changes trigger a new version):
* first_name, last_name, email, country

Each row carries:
* ``customer_sk``     - surrogate hash key (stable per ``customer_id`` + version)
* ``effective_from``  - timestamp the version became active
* ``effective_to``    - timestamp the version was superseded (NULL when current)
* ``is_current``      - boolean convenience flag

Strategy: read current dimension snapshot (if any), compare to incoming
Silver records, and use Delta MERGE to (a) close existing current rows
that have changed and (b) insert new versions.
"""

from __future__ import annotations

import argparse
import logging
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io_utils import write_delta
from src.common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Attributes that, when changed, force a new SCD2 version.
TRACKED_ATTRIBUTES: List[str] = ["first_name", "last_name", "email", "country"]


class DimCustomerBuilder:
    """Build and maintain the ``dim_customer`` SCD Type 2 table."""

    def __init__(
        self,
        spark: SparkSession,
        silver_path: str,
        gold_path: str,
    ) -> None:
        self.spark = spark
        self.silver_path = silver_path
        self.gold_path = gold_path

    def read_silver(self) -> DataFrame:
        logger.info("Reading Silver customers from %s", self.silver_path)
        return self.spark.read.format("delta").load(self.silver_path)

    @staticmethod
    def build_new_versions(silver_df: DataFrame) -> DataFrame:
        """Project Silver into the dim_customer shape for new/changed rows.

        Surrogate key (``customer_sk``) is the SHA-256 of
        ``customer_id || tracked_attrs || effective_from``. Including
        ``effective_from`` in the hash ensures **cycle stability**: if a
        customer's attributes go A -> B -> A, the second "A" version must
        get a *new* surrogate key rather than collide with the original
        "A" row (which is now expired). Without the timestamp component
        the second A row would reuse the original SK and break the
        SCD2 history join in ``fact_orders``.
        """
        # Materialize effective_from first so the same value flows into
        # both the hash and the column.
        with_eff = silver_df.select(
            "customer_id",
            *TRACKED_ATTRIBUTES,
            "signup_date",
        ).withColumn("effective_from", F.current_timestamp())

        hash_input = F.concat_ws(
            "||",
            F.col("customer_id").cast("string"),
            *[F.col(c).cast("string") for c in TRACKED_ATTRIBUTES],
            F.col("effective_from").cast("string"),
        )
        return (
            with_eff.withColumn("customer_sk", F.sha2(hash_input, 256))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

    def _initial_load(self, new_versions: DataFrame) -> None:
        logger.info("Initial load of dim_customer at %s", self.gold_path)
        write_delta(new_versions, self.gold_path, mode="overwrite")

    def _apply_scd2(self, new_versions: DataFrame) -> None:
        """Close changed current rows and insert new versions via MERGE."""
        dim = DeltaTable.forPath(self.spark, self.gold_path)

        # Identify current rows whose tracked attributes differ from incoming.
        current = (
            dim.toDF()
            .filter(F.col("is_current") == True)  # noqa: E712 - Spark needs ==
            .alias("cur")
        )
        incoming = new_versions.alias("inc")

        # NULL-safe change detection. Plain ``cur.c <> inc.c`` evaluates
        # to NULL whenever either side is NULL, and ``NULL OR x`` is
        # itself NULL/falsy, so changes that involve NULLs (NULL -> value
        # or value -> NULL) are silently missed. We use the SQL
        # ``IS DISTINCT FROM`` semantics, expanded into Spark-compatible
        # boolean logic: a change occurs when exactly one side is NULL,
        # or when both are non-NULL and differ.
        change_condition = " OR ".join(
            [
                (
                    f"((cur.{c} IS NULL AND inc.{c} IS NOT NULL) "
                    f"OR (cur.{c} IS NOT NULL AND inc.{c} IS NULL) "
                    f"OR (cur.{c} IS NOT NULL AND inc.{c} IS NOT NULL "
                    f"AND cur.{c} <> inc.{c}))"
                )
                for c in TRACKED_ATTRIBUTES
            ]
        )

        changed = (
            current.join(incoming, on="customer_id", how="inner")
            .filter(change_condition)
            .select("cur.customer_sk")
        )

        # Step 1: expire the now-stale current rows.
        (
            dim.alias("target")
            .merge(
                changed.alias("changed"),
                "target.customer_sk = changed.customer_sk",
            )
            .whenMatchedUpdate(
                set={
                    "effective_to": "current_timestamp()",
                    "is_current": "false",
                }
            )
            .execute()
        )

        # Step 2: insert genuinely new versions (new customer_id OR changed attrs).
        existing_current = (
            self.spark.read.format("delta")
            .load(self.gold_path)
            .filter(F.col("is_current") == True)  # noqa: E712
            .select("customer_id", *TRACKED_ATTRIBUTES)
            .alias("cur")
        )

        to_insert = (
            new_versions.alias("inc")
            .join(existing_current, on="customer_id", how="left")
            .where(
                F.col("cur.customer_id").isNull()
                | F.expr(change_condition)
            )
            .select("inc.*")
        )

        if to_insert.head(1):
            write_delta(to_insert, self.gold_path, mode="append")

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("Silver -> Gold pipeline: dim_customer (SCD2)")
        logger.info("=" * 60)

        silver = self.read_silver()
        new_versions = self.build_new_versions(silver)

        if not DeltaTable.isDeltaTable(self.spark, self.gold_path):
            self._initial_load(new_versions)
            return

        self._apply_scd2(new_versions)
        logger.info("dim_customer SCD2 update complete.")


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build / update dim_customer (SCD2).")
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    spark = build_spark_session(app_name="gold-dim-customer")
    try:
        DimCustomerBuilder(
            spark=spark,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
        ).run()
    finally:
        spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
