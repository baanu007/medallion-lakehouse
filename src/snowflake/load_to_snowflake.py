"""
Load Gold Delta tables into Snowflake.

Two paths are supported:

1. **Pandas path** (default): Read a Gold Delta table with Spark, convert
   to pandas, and use ``snowflake-connector-python``'s ``write_pandas``
   to bulk-load via internal stage + COPY INTO. Good for small/medium
   Gold tables (dim_customer, agg_daily_sales).

2. **Spark connector path**: When ``--use-spark-connector`` is passed,
   the loader writes directly using the ``net.snowflake.spark.snowflake``
   format. Requires the spark-snowflake JAR on the classpath at runtime.

Credentials are read from environment variables - **never hardcoded**:

    SNOWFLAKE_ACCOUNT       e.g. xy12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD      (or SNOWFLAKE_PRIVATE_KEY_PATH)
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA

Usage:
    python -m src.snowflake.load_to_snowflake \\
        --gold-path s3://bucket/gold/dim_customer/ \\
        --table DIM_CUSTOMER
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Optional

from src.common.spark_session import build_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REQUIRED_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
)


@dataclass(frozen=True)
class SnowflakeConfig:
    """Snowflake connection configuration sourced from env vars."""

    account: str
    user: str
    password: Optional[str]
    role: Optional[str]
    warehouse: str
    database: str
    schema: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
        if missing:
            raise EnvironmentError(
                f"Missing required Snowflake env vars: {', '.join(missing)}"
            )
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        )

    def as_connector_kwargs(self) -> dict:
        kwargs = {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.password:
            kwargs["password"] = self.password
        if self.role:
            kwargs["role"] = self.role
        return kwargs

    def as_spark_options(self) -> dict:
        opts = {
            "sfURL": f"{self.account}.snowflakecomputing.com",
            "sfUser": self.user,
            "sfWarehouse": self.warehouse,
            "sfDatabase": self.database,
            "sfSchema": self.schema,
        }
        if self.password:
            opts["sfPassword"] = self.password
        if self.role:
            opts["sfRole"] = self.role
        return opts


def load_via_pandas(
    gold_path: str,
    table: str,
    config: SnowflakeConfig,
    mode: str = "append",
) -> int:
    """Load a Gold Delta table to Snowflake using ``write_pandas``.

    Returns the number of rows written.
    """
    # Imported lazily so tests / Spark-only environments don't require
    # snowflake-connector-python installed.
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    spark = build_spark_session(app_name=f"snowflake-load-{table}")
    try:
        pdf = spark.read.format("delta").load(gold_path).toPandas()
    finally:
        spark.stop()

    logger.info("Loaded %d rows from %s into pandas", len(pdf), gold_path)

    # Snowflake DDL declares columns in UPPERCASE (the Snowflake default).
    # ``write_pandas`` quotes identifiers when staging the COPY INTO, so
    # lower-case pandas column names (which is what Delta hands us) end
    # up as ``"customer_id"`` and don't match the unquoted UPPERCASE
    # columns in the target table -> COPY INTO fails with
    # ``invalid column name 'customer_id'``. Uppercasing the DataFrame
    # columns here keeps the quoted identifiers aligned with the DDL.
    pdf.columns = [c.upper() for c in pdf.columns]

    with snowflake.connector.connect(**config.as_connector_kwargs()) as conn:
        if mode == "overwrite":
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        success, num_chunks, num_rows, _ = write_pandas(
            conn=conn,
            df=pdf,
            table_name=table,
            auto_create_table=False,
            overwrite=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed for table {table}")
        logger.info(
            "write_pandas succeeded: rows=%d, chunks=%d, table=%s",
            num_rows,
            num_chunks,
            table,
        )
        return num_rows


def load_via_spark_connector(
    gold_path: str,
    table: str,
    config: SnowflakeConfig,
    mode: str = "append",
) -> None:
    """Load a Gold Delta table to Snowflake using the spark-snowflake connector.

    Requires the ``net.snowflake:spark-snowflake_2.12`` package on the
    Spark classpath at runtime.
    """
    spark = build_spark_session(app_name=f"snowflake-spark-{table}")
    try:
        df = spark.read.format("delta").load(gold_path)
        (
            df.write.format("net.snowflake.spark.snowflake")
            .options(**config.as_spark_options())
            .option("dbtable", table)
            .mode(mode)
            .save()
        )
        logger.info("spark-snowflake write to %s complete", table)
    finally:
        spark.stop()


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a Gold Delta table to Snowflake.")
    parser.add_argument("--gold-path", required=True, help="Source Delta table path.")
    parser.add_argument("--table", required=True, help="Target Snowflake table name.")
    parser.add_argument(
        "--mode",
        default="append",
        choices=["append", "overwrite"],
        help="Write disposition.",
    )
    parser.add_argument(
        "--use-spark-connector",
        action="store_true",
        help="Use spark-snowflake connector instead of pandas/write_pandas.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    args = _parse_args(argv)
    config = SnowflakeConfig.from_env()

    if args.use_spark_connector:
        load_via_spark_connector(args.gold_path, args.table, config, mode=args.mode)
    else:
        load_via_pandas(args.gold_path, args.table, config, mode=args.mode)


if __name__ == "__main__":  # pragma: no cover
    main()
