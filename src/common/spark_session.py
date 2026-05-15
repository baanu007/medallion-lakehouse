"""
Reusable Spark session builder with Delta Lake configuration.

All pipeline entry points should construct their SparkSession through
``build_spark_session`` so that Delta extensions, catalog, and AQE are
configured consistently across Bronze / Silver / Gold jobs.
"""

from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import SparkSession


# Sane defaults for a Delta-enabled session. Caller-supplied ``extra_conf``
# values override these. Memory/cores left to the cluster manager so the
# same code can run on a laptop, EMR, Databricks, or Glue.
_DEFAULT_DELTA_CONF: Dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    # Helpful for local / CI runs - safe defaults in production too
    "spark.sql.session.timeZone": "UTC",
}


def build_spark_session(
    app_name: str = "medallion-lakehouse",
    master: Optional[str] = None,
    extra_conf: Optional[Dict[str, str]] = None,
    enable_hive: bool = False,
) -> SparkSession:
    """Build (or fetch) a Delta-enabled SparkSession.

    Args:
        app_name: Spark application name shown in the UI / logs.
        master: Optional master URL. Defaults to whatever the runtime
            provides (e.g. ``local[*]`` for tests, ``yarn`` on EMR).
        extra_conf: Additional ``spark.conf`` key/value pairs that
            override the Delta defaults.
        enable_hive: When True, enables Hive support on the builder
            (useful when writing to a metastore-backed catalog).

    Returns:
        A configured ``SparkSession`` ready for Delta reads/writes.
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    conf = {**_DEFAULT_DELTA_CONF, **(extra_conf or {})}
    for key, value in conf.items():
        builder = builder.config(key, value)

    if enable_hive:
        builder = builder.enableHiveSupport()

    return builder.getOrCreate()
