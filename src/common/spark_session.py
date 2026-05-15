"""
Reusable Spark session builder with Delta Lake configuration.

All pipeline entry points should construct their SparkSession through
``build_spark_session`` so that Delta extensions, catalog, and AQE are
configured consistently across Bronze / Silver / Gold jobs.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Conf keys that take effect at JVM/session-creation time only. If a
# SparkSession already exists, calling ``spark.conf.set`` on these is a
# no-op for the JVM (and PySpark may even reject some of them). We log a
# warning when callers try to override them on a cached session.
_JVM_LEVEL_CONF_PREFIXES = (
    "spark.jars",
    "spark.driver.",
    "spark.executor.",
    "spark.master",
    "spark.submit.",
    "spark.app.",
)


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

    Notes:
        ``SparkSession.builder.config(...).getOrCreate()`` returns the
        *cached* session when one already exists in this JVM, which means
        anything passed via ``builder.config`` after the first call is
        silently dropped. To make ``extra_conf`` actually take effect on
        reused sessions we explicitly call ``spark.conf.set(k, v)`` for
        each merged conf key after ``getOrCreate``. This covers all
        runtime-mutable keys (SQL/AQE/shuffle settings, time zone, etc.).

        **JVM-level options** such as ``spark.jars.packages``,
        ``spark.driver.memory``, ``spark.executor.*``, and the master URL
        can only be applied at session-creation time. If a session
        already existed when this function was called, those keys will
        not take effect on the cached session - a warning is logged to
        make that visible.
    """
    # Detect whether a session already exists *before* configuring the
    # builder. If one does, we don't want builder.config(...) to silently
    # overwrite caller customisations from a previous call (e.g. resetting
    # spark.sql.shuffle.partitions back to its default of 200).
    pre_existing = SparkSession.getActiveSession() is not None

    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    conf = {**_DEFAULT_DELTA_CONF, **(extra_conf or {})}
    if not pre_existing:
        # First-create path: apply both defaults and extras via the
        # builder so they take effect at JVM/session-creation time.
        for key, value in conf.items():
            builder = builder.config(key, value)
    else:
        # Reused-session path: only push caller-supplied overrides into
        # the builder. Defaults were already applied at first-create
        # time; re-applying them here would clobber any overrides the
        # caller set on a previous call.
        for key, value in (extra_conf or {}).items():
            builder = builder.config(key, value)

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()

    # builder.config(...) on a cached session is a no-op, so re-apply
    # the caller-supplied overrides via the runtime conf API. We only
    # touch keys that came through ``extra_conf`` (not the defaults) so
    # repeated calls without ``extra_conf`` don't clobber overrides set
    # by an earlier call. Defaults were already applied at first-create
    # time via builder.config(...) above.
    overrides_to_apply = extra_conf or {}
    for key, value in overrides_to_apply.items():
        try:
            spark.conf.set(key, value)
        except Exception as exc:  # pragma: no cover - defensive
            # Some keys (e.g. static SQL conf) raise AnalysisException
            # when set after session creation. Log and continue rather
            # than failing the whole job for a non-critical override.
            logger.warning(
                "Could not set spark.conf %s=%s on existing session: %s",
                key,
                value,
                exc,
            )

    if pre_existing and overrides_to_apply:
        jvm_level_overrides = [
            k for k in overrides_to_apply if k.startswith(_JVM_LEVEL_CONF_PREFIXES)
        ]
        if jvm_level_overrides:
            logger.warning(
                "build_spark_session() called with an existing SparkSession; "
                "JVM-level overrides will be ignored: %s",
                jvm_level_overrides,
            )
        else:
            logger.warning(
                "build_spark_session() reused an existing SparkSession; "
                "caller-supplied conf overrides have been re-applied but "
                "JVM options cannot change."
            )

    return spark
