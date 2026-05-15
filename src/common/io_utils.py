"""
Delta Lake I/O helpers.

Centralizes Delta read / write / merge patterns so individual pipeline
modules don't repeat boilerplate (and so behavior stays consistent across
Bronze, Silver, and Gold).
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_delta(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    """Read a Delta table, optionally using time travel.

    Args:
        spark: Active SparkSession.
        path: Storage path of the Delta table (local, s3://, abfss://, etc.).
        version: Optional Delta version for ``VERSION AS OF`` reads.
        timestamp: Optional timestamp string for ``TIMESTAMP AS OF`` reads.

    Returns:
        The Delta table as a DataFrame.
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    if timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    logger.info("Reading Delta table from %s (version=%s, ts=%s)", path, version, timestamp)
    return reader.load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[Iterable[str]] = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
    options: Optional[Dict[str, str]] = None,
) -> None:
    """Write a DataFrame to a Delta table.

    Args:
        df: Source DataFrame.
        path: Target Delta path.
        mode: Spark write mode (``append`` / ``overwrite`` / ``errorifexists``).
        partition_by: Optional column names to partition by.
        merge_schema: Allow additive schema evolution on write.
        overwrite_schema: Replace schema entirely (use with ``overwrite``).
        options: Extra writer options.
    """
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*list(partition_by))
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    for key, value in (options or {}).items():
        writer = writer.option(key, value)

    logger.info("Writing Delta table to %s (mode=%s)", path, mode)
    writer.save(path)


def merge_delta(
    spark: SparkSession,
    updates: DataFrame,
    target_path: str,
    merge_keys: Iterable[str],
    update_columns: Optional[Iterable[str]] = None,
    delete_condition: Optional[str] = None,
    partition_by: Optional[Iterable[str]] = None,
) -> None:
    """Upsert a DataFrame into a Delta table using MERGE.

    If the target Delta table does not yet exist, the updates DataFrame is
    written out as the initial version (so this function is safe to call
    on the first run of a pipeline).

    Args:
        spark: Active SparkSession.
        updates: DataFrame containing new/updated rows.
        target_path: Path of the Delta table to merge into.
        merge_keys: Columns that identify a row (used in the MERGE condition).
        update_columns: Optional subset of columns to update on match.
            Defaults to "update all".
        delete_condition: Optional SQL expression; matched rows satisfying
            this condition will be deleted instead of updated.
        partition_by: Columns to partition by when creating the table for the
            first time.
    """
    keys = list(merge_keys)
    if not keys:
        raise ValueError("merge_keys must contain at least one column")

    if not DeltaTable.isDeltaTable(spark, target_path):
        logger.info("Target Delta table %s does not exist - creating it", target_path)
        write_delta(updates, target_path, mode="overwrite", partition_by=partition_by)
        return

    target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])

    merge_builder = target.alias("target").merge(updates.alias("source"), condition)

    if delete_condition:
        merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)

    if update_columns:
        update_map = {col: f"source.{col}" for col in update_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_map)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    logger.info("Merging into %s on keys=%s", target_path, keys)
    merge_builder.whenNotMatchedInsertAll().execute()
