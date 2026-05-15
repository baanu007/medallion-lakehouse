"""Tests for ``src.common.spark_session``."""

from __future__ import annotations

from src.common.spark_session import build_spark_session


def test_spark_session_has_delta_extensions(spark):
    """Delta SQL extension must be registered on the session."""
    extensions = spark.conf.get("spark.sql.extensions")
    assert "DeltaSparkSessionExtension" in extensions


def test_spark_session_uses_delta_catalog(spark):
    catalog = spark.conf.get("spark.sql.catalog.spark_catalog")
    assert catalog == "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def test_spark_session_is_singleton(spark):
    """getOrCreate must return the same session for the same app."""
    other = build_spark_session(app_name="medallion-lakehouse-tests")
    assert other is spark


def test_extra_conf_overrides_defaults(spark):
    # Default is "200"; we set "2" in the fixture.
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
