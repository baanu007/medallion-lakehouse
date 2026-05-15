"""Tests for ``src.common.io_utils`` (read / write / merge Delta)."""

from __future__ import annotations

from pyspark.sql import Row

from src.common.io_utils import merge_delta, read_delta, write_delta


def test_write_and_read_delta_roundtrip(spark, tmp_delta_dir):
    path = str(tmp_delta_dir / "orders")
    df = spark.createDataFrame(
        [Row(id=1, name="a"), Row(id=2, name="b")]
    )

    write_delta(df, path, mode="overwrite")
    out = read_delta(spark, path).orderBy("id").collect()

    assert [(r.id, r.name) for r in out] == [(1, "a"), (2, "b")]


def test_merge_delta_creates_table_when_missing(spark, tmp_delta_dir):
    path = str(tmp_delta_dir / "dim")
    df = spark.createDataFrame([Row(id=1, val="x")])
    # No table exists yet - merge_delta should bootstrap it.
    merge_delta(spark, df, path, merge_keys=["id"])

    out = read_delta(spark, path).collect()
    assert len(out) == 1
    assert out[0].val == "x"


def test_merge_delta_upserts(spark, tmp_delta_dir):
    path = str(tmp_delta_dir / "dim")
    initial = spark.createDataFrame(
        [Row(id=1, val="x"), Row(id=2, val="y")]
    )
    write_delta(initial, path, mode="overwrite")

    updates = spark.createDataFrame(
        [Row(id=2, val="Y_NEW"), Row(id=3, val="z")]
    )
    merge_delta(spark, updates, path, merge_keys=["id"])

    out = {r.id: r.val for r in read_delta(spark, path).collect()}
    assert out == {1: "x", 2: "Y_NEW", 3: "z"}


def test_read_delta_version_as_of(spark, tmp_delta_dir):
    path = str(tmp_delta_dir / "versioned")
    write_delta(spark.createDataFrame([Row(id=1)]), path, mode="overwrite")
    write_delta(spark.createDataFrame([Row(id=2)]), path, mode="append")

    v0 = read_delta(spark, path, version=0).collect()
    assert [r.id for r in v0] == [1]
