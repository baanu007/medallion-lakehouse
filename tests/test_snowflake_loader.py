"""Tests for ``src.snowflake.load_to_snowflake``.

These tests focus on the data-shape guarantees of the pandas loader path
(specifically: pandas columns are uppercased before ``write_pandas`` so
they match the UPPERCASE Snowflake DDL). Spark / Snowflake connectivity
is fully mocked so the suite runs without either dependency.
"""

from __future__ import annotations

import sys
import types
from unittest import mock

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Stub out ``snowflake.connector`` so importing the loader doesn't require
# the real package at test time. The actual ``write_pandas`` call is patched
# per-test below.
# ---------------------------------------------------------------------------


def _install_snowflake_stub() -> None:
    if "snowflake" not in sys.modules:
        sys.modules["snowflake"] = types.ModuleType("snowflake")

    if "snowflake.connector" not in sys.modules:
        connector_mod = types.ModuleType("snowflake.connector")

        def _connect(**_kwargs):  # pragma: no cover - replaced per test
            raise RuntimeError("snowflake.connector.connect not patched in this test")

        connector_mod.connect = _connect
        sys.modules["snowflake.connector"] = connector_mod
        sys.modules["snowflake"].connector = connector_mod

    if "snowflake.connector.pandas_tools" not in sys.modules:
        pandas_tools_mod = types.ModuleType("snowflake.connector.pandas_tools")

        def _write_pandas(*_args, **_kwargs):  # pragma: no cover - patched per test
            return (True, 1, 0, [])

        pandas_tools_mod.write_pandas = _write_pandas
        sys.modules["snowflake.connector.pandas_tools"] = pandas_tools_mod
        sys.modules["snowflake.connector"].pandas_tools = pandas_tools_mod


_install_snowflake_stub()

from src.snowflake import load_to_snowflake  # noqa: E402  (after stub install)


@pytest.fixture()
def fake_config():
    """Return a SnowflakeConfig instance without touching real env vars."""
    return load_to_snowflake.SnowflakeConfig(
        account="acc",
        user="usr",
        password="pwd",
        role=None,
        warehouse="wh",
        database="db",
        schema="sch",
    )


def _fake_spark_with_pandas(pdf: pd.DataFrame):
    """Build a chain of MagicMocks that mimic ``spark.read.format(...).load(...).toPandas()``."""
    spark = mock.MagicMock()
    reader = mock.MagicMock()
    df = mock.MagicMock()
    spark.read.format.return_value = reader
    reader.load.return_value = df
    df.toPandas.return_value = pdf
    return spark


def test_load_via_pandas_uppercases_columns_before_write(monkeypatch, fake_config):
    """Regression: write_pandas must receive UPPERCASE columns to match DDL."""
    # Delta hands us lower_snake_case columns - that's the bug surface.
    pdf = pd.DataFrame(
        {
            "customer_id": [1, 2],
            "first_name": ["a", "b"],
            "net_amount": [10.0, 20.0],
        }
    )
    spark = _fake_spark_with_pandas(pdf)

    monkeypatch.setattr(
        load_to_snowflake,
        "build_spark_session",
        lambda **kwargs: spark,
    )

    fake_conn = mock.MagicMock()
    fake_conn.__enter__.return_value = fake_conn
    fake_conn.__exit__.return_value = False

    write_pandas_mock = mock.MagicMock(return_value=(True, 1, len(pdf), []))

    with mock.patch(
        "snowflake.connector.connect", return_value=fake_conn
    ), mock.patch(
        "snowflake.connector.pandas_tools.write_pandas",
        write_pandas_mock,
    ):
        rows = load_to_snowflake.load_via_pandas(
            gold_path="/tmp/gold/dim_customer",
            table="DIM_CUSTOMER",
            config=fake_config,
            mode="append",
        )

    assert rows == len(pdf)
    assert write_pandas_mock.call_count == 1

    _, kwargs = write_pandas_mock.call_args
    sent_df = kwargs["df"]
    assert list(sent_df.columns) == ["CUSTOMER_ID", "FIRST_NAME", "NET_AMOUNT"], (
        "write_pandas received non-uppercase columns; Snowflake COPY INTO "
        "would fail with 'invalid column name'."
    )
    assert kwargs["table_name"] == "DIM_CUSTOMER"
    assert kwargs["auto_create_table"] is False


def test_load_via_pandas_overwrite_truncates(monkeypatch, fake_config):
    """``mode='overwrite'`` should issue TRUNCATE before writing."""
    pdf = pd.DataFrame({"customer_id": [1]})
    spark = _fake_spark_with_pandas(pdf)
    monkeypatch.setattr(
        load_to_snowflake, "build_spark_session", lambda **kwargs: spark
    )

    cursor = mock.MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.__exit__.return_value = False

    conn = mock.MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    conn.cursor.return_value = cursor

    with mock.patch(
        "snowflake.connector.connect", return_value=conn
    ), mock.patch(
        "snowflake.connector.pandas_tools.write_pandas",
        return_value=(True, 1, 1, []),
    ):
        load_to_snowflake.load_via_pandas(
            gold_path="/tmp/gold/dim_customer",
            table="DIM_CUSTOMER",
            config=fake_config,
            mode="overwrite",
        )

    # TRUNCATE issued exactly once on the cursor.
    truncate_calls = [
        c for c in cursor.execute.call_args_list
        if "TRUNCATE" in str(c).upper()
    ]
    assert len(truncate_calls) == 1
