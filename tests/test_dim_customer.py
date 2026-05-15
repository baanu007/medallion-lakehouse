"""Tests for ``src.gold.dim_customer`` SCD2 logic.

Focus areas (matching the interview-defense fixes):

1. NULL-safe change detection - transitions involving NULLs must be
   flagged as a change (NULL -> value, value -> NULL).
2. Surrogate key cycle stability - A -> B -> A must produce three
   distinct ``customer_sk`` values (the second A must not reuse the
   original A's key).
3. The ``build_new_versions`` projection emits the expected schema.

These tests use a real local SparkSession (via the ``spark`` fixture)
because the change-detection logic is expressed as a Spark SQL
expression that's hard to validate without a real evaluator.
"""

from __future__ import annotations

import time
from datetime import date

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.gold.dim_customer import (
    TRACKED_ATTRIBUTES,
    DimCustomerBuilder,
)


pytestmark = pytest.mark.usefixtures("spark")


def _silver_row(
    customer_id: str,
    first_name=None,
    last_name=None,
    email=None,
    country=None,
    signup_date=date(2026, 1, 1),
):
    return Row(
        customer_id=customer_id,
        first_name=first_name,
        last_name=last_name,
        email=email,
        country=country,
        signup_date=signup_date,
    )


def test_surrogate_key_cycle_stability(spark):
    """A -> B -> A must produce three *distinct* surrogate keys.

    Without ``effective_from`` in the hash input, the second ``A`` row
    would collide with the original ``A`` row's SK and silently corrupt
    the SCD2 history.
    """
    silver_a1 = spark.createDataFrame(
        [_silver_row("c1", "Ann", "Lee", "ann@x.com", "US")]
    )
    sk_a1 = DimCustomerBuilder.build_new_versions(silver_a1).collect()[0].customer_sk

    # Ensure effective_from advances (current_timestamp granularity is ms).
    time.sleep(0.01)
    silver_b = spark.createDataFrame(
        [_silver_row("c1", "Ann", "Lee", "ann@x.com", "CA")]
    )
    sk_b = DimCustomerBuilder.build_new_versions(silver_b).collect()[0].customer_sk

    time.sleep(0.01)
    silver_a2 = spark.createDataFrame(
        [_silver_row("c1", "Ann", "Lee", "ann@x.com", "US")]
    )
    sk_a2 = DimCustomerBuilder.build_new_versions(silver_a2).collect()[0].customer_sk

    assert sk_a1 != sk_b, "A and B must hash to different surrogate keys"
    assert sk_b != sk_a2, "B and second-A must differ"
    assert sk_a1 != sk_a2, (
        "Second A must NOT reuse the original A's surrogate key "
        "(SCD2 cycle stability bug)."
    )


def test_build_new_versions_schema(spark):
    silver = spark.createDataFrame(
        [_silver_row("c1", "Ann", "Lee", "ann@x.com", "US")]
    )
    out = DimCustomerBuilder.build_new_versions(silver)

    expected_cols = {
        "customer_id",
        *TRACKED_ATTRIBUTES,
        "signup_date",
        "customer_sk",
        "effective_from",
        "effective_to",
        "is_current",
    }
    assert set(out.columns) == expected_cols

    row = out.collect()[0]
    assert row.is_current is True
    assert row.effective_to is None
    assert row.customer_sk is not None and len(row.customer_sk) == 64  # sha256 hex


# --------------------------------------------------------------------------
# NULL-safe change detection.
#
# We exercise the same change_condition string the builder constructs in
# ``_apply_scd2`` against a hand-rolled join of fake current / incoming
# rows. This lets us verify the boolean logic without a Delta table.
# --------------------------------------------------------------------------


def _build_change_condition() -> str:
    """Replicate the change_condition expression from ``_apply_scd2``.

    Kept in sync with ``src/gold/dim_customer.py``.
    """
    return " OR ".join(
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


@pytest.mark.parametrize(
    "cur_country,inc_country,expected_change",
    [
        ("US", "CA", True),    # value -> value (different)
        ("US", "US", False),   # unchanged
        (None, "US", True),    # NULL -> value
        ("US", None, True),    # value -> NULL
        (None, None, False),   # both NULL - no change
    ],
)
def test_change_condition_null_semantics(spark, cur_country, inc_country, expected_change):
    cur = spark.createDataFrame(
        [Row(customer_id="c1", first_name="A", last_name="L", email="a@x", country=cur_country)]
    ).alias("cur")
    inc = spark.createDataFrame(
        [Row(customer_id="c1", first_name="A", last_name="L", email="a@x", country=inc_country)]
    ).alias("inc")

    joined = cur.join(inc, on="customer_id", how="inner")
    changed = joined.filter(_build_change_condition())

    actual_change = changed.count() == 1
    assert actual_change is expected_change, (
        f"NULL-distinct change detection wrong for "
        f"({cur_country!r} -> {inc_country!r}): "
        f"expected change={expected_change}, got {actual_change}"
    )


def test_change_condition_detects_email_change_with_other_nulls(spark):
    """A real change in one attr must still fire even when other attrs are NULL on both sides."""
    cur = spark.createDataFrame(
        [Row(customer_id="c1", first_name=None, last_name=None, email="old@x", country=None)]
    ).alias("cur")
    inc = spark.createDataFrame(
        [Row(customer_id="c1", first_name=None, last_name=None, email="new@x", country=None)]
    ).alias("inc")
    joined = cur.join(inc, on="customer_id", how="inner")
    assert joined.filter(_build_change_condition()).count() == 1
