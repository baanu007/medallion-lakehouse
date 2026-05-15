"""Tests for ``src.silver.clean_orders.OrdersCleaner``.

These tests run against a local Spark session and a temp Delta directory;
no AWS/Snowflake access is required.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pyspark.sql import Row

from src.common.io_utils import read_delta, write_delta
from src.silver.clean_orders import OrdersCleaner


def _make_bronze_df(spark):
    """Build a Bronze-shaped DataFrame containing edge cases.

    Edge cases covered:
    * Mixed casing in ``order_status`` / ``payment_method`` (cleansed)
    * Duplicate ``order_id`` with different ``_ingestion_timestamp`` (deduped)
    * A ``customer_id`` starting with TEST (dropped by business rules)
    * An invalid status (dropped by business rules)
    """
    return spark.createDataFrame(
        [
            Row(
                order_id="ORD-001",
                customer_id="CUST-1",
                product_id="PROD-A",
                order_date="2024-01-15",
                order_timestamp="2024-01-15 10:00:00",
                quantity=2,
                unit_price=Decimal("10.00"),
                discount_amount=Decimal("0.00"),
                order_status="completed",
                payment_method="credit_card",
                _ingestion_timestamp=datetime(2024, 1, 15, 10, 30),
            ),
            # Duplicate of ORD-001, newer ingestion - should win
            Row(
                order_id="ORD-001",
                customer_id="CUST-1",
                product_id="PROD-A",
                order_date="2024-01-15",
                order_timestamp="2024-01-15 10:00:00",
                quantity=2,
                unit_price=Decimal("10.00"),
                discount_amount=Decimal("0.00"),
                order_status="shipped",
                payment_method="credit_card",
                _ingestion_timestamp=datetime(2024, 1, 15, 12, 0),
            ),
            Row(
                order_id="ORD-002",
                customer_id="CUST-2",
                product_id="PROD-B",
                order_date="2024-01-16",
                order_timestamp="2024-01-16 09:00:00",
                quantity=1,
                unit_price=Decimal("50.00"),
                discount_amount=Decimal("5.00"),
                order_status="DELIVERED",
                payment_method="PayPal",
                _ingestion_timestamp=datetime(2024, 1, 16, 9, 30),
            ),
            # Test order - should be dropped
            Row(
                order_id="ORD-003",
                customer_id="TEST-001",
                product_id="PROD-C",
                order_date="2024-01-16",
                order_timestamp="2024-01-16 10:00:00",
                quantity=1,
                unit_price=Decimal("20.00"),
                discount_amount=Decimal("0.00"),
                order_status="DELIVERED",
                payment_method="cash",
                _ingestion_timestamp=datetime(2024, 1, 16, 10, 30),
            ),
            # Invalid status - should be dropped
            Row(
                order_id="ORD-004",
                customer_id="CUST-4",
                product_id="PROD-D",
                order_date="2024-01-16",
                order_timestamp="2024-01-16 11:00:00",
                quantity=1,
                unit_price=Decimal("20.00"),
                discount_amount=Decimal("0.00"),
                order_status="WHO_KNOWS",
                payment_method="cash",
                _ingestion_timestamp=datetime(2024, 1, 16, 11, 30),
            ),
        ]
    )


def test_deduplicate_keeps_latest_ingestion(spark):
    cleaner = OrdersCleaner(spark, "bronze", "silver")
    deduped = cleaner.deduplicate(_make_bronze_df(spark))

    by_id = {r.order_id: r for r in deduped.collect()}
    # ORD-001 must be the newer row with status "shipped"
    assert by_id["ORD-001"].order_status == "shipped"


def test_clean_data_standardizes_and_computes_amounts(spark):
    cleaner = OrdersCleaner(spark, "bronze", "silver")
    cleaned = cleaner.clean_data(_make_bronze_df(spark)).collect()

    statuses = {r.order_status for r in cleaned}
    # All statuses must be uppercased & trimmed
    assert all(s == s.upper() for s in statuses)

    ord_002 = next(r for r in cleaned if r.order_id == "ORD-002")
    assert ord_002.gross_amount == Decimal("50.00")
    assert ord_002.net_amount == Decimal("45.00")


def test_business_rules_drop_test_and_invalid_rows(spark):
    cleaner = OrdersCleaner(spark, "bronze", "silver")
    df = cleaner.clean_data(_make_bronze_df(spark))
    out = cleaner.apply_business_rules(df).collect()
    ids = {r.order_id for r in out}

    assert "ORD-003" not in ids, "TEST customer should be dropped"
    assert "ORD-004" not in ids, "Invalid status should be dropped"
    assert {"ORD-001", "ORD-002"}.issubset(ids)


def test_end_to_end_writes_silver_delta(spark, tmp_delta_dir):
    bronze_path = str(tmp_delta_dir / "bronze_orders")
    silver_path = str(tmp_delta_dir / "silver_orders")

    write_delta(_make_bronze_df(spark), bronze_path, mode="overwrite")

    OrdersCleaner(spark, bronze_path, silver_path).run(incremental=False)

    silver = read_delta(spark, silver_path).collect()
    ids = {r.order_id for r in silver}
    assert ids == {"ORD-001", "ORD-002"}
    # Audit columns should be present
    cols = set(read_delta(spark, silver_path).columns)
    assert "_silver_timestamp" in cols
    assert "_source_layer" in cols
