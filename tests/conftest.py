"""Shared pytest fixtures for the medallion-lakehouse test suite."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Iterator

import pytest

from src.common.spark_session import build_spark_session


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local Spark with Delta enabled."""
    session = build_spark_session(
        app_name="medallion-lakehouse-tests",
        master="local[2]",
        extra_conf={
            "spark.sql.shuffle.partitions": "2",
            "spark.ui.enabled": "false",
            "spark.driver.host": "127.0.0.1",
            # Required for Delta to function on a vanilla local Spark.
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.1.0",
        },
    )
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta_dir() -> Iterator[Path]:
    """Function-scoped temporary directory for Delta tables."""
    path = Path(tempfile.mkdtemp(prefix="delta-test-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
