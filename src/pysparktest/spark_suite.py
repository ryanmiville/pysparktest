"""Pytest fixtures for Spark testing with Iceberg support and migrations."""

import tempfile
from typing import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a SparkSession configured for testing with Iceberg support.

    This fixture creates a local SparkSession with the same configuration
    as the Scala SparkSuite, including Iceberg catalog support.
    """
    catalog_name = "spark_catalog"
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")

    # Determine catalog implementation
    catalog_impl = (
        "SparkSessionCatalog" if catalog_name == "spark_catalog" else "SparkCatalog"
    )

    iceberg_version = "1.9.2"

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pytest-spark-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.jars.packages",
            f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg_version}",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}",
            f"org.apache.iceberg.spark.{catalog_impl}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_dir)
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    # Set log level to reduce noise during testing
    spark.sparkContext.setLogLevel("ERROR")

    try:
        yield spark
    finally:
        spark.stop()


def pytest_configure(config):
    """Add custom markers for spark testing."""
    config.addinivalue_line(
        "markers", "spark: mark test as requiring Spark functionality"
    )
    config.addinivalue_line(
        "markers",
        "migrations(path): mark test as requiring migrations from specified path",
    )
