"""Pytest fixtures for Spark testing with Iceberg support and migrations."""

import shutil
import tempfile
from typing import Generator, Optional

import pytest
from pyspark.sql import SparkSession

from .migrations import run_migrations


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

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("pytest-spark-test")
        .config("spark.sql.shuffle.partitions", "1")
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
        # Clean up warehouse directory
        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def spark_suite(spark_session: SparkSession, migrations_path: Optional[str] = None):
    """
    Pytest fixture that provides a configured SparkSession with migrations run.

    Args:
        spark_session: The SparkSession fixture
        migrations_path: Optional path to migrations directory. If not provided,
                        the test class should define a migrations_path attribute
                        or this fixture will be skipped.

    Usage:
        @pytest.mark.parametrize("migrations_path", ["/path/to/migrations"], indirect=True)
        def test_something(spark_suite):
            spark = spark_suite
            # Use spark session with migrations already run
    """
    # If migrations_path is provided, run migrations
    if migrations_path:
        run_migrations(migrations_path, spark_session)

    yield spark_session


def pytest_configure(config):
    """Add custom markers for spark testing."""
    config.addinivalue_line(
        "markers", "spark: mark test as requiring Spark functionality"
    )
    config.addinivalue_line(
        "markers",
        "migrations(path): mark test as requiring migrations from specified path",
    )


class SparkTestCase:
    """
    Base class for Spark test cases that provides convenient access to SparkSession.

    This class can be used as a mixin or base class for test classes that need
    access to a configured SparkSession. It follows pytest conventions while
    providing similar functionality to the Scala SparkSuite trait.

    Usage:
        class TestMySparkCode(SparkTestCase):
            migrations_path = "/path/to/migrations"

            def test_something(self, spark_suite):
                df = self.spark.table("my_table")
                assert df.count() > 0
    """

    migrations_path: Optional[str] = None

    @pytest.fixture(autouse=True)
    def setup_spark(self, spark_session: SparkSession):
        """Automatically inject SparkSession into test instance."""
        self.spark = spark_session
        if self.migrations_path:
            run_migrations(self.migrations_path, spark_session)


# Convenience function for direct fixture usage
def create_spark_suite_fixture(migrations_path: str):
    """
    Create a custom spark_suite fixture with a specific migrations path.

    Args:
        migrations_path: Path to the migrations directory

    Returns:
        A pytest fixture that provides a SparkSession with migrations run

    Usage:
        # In conftest.py or test file
        my_spark_suite = create_spark_suite_fixture("/my/migrations/path")

        def test_something(my_spark_suite):
            spark = my_spark_suite
            # Use spark with migrations
    """

    @pytest.fixture
    def custom_spark_suite(spark_session: SparkSession):
        run_migrations(migrations_path, spark_session)
        return spark_session

    return custom_spark_suite
