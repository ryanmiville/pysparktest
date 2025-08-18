"""Example test demonstrating usage of the Spark testing framework."""

import pytest

from pysparktest import run_migrations


@pytest.fixture(autouse=True, scope="module")
def spark(spark_session):
    run_migrations("tests/migrations", spark_session)
    data = [
        (1, "Alice", "2025-01-01"),
        (2, "Bob", "2025-01-02"),
        (3, "Charlie", "2025-01-03"),
    ]
    columns = ["id", "name", "date"]

    df = spark_session.createDataFrame(data, columns)
    df.writeTo("my_db.my_table").append()

    return spark_session


def test_spark_session_with_migrations(spark):
    """Test that SparkSession is available and migrations work."""
    # Test basic SQL functionality
    result = spark.sql("SELECT * FROM my_db.my_table WHERE id > 2")
    assert result.count() == 1


def test_spark(spark):
    result = spark.read.table("my_db.my_table")
    assert result.count() == 3
