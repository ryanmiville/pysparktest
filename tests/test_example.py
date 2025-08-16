"""Example test demonstrating usage of the Spark testing framework."""

import pytest

from pysparktest.spark_suite import SparkTestCase


class TestSparkSuiteExample(SparkTestCase):
    """Example test class using SparkTestCase base class."""

    migrations_path = "tests/migrations"

    def test_spark_session_available(self, spark_suite):
        """Test that SparkSession is available and migrations work."""
        data = [
            (1, "Alice", "2025-01-01"),
            (2, "Bob", "2025-01-02"),
            (3, "Charlie", "2025-01-03"),
        ]
        columns = ["id", "name", "date"]

        df = self.spark.createDataFrame(data, columns)
        df.writeTo("my_db.my_table").overwritePartitions()

        # Test basic SQL functionality
        result = self.spark.sql("SELECT * FROM my_db.my_table WHERE id > 2")
        assert result.count() == 1


# Example of using fixture-based approach
def test_spark_with_fixture(spark_session):
    """Example test using the spark_session fixture directly."""
    # Create and test a simple DataFrame
    data = [("test1", 1), ("test2", 2)]
    df = spark_session.createDataFrame(data, ["name", "value"])

    assert df.count() == 2
    assert "name" in df.columns
    assert "value" in df.columns


# Example of creating a custom fixture with migrations
# Uncomment when you have actual migration files
# my_migrations_suite = create_spark_suite_fixture("tests/migrations")


# def test_with_custom_migrations(my_migrations_suite):
#     """Example test using custom migrations fixture."""
#     spark = my_migrations_suite

#     # Your tables from migrations should be available here
#     df = spark.table("your_database.your_table")
#     assert df.count() > 0


@pytest.mark.spark
def test_marked_spark_test(spark_session):
    """Example of using the spark marker for test organization."""
    # This test is marked with @pytest.mark.spark
    # You can run only spark tests with: pytest -m spark

    result = spark_session.sql("SELECT 1 as test_value")
    assert result.collect()[0]["test_value"] == 1
