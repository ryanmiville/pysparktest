"""Example test demonstrating usage of the Spark testing framework."""

import pytest
from pysparktest.spark_suite import SparkTestCase, create_spark_suite_fixture


class TestSparkSuiteExample(SparkTestCase):
    """Example test class using SparkTestCase base class."""
    
    # migrations_path = "/path/to/your/migrations"  # Uncomment and set when you have migrations
    
    def test_spark_session_available(self, spark_suite):
        """Test that SparkSession is available and working."""
        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        
        df = self.spark.createDataFrame(data, columns)
        
        assert df.count() == 3
        assert df.columns == ["name", "age"]
        
        # Test basic SQL functionality
        df.createOrReplaceTempView("people")
        result = self.spark.sql("SELECT * FROM people WHERE age > 25")
        assert result.count() == 2
    
    def test_iceberg_configuration(self, spark_suite):
        """Test that Iceberg configuration is properly set up."""
        # Check that Iceberg configurations are present
        conf = self.spark.conf
        
        # These should be set by our spark_suite fixture
        assert "org.apache.iceberg.spark" in conf.get("spark.sql.catalog.spark_catalog")
        assert "IcebergSparkSessionExtensions" in conf.get("spark.sql.extensions")
        assert conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"


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
# my_migrations_suite = create_spark_suite_fixture("/path/to/my/migrations")
# 
# def test_with_custom_migrations(my_migrations_suite):
#     """Example test using custom migrations fixture."""
#     spark = my_migrations_suite
#     
#     # Your tables from migrations should be available here
#     # df = spark.table("your_database.your_table")
#     # assert df.count() > 0


@pytest.mark.spark
def test_marked_spark_test(spark_session):
    """Example of using the spark marker for test organization."""
    # This test is marked with @pytest.mark.spark
    # You can run only spark tests with: pytest -m spark
    
    result = spark_session.sql("SELECT 1 as test_value")
    assert result.collect()[0]["test_value"] == 1


class TestMigrationExample:
    """Example showing how to use migrations without inheritance."""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark_session):
        """Set up test with migrations."""
        self.spark = spark_session
        # Uncomment when you have migrations:
        # from pysparktest.migrations import run_migrations
        # run_migrations("/path/to/migrations", spark_session)
    
    def test_migrations_loaded(self):
        """Test that would verify migrations were loaded."""
        # After migrations run, your tables should be available:
        # df = self.spark.table("your_database.your_table") 
        # assert df.count() >= 0
        
        # For now, just test that spark is working
        assert self.spark.sql("SELECT 1").collect()[0][0] == 1