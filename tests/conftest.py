"""Pytest configuration and fixtures for Spark testing."""

# Import all fixtures from spark_suite to make them available to tests
from pysparktest.spark_suite import (  # noqa: F401
    pytest_configure,
    spark_session,
    spark_suite,
)
