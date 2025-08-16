# pysparktest

A PySpark testing utilities library that provides pytest fixtures and utilities for testing PySpark applications with Iceberg support. The library includes SQL migration capabilities for setting up test data and schemas.

## Features

- **Pytest fixtures** for SparkSession creation with Iceberg configuration
- **SQL migration runner** for executing .sql files in sorted order
- **Base test classes** for convenient SparkSession access
- **Automatic cleanup** of temporary warehouse directories
- **Flexible testing patterns** supporting both fixture-based and class-based approaches

## Installation

Install as a git dependency using `uv`:

```bash
# Add from GitHub (HTTPS)
uv add git+https://github.com/ryanmiville/pysparktest.git

# Add from GitHub (SSH)
uv add git+ssh://git@github.com/ryanmiville/pysparktest.git
```

## Quick Start

### Basic Usage

```python
from pysparktest import SparkTestCase

class TestWithMigrations(SparkTestCase):
    migrations_path = "/path/to/your/sql/migrations"
    
    def test_migrated_tables(self, spark_suite):
        # Your migration SQL files have been executed
        df = self.spark.table("your_database.your_table")
        assert df.count() >= 0
```

## Requirements

- Python e3.13
- PySpark e4.0.0
- Apache Iceberg Spark extensions (configured automatically)