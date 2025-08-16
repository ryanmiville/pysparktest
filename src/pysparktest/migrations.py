"""Migration runner for executing SQL files in sorted order."""

from pathlib import Path
from typing import List

try:
    from importlib.resources import files
except ImportError:
    from importlib_resources import files

from pyspark.sql import SparkSession


def run_migrations(migrations_path: str, spark: SparkSession) -> None:
    """
    Recursively walks the specified path to find all .sql files and runs them.
    Note that the migrations are expected to be in the resources directory.

    Args:
        migrations_path: The path to the migrations (e.g., "/path/to/migrations")
        spark: The Spark session to use for executing SQL
    """
    migration_files = _list_migration_files(migrations_path)
    sorted_migrations = sorted(migration_files)

    for migration_file in sorted_migrations:
        content = _read_migration(migration_file)
        if content.strip():
            spark.sql(content)


def _read_migration(resource_path: str) -> str:
    """Read the contents of a migration file from resources."""
    # Remove leading slash if present
    clean_path = resource_path.lstrip("/")

    try:
        # Try to read from package resources first
        resource_parts = clean_path.split("/")
        if len(resource_parts) > 1:
            package_name = resource_parts[0]
            file_path = "/".join(resource_parts[1:])
            resource_files = files(package_name)
            for part in file_path.split("/")[:-1]:
                resource_files = resource_files / part
            migration_file = resource_files / file_path.split("/")[-1]
            if migration_file.is_file():
                return migration_file.read_text(encoding="utf-8")
    except (ImportError, FileNotFoundError, AttributeError):
        pass

    # Fall back to reading from filesystem
    try:
        file_path = Path(clean_path)
        if file_path.exists():
            return file_path.read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError):
        pass

    raise FileNotFoundError(f"Migration file not found: {resource_path}")


def _list_migration_files(root_path: str) -> List[str]:
    """List all .sql files in the given path."""
    clean_path = root_path.lstrip("/")
    migration_files = []

    try:
        # Try to list from package resources first
        resource_parts = clean_path.split("/")
        if len(resource_parts) >= 1:
            package_name = resource_parts[0]
            subpath = "/".join(resource_parts[1:]) if len(resource_parts) > 1 else ""

            resource_files = files(package_name)
            if subpath:
                for part in subpath.split("/"):
                    resource_files = resource_files / part

            if resource_files.is_dir():
                migration_files.extend(
                    _walk_resource_directory(resource_files, f"/{clean_path}")
                )
    except (ImportError, FileNotFoundError, AttributeError):
        pass

    # Fall back to filesystem
    if not migration_files:
        try:
            path = Path(clean_path)
            if path.exists() and path.is_dir():
                migration_files.extend(
                    _walk_filesystem_directory(path, f"/{clean_path}")
                )
        except (FileNotFoundError, PermissionError):
            pass

    return [f for f in migration_files if f.endswith(".sql")]


def _walk_resource_directory(resource_dir, prefix: str) -> List[str]:
    """Walk a resource directory and return all file paths."""
    files_list = []
    try:
        for item in resource_dir.iterdir():
            item_path = f"{prefix}/{item.name}"
            if item.is_file():
                files_list.append(item_path)
            elif item.is_dir():
                files_list.extend(_walk_resource_directory(item, item_path))
    except AttributeError:
        pass
    return files_list


def _walk_filesystem_directory(directory: Path, prefix: str) -> List[str]:
    """Walk a filesystem directory and return all file paths."""
    files_list = []
    try:
        for item in directory.rglob("*"):
            if item.is_file():
                relative_path = item.relative_to(directory)
                files_list.append(f"{prefix}/{relative_path}")
    except (FileNotFoundError, PermissionError):
        pass
    return files_list
