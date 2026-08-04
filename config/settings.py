"""
Application configuration.

This module contains application-level configuration.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True, slots=True)
class PathSettings:
    """Project paths."""

    project_root: Path = PROJECT_ROOT

    input_file: Path = PROJECT_ROOT / "data" / "raw" / "bookings.jsonl"

    warehouse: Path = PROJECT_ROOT / "data" / "warehouse"

    mappings: Path = PROJECT_ROOT / "mappings"

    device_mapping: Path = mappings / "device.json"

    booking_status_mapping: Path = mappings / "booking_status.json"

    country_region_mapping: Path = mappings / "country_region.json"


@dataclass(frozen=True, slots=True)
class IcebergSettings:
    """Iceberg configuration."""

    catalog: str = "local"

    database: str = "default"

    table: str = "bookings"

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass(frozen=True, slots=True)
class AppSettings:
    """Application configuration."""

    app_name: str = "PySpark Iceberg ETL"

    paths: PathSettings = PathSettings()

    iceberg: IcebergSettings = IcebergSettings()


settings = AppSettings()