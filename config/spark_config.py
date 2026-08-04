"""
Spark configuration.

Cluster resource settings such as driver memory and executor
memory should be supplied through spark-submit rather than
hardcoded here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SparkConfig:
    """Spark runtime configuration."""

    master: str = "local[*]"

    serializer: str = (
        "org.apache.spark.serializer.KryoSerializer"
    )

    shuffle_partitions: int = 8

    adaptive_execution: bool = True

    case_sensitive: bool = False

    catalog_impl: str = (
        "org.apache.iceberg.spark.SparkCatalog"
    )

    catalog_type: str = "hadoop"

    extensions: str = (
        "org.apache.iceberg.spark.extensions."
        "IcebergSparkSessionExtensions"
    )


spark_config = SparkConfig()