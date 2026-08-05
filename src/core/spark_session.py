"""
Spark session factory with Iceberg catalog config.
"""

from pathlib import Path

from pyspark.sql import SparkSession

from config import settings


class SparkSessionFactory:
    """Creates the local SparkSession with Iceberg enabled."""

    @staticmethod
    def create() -> SparkSession:
        Path(settings.WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)

        spark = (
            SparkSession.builder
            .appName(settings.APP_NAME)
            .master(settings.MASTER)
            .config("spark.driver.memory", settings.DRIVER_MEMORY)
            .config("spark.executor.memory", settings.EXECUTOR_MEMORY)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", settings.SHUFFLE_PARTITIONS)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.jars.packages", settings.ICEBERG_PACKAGE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{settings.ICEBERG_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.type",
                "hadoop",
            )
            .config(
                f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.warehouse",
                settings.WAREHOUSE_DIR,
            )
            .config(
                f"spark.sql.catalog.{settings.ICEBERG_CATALOG}.io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
