"""
Spark session factory.
"""

from pyspark.sql import SparkSession

from config import settings


class SparkSessionFactory:
    """Creates the local SparkSession."""

    @staticmethod
    def create() -> SparkSession:
        spark = (
            SparkSession.builder
            .appName(settings.APP_NAME)
            .master(settings.MASTER)
            .config("spark.driver.memory", settings.DRIVER_MEMORY)
            .config("spark.executor.memory", settings.EXECUTOR_MEMORY)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", settings.SHUFFLE_PARTITIONS)
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
