"""
SparkSession factory.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import settings
from src.core.logger import get_logger

logger = get_logger(__name__)


class SparkSessionFactory:
    """Factory for creating a SparkSession."""

    @staticmethod
    def create() -> SparkSession:

        logger.info("Creating SparkSession...")

        spark = (
            SparkSession.builder
            .appName(settings.app_name)
            .master("local[*]")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession created successfully.")

        return spark