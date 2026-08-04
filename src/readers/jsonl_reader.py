"""
JSON Lines reader.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date

from src.core.logger import get_logger

logger = get_logger(__name__)


class JsonlReader:
    """Reads JSON Lines files into a Spark DataFrame."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def read(
        self,
        file_path: str,
        *,
        updated_from: str | None = None,
        updated_to: str | None = None,
    ) -> DataFrame:
  
        logger.info("Reading JSONL file: %s", file_path)

        dataframe = (
            self._spark.read
            .option("multiLine", "false")
            .json(file_path)
        )

        if updated_from is not None:
            logger.info("Filtering from date: %s", updated_from)

            dataframe = dataframe.filter(
                to_date(col("updated")) >= to_date(lit(updated_from))
            )

        if updated_to is not None:
            logger.info("Filtering to date: %s", updated_to)

            dataframe = dataframe.filter(
                to_date(col("updated")) <= to_date(lit(updated_to))
            )

        return dataframe