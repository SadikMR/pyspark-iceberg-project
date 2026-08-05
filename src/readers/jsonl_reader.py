"""
JSON Lines reader.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class JsonlReader:
    """Reads JSONL files into a Spark DataFrame."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(
        self,
        file_path: str,
        updated_from: str | None = None,
        updated_to: str | None = None,
    ) -> DataFrame:
        df = self.spark.read.json(file_path)

        if updated_from:
            df = df.filter(F.to_date("updated") >= F.lit(updated_from))

        if updated_to:
            df = df.filter(F.to_date("updated") <= F.lit(updated_to))

        return df
