"""
Mapping file reader.

Mapping JSON files are plain objects, for example:
  {"m": "mobile", "d": "desktop"}

Loaded into a DataFrame with columns: key, value
(for joining — those names are not required inside the JSON file).
"""

import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class MappingReader:
    """Reads object-style mapping JSON into a two-column DataFrame."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def read(self, file_path: str) -> DataFrame:
        data = json.loads(Path(file_path).read_text(encoding="utf-8"))
        rows = [(str(k), str(v)) for k, v in data.items()]
        return self.spark.createDataFrame(rows, ["key", "value"])
