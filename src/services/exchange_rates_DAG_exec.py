"""
Exchange rates using a Spark UDF.

NOTE:
This implementation is for learning purposes.
The API call happens inside the Spark DAG on the executors.
Each Python worker has its own cache.
"""

import requests
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


class ExchangeRateService:
    """Get USD exchange rates using a Spark UDF."""

    @staticmethod
    def add_rate_column(df: DataFrame) -> DataFrame:
        """Add a rate_to_usd column by calling the exchange-rate API."""

        cache: dict[str, float] = {"USD": 1.0}

        @F.udf(returnType=T.DoubleType())
        def get_rate(currency: str) -> float:
            if currency is None:
                return None

            if currency in cache:
                return cache[currency]

            url = f"https://open.er-api.com/v6/latest/{currency}"
            response = requests.get(url, timeout=20)
            response.raise_for_status()

            rate = float(response.json()["rates"]["USD"])
            cache[currency] = rate
            return rate

        return df.withColumn(
            "rate_to_usd",
            get_rate(F.col("currency")),
        )