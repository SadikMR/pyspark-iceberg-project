"""
Exchange rates from API, with an in-memory cache.
"""

import requests
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from src.utils.timing import timed


class ExchangeRateService:
    """Get USD rates for currencies found in the data."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._cache: dict[str, float] = {"USD": 1.0}

    def get_rates_dataframe(self, bookings: DataFrame) -> DataFrame:
        """Build rates DataFrame from distinct currency values in bookings."""

        currencies = [
            row["currency"]
            for row in bookings.select("currency").distinct().collect()
            if row["currency"]
        ]
        rows = self._fetch_rates_from_api(currencies)
        return self.spark.createDataFrame(rows, ["currency", "rate_to_usd"])

    @timed("fx_api")
    def _fetch_rates_from_api(self, currencies: list[str]) -> list[tuple[str, float]]:
        """HTTP only — compare this timer to DAG fx_api."""

        return [(currency, self.get_rate(currency)) for currency in currencies]

    def get_rate(self, currency: str) -> float:
        """How many USD for 1 unit of this currency."""

        if currency in self._cache:
            return self._cache[currency]

        url = f"https://open.er-api.com/v6/latest/{currency}"
        rate = float(requests.get(url, timeout=20).json()["rates"]["USD"])
        self._cache[currency] = rate
        return rate
