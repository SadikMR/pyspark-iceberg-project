"""
Booking transforms.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column
from pyspark.sql import DataFrame

from src.services.exchange_rates import ExchangeRateService

PROPERTY_PREFIX = "BC-"
SITE_PATTERN = r"k-([^_]+)"
DEVICE_PATTERN = r"d-([^_]+)"
REFERRAL_PATTERN = r"p-([^_]+)"


class BookingTransformer:
    """Transforms raw booking rows into the target schema."""

    def __init__(self, exchange_rates: ExchangeRateService) -> None:
        self.exchange_rates = exchange_rates

    def transform(
        self,
        df: DataFrame,
        status_map: DataFrame,
        device_map: DataFrame,
        region_map: DataFrame,
    ) -> DataFrame:
        df = df.select(*self._booking_fields(), *self._user_fields())
        df = self._transform_user_fields(df)
        df = self._extract_label_fields(df)
        df = self._transform_property_id(df)
        df = self._map_status(df, status_map)
        df = self._map_device(df, device_map)
        df = self._map_country_region(df, region_map)
        df = self._cast_types(df)
        df = self._add_revenue_usd(df)

        return self._finalize_columns(df)

        # DAG implementation (executor API via UDF)
        # return self._add_revenue_usd_dag(df)

    def _booking_fields(self) -> list[Column]:
        return [
            F.col("accommodations.reservation").alias("transaction_id"),
            F.col("label").alias("conversion_key"),
            F.col("accommodation_details.accommodation").alias("property_id"),
            F.col("status"),
            F.col("currencies.booker").alias("currency"),
            F.col("start").alias("check_in_date"),
            F.col("end").alias("check_out_date"),
            F.col("commission.estimate_commission_amount.booker_currency").alias("revenue"),
        ]

    def _user_fields(self) -> list[Column]:
        return [
            F.col("booker.travel_purpose").alias("travel_purpose"),
            F.col("booker.address.country").alias("country_code"),
        ]

    def _transform_user_fields(self, df: DataFrame) -> DataFrame:
        return df.withColumn("country_code", F.upper("country_code"))

    def _extract_label_fields(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(
                "site_key",
                F.upper(F.regexp_extract("conversion_key", SITE_PATTERN, 1)),
            )
            .withColumn(
                "device_code",
                F.regexp_extract("conversion_key", DEVICE_PATTERN, 1),
            )
            .withColumn(
                "referral_property_id",
                F.regexp_extract("conversion_key", REFERRAL_PATTERN, 1),
            )
        )

    def _transform_property_id(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "property_id",
            F.concat(F.lit(PROPERTY_PREFIX), F.col("property_id").cast("string")),
        )

    def _map_status(self, df: DataFrame, mapping: DataFrame) -> DataFrame:
        lookup = mapping.select(
            F.col("key").alias("status_key"),
            F.col("value").alias("status_mapped"),
        )
        df = df.join(F.broadcast(lookup), df.status == lookup.status_key, "left")
        return (
            df.withColumn("status", F.coalesce("status_mapped", "status"))
            .drop("status_key", "status_mapped")
        )

    def _map_device(self, df: DataFrame, mapping: DataFrame) -> DataFrame:
        lookup = mapping.select(
            F.col("key").alias("device_key"),
            F.col("value").alias("device"),
        )
        return (
            df.join(F.broadcast(lookup), df.device_code == lookup.device_key, "left")
            .drop("device_code", "device_key")
        )

    def _map_country_region(self, df: DataFrame, mapping: DataFrame) -> DataFrame:
        lookup = mapping.select(
            F.col("key").alias("country_key"),
            F.col("value").alias("region"),
        )
        return (
            df.join(F.broadcast(lookup), df.country_code == lookup.country_key, "left")
            .drop("country_key")
        )

    def _cast_types(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("check_in_date", F.to_date("check_in_date"))
            .withColumn("check_out_date", F.to_date("check_out_date"))
            .withColumn("revenue", F.col("revenue").cast(T.DecimalType(18, 2)))
        )

    def _add_revenue_usd(self, df: DataFrame) -> DataFrame:
        """Keep revenue, add revenue_usd using rates for currencies in the data."""

        rates = self.exchange_rates.get_rates_dataframe(df).select(
            F.col("currency").alias("rate_currency"),
            F.col("rate_to_usd"),
        )
        df = df.join(F.broadcast(rates), df.currency == rates.rate_currency, "left")
        return (
            df.withColumn(
                "revenue_usd",
                (F.col("revenue") * F.col("rate_to_usd")).cast(T.DecimalType(18, 2)),
            )
            .drop("rate_currency", "rate_to_usd")
        )

    def _finalize_columns(self, df: DataFrame) -> DataFrame:
        """Return the canonical booking columns in a stable order."""
        target_columns = [
            "transaction_id",
            "conversion_key",
            "property_id",
            "status",
            "currency",
            "check_in_date",
            "check_out_date",
            "revenue",
            "travel_purpose",
            "country_code",
            "site_key",
            "referral_property_id",
            "device",
            "region",
            "revenue_usd",
        ]
        return df.select(*target_columns)

    def _add_revenue_usd_dag(self, df: DataFrame) -> DataFrame:
        """Add revenue_usd using a UDF that calls the exchange-rate API during DAG execution."""

        df = self.exchange_rates.add_rate_column(df)

        return (
            df.withColumn(
                "revenue_usd",
                (F.col("revenue") * F.col("rate_to_usd"))
                .cast(T.DecimalType(18, 2))
            )
            .drop("rate_to_usd")
        )
