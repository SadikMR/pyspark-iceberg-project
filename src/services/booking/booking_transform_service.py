"""
Booking transformation service.
"""

from __future__ import annotations

from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType

from config.settings import settings
from src.utils.mapping_loader import MappingLoader


class BookingTransformService:
    """Transforms booking data."""

    _PROPERTY_PREFIX = "BC-"

    _SITE_PATTERN = r"k-([^_]+)"
    _DEVICE_PATTERN = r"d-([^_]+)"
    _REFERRAL_PATTERN = r"p-([^_]+)"

    def __init__(self) -> None:
        self._device_mapping = MappingLoader.load(
            settings.paths.device_mapping,
        )

        self._status_mapping = MappingLoader.load(
            settings.paths.booking_status_mapping,
        )

        self._country_region_mapping = MappingLoader.load(
            settings.paths.country_region_mapping,
        )

    def transform(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Transform booking data.
        """

        dataframe = self._project_columns(dataframe)
        dataframe = self._transform_property_id(dataframe)
        dataframe = self._extract_columns(dataframe)

        dataframe = self._map_booking_status(dataframe)
        dataframe = self._map_device(dataframe)
        dataframe = self._map_country_region(dataframe)

        dataframe = self._cast_columns(dataframe)

        return dataframe

    def _project_columns(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Select and rename required columns.
        """

        return dataframe.select(
            F.col("accommodations.reservation").alias(
                "transaction_id",
            ),
            F.col("label").alias(
                "conversion_key",
            ),
            F.col("accommodation_details.accommodation").alias(
                "property_id",
            ),
            F.col("status"),
            F.col("booker.travel_purpose").alias(
                "travel_purpose",
            ),
            F.col("booker.address.country").alias(
                "country_code",
            ),
            F.col("currencies.booker").alias(
                "currency",
            ),
            F.col("start").alias(
                "check_in_date",
            ),
            F.col("end").alias(
                "check_out_date",
            ),
            F.col(
                "commission.estimate_commission_amount.booker_currency",
            ).alias(
                "revenue",
            ),
        )

    def _transform_property_id(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Prefix property_id with BC-.
        """

        return dataframe.withColumn(
            "property_id",
            F.concat(
                F.lit(self._PROPERTY_PREFIX),
                F.col("property_id").cast("string"),
            ),
        )

    def _extract_columns(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Extract values from conversion_key.
        """

        return (
            dataframe
            .withColumn(
                "site_key",
                F.upper(
                    F.regexp_extract(
                        F.col("conversion_key"),
                        self._SITE_PATTERN,
                        1,
                    ),
                ),
            )
            .withColumn(
                "device_code",
                F.regexp_extract(
                    F.col("conversion_key"),
                    self._DEVICE_PATTERN,
                    1,
                ),
            )
            .withColumn(
                "referral_property_id",
                F.regexp_extract(
                    F.col("conversion_key"),
                    self._REFERRAL_PATTERN,
                    1,
                ),
            )
        )

    def _map_booking_status(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Map booking status.
        """

        mapping = self._create_mapping_expression(
            self._status_mapping,
        )

        return dataframe.withColumn(
            "status",
            F.coalesce(
                mapping[F.col("status")],
                F.col("status"),
            ),
        )

    def _map_device(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Map device code to device.
        """

        mapping = self._create_mapping_expression(
            self._device_mapping,
        )

        return (
            dataframe
            .withColumn(
                "device",
                mapping[F.col("device_code")],
            )
            .drop("device_code")
        )

    def _map_country_region(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Map country code to region.
        """

        mapping = self._create_mapping_expression(
            self._country_region_mapping,
        )

        return dataframe.withColumn(
            "region",
            mapping[F.col("country_code")],
        )

    def _cast_columns(
        self,
        dataframe: DataFrame,
    ) -> DataFrame:
        """
        Cast target columns.
        """

        return (
            dataframe
            .withColumn(
                "check_in_date",
                F.to_date(
                    F.col("check_in_date"),
                ),
            )
            .withColumn(
                "check_out_date",
                F.to_date(
                    F.col("check_out_date"),
                ),
            )
            .withColumn(
                "country_code",
                F.upper(
                    F.col("country_code"),
                ),
            )
            .withColumn(
                "revenue",
                F.col("revenue").cast(
                    DecimalType(18, 2),
                ),
            )
        )

    @staticmethod
    def _create_mapping_expression(
        mapping: dict[str, str],
    ) -> Column:
        """
        Create a Spark map expression from a Python dictionary.
        """

        return F.create_map(
            *[
                F.lit(value)
                for value in chain(
                    *mapping.items(),
                )
            ]
        )