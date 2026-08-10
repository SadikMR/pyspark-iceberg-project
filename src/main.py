"""
Application entry point.

  python -m src.main --cron-name booking --updated_from YYYY-MM-DD --updated_to YYYY-MM-DD
  python -m src.main --cron-name migrate_postgres
"""

import argparse
import logging
import os

from config import settings
from src.jobs.booking_etl import BookingEtlJob
from src.jobs.migrate_postgres import MigratePostgresJob

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    (
        f"--driver-memory {settings.DRIVER_MEMORY} "
        f"--executor-memory {settings.EXECUTOR_MEMORY} "
        f"--packages {settings.ICEBERG_PACKAGE} "
        "pyspark-shell"
    ),
)


class Application:
    """CLI entrypoint — dispatch by --cron-name."""

    def __init__(
        self,
        cron_name: str,
        updated_from: str | None,
        updated_to: str | None,
    ) -> None:
        self._cron_name = cron_name
        self._updated_from = updated_from
        self._updated_to = updated_to

    @classmethod
    def from_cli(cls) -> "Application":
        args = cls._parse_args()
        return cls(
            cron_name=args.cron_name,
            updated_from=args.updated_from,
            updated_to=args.updated_to,
        )

    @staticmethod
    def _parse_args() -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Run an ETL cron job")
        parser.add_argument(
            "--cron-name",
            required=True,
            choices=["booking", "migrate_postgres"],
            help="booking = transform JSONL → Iceberg; migrate_postgres = Iceberg → Postgres",
        )
        parser.add_argument(
            "--updated_from",
            help="Required for booking (YYYY-MM-DD)",
        )
        parser.add_argument(
            "--updated_to",
            help="Required for booking (YYYY-MM-DD)",
        )
        return parser.parse_args()

    def run(self) -> None:
        logger.info("Starting cron-name=%s", self._cron_name)

        if self._cron_name == "booking":
            if not self._updated_from or not self._updated_to:
                raise ValueError(
                    "--updated_from and --updated_to are required for booking"
                )
            BookingEtlJob(
                updated_from=self._updated_from,
                updated_to=self._updated_to,
            ).run()
            return

        if self._cron_name == "migrate_postgres":
            MigratePostgresJob().run()
            return

        raise ValueError(f"Unsupported cron-name: {self._cron_name}")


if __name__ == "__main__":
    Application.from_cli().run()
