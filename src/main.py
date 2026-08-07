"""
Application entry point.
"""

import argparse
import logging
import os

from config import settings
from src.jobs.booking_etl import BookingEtlJob
from src.jobs.migrate_postgres_job import MigratePostgresJob

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
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
    """Application entry point."""

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
        """Create an application from CLI arguments."""

        args = cls._parse_args()

        return cls(
            cron_name=args.cron_name,
            updated_from=args.updated_from,
            updated_to=args.updated_to,
        )

    @staticmethod
    def _parse_args() -> argparse.Namespace:
        """Parse command-line arguments."""

        parser = argparse.ArgumentParser(
            description="Run ETL jobs.",
        )

        parser.add_argument(
            "--cron-name",
            required=True,
            choices=[
                "booking",
                "migrate_postgres",
            ],
        )

        parser.add_argument("--updated_from")
        parser.add_argument("--updated_to")

        return parser.parse_args()

    def run(self) -> None:
        """Run the selected job."""

        logger.info(
            "Starting '%s' job.",
            self._cron_name,
        )

        if self._cron_name == "booking":
            self._run_booking()
            return

        if self._cron_name == "migrate_postgres":
            self._run_migration()
            return

        raise ValueError(
            f"Unsupported cron job: {self._cron_name}",
        )

    def _run_booking(self) -> None:
        """Run the booking ETL job."""

        if self._updated_from is None or self._updated_to is None:
            raise ValueError(
                "--updated_from and --updated_to are required.",
            )

        BookingEtlJob(
            updated_from=self._updated_from,
            updated_to=self._updated_to,
        ).run()

    @staticmethod
    def _run_migration() -> None:
        """Run the PostgreSQL migration job."""

        MigratePostgresJob().run()


if __name__ == "__main__":
    Application.from_cli().run()