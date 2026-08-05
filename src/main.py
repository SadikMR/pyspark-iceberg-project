"""
Application entry point.

Run:
  python -m src.main \\
    --cron-name booking \\
    --updated_from 2026-07-12 \\
    --updated_to 2026-07-13
"""

import argparse
import os

from config import settings

# Driver memory + Iceberg jars must be available when the JVM starts.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    (
        f"--driver-memory {settings.DRIVER_MEMORY} "
        f"--executor-memory {settings.EXECUTOR_MEMORY} "
        f"--packages {settings.ICEBERG_PACKAGE} "
        "pyspark-shell"
    ),
)

from src.jobs.booking_etl import BookingEtlJob


class Application:
    """CLI entrypoint — parse args and run the selected cron job."""

    def __init__(self, cron_name: str, updated_from: str, updated_to: str) -> None:
        self.cron_name = cron_name
        self.updated_from = updated_from
        self.updated_to = updated_to

    @classmethod
    def from_cli(cls) -> "Application":
        args = cls.parse_args()
        return cls(
            cron_name=args.cron_name,
            updated_from=args.updated_from,
            updated_to=args.updated_to,
        )

    @staticmethod
    def parse_args() -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Run an ETL cron job")
        parser.add_argument(
            "--cron-name",
            required=True,
            choices=["booking"],
            help="Which job to run",
        )
        parser.add_argument(
            "--updated_from",
            required=True,
            help="Filter start date (YYYY-MM-DD)",
        )
        parser.add_argument(
            "--updated_to",
            required=True,
            help="Filter end date (YYYY-MM-DD)",
        )
        return parser.parse_args()

    def run(self) -> None:
        if self.cron_name == "booking":
            BookingEtlJob(
                updated_from=self.updated_from,
                updated_to=self.updated_to,
            ).run()
            return

        raise ValueError(f"Unsupported cron-name: {self.cron_name}")


if __name__ == "__main__":
    Application.from_cli().run()
