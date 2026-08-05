"""
Backward-compatible entrypoint.

Prefer: python -m src.jobs.booking_etl
"""

from src.jobs.booking_etl import run


def main() -> None:
    run()


if __name__ == "__main__":
    main()
