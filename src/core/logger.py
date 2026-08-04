"""
Application logging configuration.
"""

from __future__ import annotations

import logging
import sys


LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging() -> None:
    """Configure the root logger."""

    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger.

    Args:
        name: Logger name.

    Returns:
        Configured logger instance.
    """

    return logging.getLogger(name)