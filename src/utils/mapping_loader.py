"""
Mapping file loader.
"""

from __future__ import annotations

import json
from pathlib import Path


class MappingLoader:
    """Loads mapping JSON files."""

    @staticmethod
    def load(path: Path) -> dict[str, str]:

        with path.open(
            mode="r",
            encoding="utf-8",
        ) as file:
            return json.load(file)