"""
Runtime timing decorator.
"""

from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable[..., object])


def timed(label: str | None = None) -> Callable[[F], F]:
    """Print wall-clock seconds when the wrapped function finishes."""

    def decorator(func: F) -> F:
        name = label or func.__qualname__

        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            started = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - started
                print(f"[runtime] {name}: {elapsed:.3f}s")

        return wrapper  # type: ignore[return-value]

    return decorator
