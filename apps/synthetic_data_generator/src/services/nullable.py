import functools
import random
from typing import Any


def nullable(ratio: float = 0.05):
    """Decorator to introduce null values into data generation methods.

    Args:
        ratio (float): The ratio of nulls to introduce.
        min_threshold (int): Minimum threshold to apply nulls.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> Any | None:
            if not self.allow_nulls:
                return func(self, *args, **kwargs)

            if random.random() < ratio:
                return None

            return func(self, *args, **kwargs)

        return wrapper

    return decorator
