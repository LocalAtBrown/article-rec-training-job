import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def get_elapsed_time(func: Callable[P, R]) -> Callable[P, tuple[float, R]]:
    """
    Decorator to get elapsed time of function execution.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> tuple[float, R]:
        start = time.perf_counter()
        output = func(*args, **kwargs)
        end = time.perf_counter()
        return (end - start), output

    return wrapper
