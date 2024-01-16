from collections.abc import Iterable
from itertools import islice
from typing import TypeVar

T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[tuple[T, ...]]:
    """
    Divide an iterable into batches of size n, e.g. batched('ABCDEFG', 3) --> ABC DEF G
    Replace this function with itertools.batched once we upgrade to Python 3.12
    (see: https://docs.python.org/3.12/library/itertools.html#itertools.batched).
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch
