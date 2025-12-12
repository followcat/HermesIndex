# Common helpers for CPU node
import hashlib
from typing import Iterable, List


def text_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def chunked(iterable: List, size: int) -> Iterable[List]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]
