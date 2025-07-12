"""
Utility functions for archivey.
"""

import logging
from typing import TypeVar, overload


@overload
def decode_bytes_with_fallback(data: None, encodings: list[str]) -> None: ...


@overload
def decode_bytes_with_fallback(data: bytes, encodings: list[str]) -> str: ...


def decode_bytes_with_fallback(data: bytes | None, encodings: list[str]) -> str | None:
    """
    Decode bytes with a list of encodings, falling back to utf-8 if the first encoding fails.
    """
    if data is None:
        return None

    assert isinstance(data, bytes), "Expected bytes for data"

    for encoding in encodings:
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    logging.warning(f"Failed to decode {data!r}, falling back to utf-8")
    return data.decode("utf-8", errors="replace")


@overload
def str_to_bytes(s: None) -> None: ...


@overload
def str_to_bytes(s: str | bytes) -> bytes: ...


def str_to_bytes(s: str | bytes | None) -> bytes | None:
    if s is None or isinstance(s, bytes):
        return s
    assert isinstance(s, str), f"Expected str, got {type(s)}"
    return s.encode("utf-8")


@overload
def bytes_to_str(b: None) -> None: ...


@overload
def bytes_to_str(b: str | bytes) -> str: ...


@overload
def bytes_to_str(b: str | bytes | None) -> str | None: ...


def bytes_to_str(b: str | bytes | None) -> str | None:
    if b is None or isinstance(b, str):
        return b
    assert isinstance(b, bytes), f"Expected bytes, got {type(b)}"
    return b.decode("utf-8")


T = TypeVar("T")


def ensure_not_none(x: T | None) -> T:
    if x is None:
        raise ValueError("Expected non-None value")
    return x
