"""
Utility functions for archivey.
"""

import logging


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
