"""Core utilities for stream adaptation."""
from __future__ import annotations

import io
from typing import (
    BinaryIO,
    TextIO,
    overload,
    Literal,
)

from .errors import StreamModeError
from .types import BinaryStreamLike, TextStreamLike
from .wrappers import BinaryIOWrapper, LimitedSeekableWrapper


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

def detect_stream_mode(obj: BinaryStreamLike | TextStreamLike) -> Literal["binary", "text"]:
    """Detect whether ``obj`` is binary or text by calling ``read(0)``."""
    if not hasattr(obj, "read"):
        raise StreamModeError("object has no read() method for mode detection")
    try:
        res = obj.read(0)  # type: ignore[arg-type]
    except Exception as exc:  # pragma: no cover - rare edge cases
        raise StreamModeError(f"unable to detect stream mode: {exc}") from exc
    if isinstance(res, bytes):
        return "binary"
    if isinstance(res, str):
        return "text"
    raise StreamModeError("read(0) returned neither bytes nor str")


# ---------------------------------------------------------------------------
# Basic ensuring helpers
# ---------------------------------------------------------------------------

def ensure_binaryio(obj: BinaryStreamLike) -> BinaryIO:
    """Ensure ``obj`` behaves like a ``BinaryIO``."""
    needed = ("read", "write", "seek", "tell")
    if all(callable(getattr(obj, m, None)) for m in needed):
        return obj  # type: ignore[return-value]
    return BinaryIOWrapper(obj)


def ensure_textio(
    obj: TextStreamLike | BinaryIO,
    *,
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: str | None = None,
) -> TextIO:
    if isinstance(obj, io.TextIOBase):
        return obj
    if isinstance(obj, io.BufferedIOBase) or isinstance(obj, io.RawIOBase) or isinstance(obj, BinaryIOWrapper):
        return io.TextIOWrapper(obj, encoding=encoding, errors=errors, newline=newline)
    # If it's TextStreamLike (e.g., custom object with write/read returning str)
    if hasattr(obj, "read") or hasattr(obj, "write"):
        return io.TextIOWrapper(ensure_binaryio(obj), encoding=encoding, errors=errors, newline=newline)
    raise TypeError("object is not text stream compatible")


def ensure_bufferedio(obj: BinaryIO) -> BinaryIO:
    if isinstance(obj, io.BufferedIOBase):
        return obj
    has_read = hasattr(obj, "read")
    has_write = hasattr(obj, "write")
    if has_read and has_write:
        return io.BufferedRWPair(obj, obj)
    if has_read:
        return io.BufferedReader(obj)
    if has_write:
        return io.BufferedWriter(obj)
    raise TypeError("object cannot be buffered")


def ensure_buffered_textio(obj: TextIO) -> TextIO:
    if isinstance(obj, io.TextIOBase) and isinstance(obj.buffer, io.BufferedIOBase):
        return obj
    return io.TextIOWrapper(ensure_bufferedio(obj.buffer), encoding=obj.encoding or "utf-8")


def ensure_seekable(obj: BinaryIO | TextIO, *, force_wrapper: bool = False):
    if not force_wrapper and hasattr(obj, "seek") and hasattr(obj, "tell"):
        try:
            if obj.seekable():  # type: ignore[attr-defined]
                return obj
        except Exception:  # pragma: no cover - defensive
            pass
    return LimitedSeekableWrapper(obj)  # type: ignore[arg-type]


def slice_stream(stream: BinaryIO, offset: int, length: int) -> BinaryIO:
    """Return a seekable view of a slice of *stream*."""
    stream.seek(offset)
    data = stream.read(length)
    return io.BytesIO(data)


# ---------------------------------------------------------------------------
# Public high level helper
# ---------------------------------------------------------------------------

@overload
def ensure_stream(
    obj: BinaryStreamLike,
    *,
    mode: Literal["binary"],
    buffered: bool = True,
    seekable: bool = False,
) -> BinaryIO:
    ...


@overload
def ensure_stream(
    obj: TextStreamLike | BinaryStreamLike,
    *,
    mode: Literal["text"],
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: str | None = None,
    buffered: bool = True,
    seekable: bool = False,
) -> TextIO:
    ...


def ensure_stream(
    obj: TextStreamLike | BinaryStreamLike,
    *,
    mode: Literal["binary", "text"] | None = None,
    encoding: str = "utf-8",
    errors: str = "strict",
    newline: str | None = None,
    buffered: bool = True,
    seekable: bool = False,
):
    if mode is None:
        mode = detect_stream_mode(obj)

    if mode == "binary":
        stream = ensure_binaryio(obj)  # type: ignore[arg-type]
        if buffered:
            stream = ensure_bufferedio(stream)
        if seekable:
            stream = ensure_seekable(stream)
        return stream
    else:
        text = ensure_textio(obj, encoding=encoding, errors=errors, newline=newline)  # type: ignore[arg-type]
        if buffered:
            # TextIOWrapper is already buffered; just ensure underlying buffer
            if hasattr(text, "buffer") and not isinstance(text.buffer, io.BufferedIOBase):
                text = ensure_textio(text.buffer, encoding=text.encoding or encoding)  # type: ignore[arg-type]
        if seekable:
            text = ensure_seekable(text)
        return text

