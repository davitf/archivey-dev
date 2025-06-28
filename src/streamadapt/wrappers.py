"""Stream wrapper utilities used by :mod:`streamadapt`."""
from __future__ import annotations

import io
from typing import BinaryIO, Any

from .types import BinaryStreamLike


class BinaryIOWrapper(io.IOBase, BinaryIO):
    """Wraps a partially-implemented binary stream to satisfy ``BinaryIO``."""

    def __init__(self, raw: BinaryStreamLike) -> None:
        self._raw = raw

    def read(self, size: int = -1) -> bytes:
        if not hasattr(self._raw, "read"):
            raise io.UnsupportedOperation("read not supported")
        self.read = self._raw.read  # type: ignore[assignment]
        return self._raw.read(size)  # type: ignore[arg-type]

    def write(self, data: bytes) -> int:
        if not hasattr(self._raw, "write"):
            raise io.UnsupportedOperation("write not supported")
        self.write = self._raw.write  # type: ignore[assignment]
        return self._raw.write(data)  # type: ignore[arg-type]

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if not hasattr(self._raw, "seek"):
            raise io.UnsupportedOperation("seek not supported")
        self.seek = self._raw.seek  # type: ignore[assignment]
        return self._raw.seek(offset, whence)  # type: ignore[arg-type]

    def tell(self) -> int:
        if not hasattr(self._raw, "tell"):
            raise io.UnsupportedOperation("tell not supported")
        self.tell = self._raw.tell  # type: ignore[assignment]
        return self._raw.tell()  # type: ignore[arg-type]

    def close(self) -> None:
        if hasattr(self._raw, "close"):
            self._raw.close()  # type: ignore[arg-type]

    def flush(self) -> None:
        if hasattr(self._raw, "flush"):
            self._raw.flush()  # type: ignore[arg-type]

    def readable(self) -> bool:
        try:
            return self._raw.readable()  # type: ignore[attr-defined]
        except AttributeError:
            return hasattr(self._raw, "read")

    def writable(self) -> bool:
        try:
            return self._raw.writable()  # type: ignore[attr-defined]
        except AttributeError:
            return hasattr(self._raw, "write")

    def seekable(self) -> bool:
        try:
            return self._raw.seekable()  # type: ignore[attr-defined]
        except AttributeError:
            return hasattr(self._raw, "seek")


class LimitedSeekableWrapper(io.RawIOBase, BinaryIO):
    """In-memory seekable wrapper for non-seekable binary streams."""

    def __init__(self, inner: BinaryIO) -> None:
        super().__init__()
        self._inner = inner
        self._buffer = bytearray()
        self._pos = 0
        self._eof = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure(self, end: int) -> None:
        while len(self._buffer) < end and not self._eof:
            chunk = self._inner.read(end - len(self._buffer))
            if not chunk:
                self._eof = True
                break
            self._buffer.extend(chunk)

    # ------------------------------------------------------------------
    # Basic IO methods
    # ------------------------------------------------------------------
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            data = self._inner.read()
            if data:
                self._buffer.extend(data)
            out = bytes(self._buffer[self._pos :])
            self._pos = len(self._buffer)
            return out

        self._ensure(self._pos + size)
        data = bytes(self._buffer[self._pos : self._pos + size])
        self._pos += len(data)
        return data

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            self._buffer.extend(self._inner.read())
            self._eof = True
            new_pos = len(self._buffer) + offset
        else:
            raise ValueError(f"invalid whence: {whence}")

        if new_pos < 0:
            raise ValueError("negative seek position")
        self._ensure(new_pos)
        self._pos = new_pos
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return True
