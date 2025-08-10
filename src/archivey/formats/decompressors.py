"""Utility classes for streaming decompression with random access support."""

from __future__ import annotations

import abc
import io
import logging
import os
from typing import Any, BinaryIO

from archivey.exceptions import ArchiveEOFError
from archivey.internal.io_helpers import ensure_bufferedio

logger = logging.getLogger(__name__)


class DecompressorStream(io.RawIOBase, BinaryIO):
    """Base class for decompressor streams supporting seeking."""

    def __init__(self, path: str | BinaryIO) -> None:
        super().__init__()
        if isinstance(path, (str, bytes, os.PathLike)):
            self._inner = open(path, "rb")
            self._should_close = True
        else:
            self._inner = ensure_bufferedio(path)
            self._should_close = False
        self._decompressor = self._create_decompressor()
        self._buffer = bytearray()
        self._eof = False
        self._pos = 0
        self._size: int | None = None

    @abc.abstractmethod
    def _create_decompressor(self) -> Any: ...

    @abc.abstractmethod
    def _decompress_chunk(self, chunk: bytes) -> bytes: ...

    @abc.abstractmethod
    def _flush_decompressor(self) -> bytes: ...

    @abc.abstractmethod
    def _is_decompressor_finished(self) -> bool: ...

    def readable(self) -> bool:  # pragma: no cover - behaviour inherited
        return True

    def writable(self) -> bool:  # pragma: no cover - not used
        return False

    def seekable(self) -> bool:
        return self._inner.seekable()

    def _rewind(self) -> None:
        self._inner.seek(0)
        self._decompressor = self._create_decompressor()
        self._buffer.clear()
        self._eof = False
        self._pos = 0
        self._size = None

    def _read_decompressed_chunk(self) -> bytes:
        chunk = self._inner.read(65536)
        if not chunk:
            self._eof = True
            leftover = self._flush_decompressor()
            if not self._is_decompressor_finished():
                raise ArchiveEOFError("File is truncated")
            self._size = self._pos + len(self._buffer) + len(leftover)
            return leftover
        return self._decompress_chunk(chunk)

    def _seek_to_pos(self, pos: int) -> None:
        if pos == self._pos:
            return
        if pos < self._pos:
            self._rewind()
            assert self._pos == 0
        if self._pos + len(self._buffer) >= pos:
            del self._buffer[: pos - self._pos]
            self._pos = pos
            return
        self._pos += len(self._buffer)
        self._buffer.clear()
        while not self._eof:
            decompressed = self._read_decompressed_chunk()
            if self._pos + len(decompressed) >= pos:
                self._buffer.extend(decompressed[pos - self._pos :])
                self._pos = pos
                return
            self._pos += len(decompressed)
        # The position is past EOF
        self._pos = pos

    def readall(self) -> bytes:
        while not self._eof:
            self._buffer.extend(self._read_decompressed_chunk())
        data = bytes(self._buffer)
        self._pos += len(data)
        if self._size is not None:
            assert self._size == self._pos
        self._size = self._pos
        self._buffer.clear()
        return data

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n is None or n < 0:
            return self.readall()
        if len(self._buffer) < n and not self._eof:
            self._buffer.extend(self._read_decompressed_chunk())
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        self._pos += len(data)
        return data

    def readinto(self, b: bytearray | memoryview) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def close(self) -> None:  # pragma: no cover - trivial
        if self._should_close:
            self._inner.close()
        super().close()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                self.readall()
                assert self._size is not None
            new_pos = self._size + offset
        else:  # pragma: no cover - validated by io module
            raise ValueError(f"Invalid whence: {whence}")
        logger.info(
            f"Seeking to {new_pos} (offset: {offset}, whence: {whence}, current pos: {self._pos})"
        )
        if new_pos < 0:
            raise ValueError(f"Invalid offset: {offset}")
        self._seek_to_pos(new_pos)
        return self._pos

    def tell(self) -> int:  # pragma: no cover - trivial
        return self._pos
