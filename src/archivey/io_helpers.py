"""Provides I/O helper classes, including exception translation and lazy opening."""

import io
import logging
import gzip
import lzma
import tarfile
import zipfile
from dataclasses import dataclass, field
from typing import IO, Any, BinaryIO, Callable, Optional, cast

try:
    import rarfile
except ImportError:  # pragma: no cover - optional dependency
    rarfile = None

try:
    import py7zr.exceptions as py7zr_exceptions
except ImportError:  # pragma: no cover - optional dependency
    py7zr_exceptions = None

try:
    import zstandard
except ImportError:  # pragma: no cover - optional dependency
    zstandard = None

try:
    import pyzstd
except ImportError:  # pragma: no cover - optional dependency
    pyzstd = None

try:
    import xz
except ImportError:  # pragma: no cover - optional dependency
    xz = None

from archivey.exceptions import ArchiveError

logger = logging.getLogger(__name__)

_EXCEPTIONS = [
    OSError,
    RuntimeError,
    ValueError,
    EOFError,
    lzma.LZMAError,
    gzip.BadGzipFile,
    tarfile.TarError,
    zipfile.BadZipFile,
]
if rarfile is not None:
    _EXCEPTIONS.append(rarfile.Error)
if py7zr_exceptions is not None:
    _EXCEPTIONS.append(py7zr_exceptions.ArchiveError)
if zstandard is not None:
    _EXCEPTIONS.append(zstandard.ZstdError)
if pyzstd is not None:
    _EXCEPTIONS.append(pyzstd.ZstdError)
if xz is not None:
    _EXCEPTIONS.append(xz.XZError)
_CAUGHT_EXCEPTIONS = tuple(_EXCEPTIONS)


class ErrorIOStream(io.RawIOBase, BinaryIO):
    """
    An I/O stream that always raises a predefined exception on any I/O operation.

    This is useful for testing error handling paths or for representing
    unreadable members within an archive without returning None.
    """

    def __init__(self, exc: Exception):
        """Initialize the error stream."""
        self._exc = exc

    def read(self, size: int = -1) -> bytes:
        """Raise the stored exception."""
        raise self._exc

    def write(self, b: bytes) -> int:
        """Raise the stored exception."""
        raise self._exc

    def readable(self) -> bool:
        return True  # pragma: no cover - trivial

    def writable(self) -> bool:
        return False  # pragma: no cover - trivial

    def seekable(self) -> bool:
        return False  # pragma: no cover - trivial


class ExceptionTranslatingIO(io.RawIOBase, BinaryIO):
    """
    Wraps an I/O stream to translate specific exceptions from an underlying library
    into ArchiveError subclasses.

    This class is crucial for providing a consistent exception hierarchy to the
    users of `archivey`, regardless of the third-party library used for a
    specific archive format.
    """

    def __init__(
        self,
        # TODO: can we reduce the number of types here?
        inner: io.IOBase | IO[bytes] | Callable[[], io.IOBase | IO[bytes]],
        exception_translator: Callable[[Exception], Optional[ArchiveError]],
    ):
        """
        Initialize the ExceptionTranslatingIO wrapper.

        Args:
            inner: The underlying binary I/O stream (e.g., a file object opened by
                a third-party library) or a callable that returns such a stream.
                If a callable is provided, it will be called to obtain the stream.
                This can be useful for deferring the actual opening of the stream.
            exception_translator: A callable that takes an Exception instance
                (raised by the `inner` stream) and returns an Optional[ArchiveError].
                - If it returns an ArchiveError instance, that error is raised,
                  chaining the original exception.
                - If it returns None, the original exception is re-raised.
                The translator should be specific in the exceptions it handles and
                avoid catching generic `Exception`.
        """
        super().__init__()
        self._translate = exception_translator
        self._inner: io.IOBase | IO[bytes] | None = None

        if isinstance(inner, Callable):
            try:
                self._inner = inner()
            except _CAUGHT_EXCEPTIONS as e:
                self._translate_exception(e)
        else:
            self._inner = inner

    def _translate_exception(self, e: Exception) -> None:
        translated = self._translate(e)
        if translated is not None:
            logger.debug(f"Translated exception: {repr(e)} -> {repr(translated)}")
            raise translated from e

        if not isinstance(e, ArchiveError):
            logger.error(f"Unknown exception when reading IO: {e}", exc_info=e)
        raise e

    def read(self, n: int = -1) -> bytes:
        assert self._inner is not None
        try:
            return self._inner.read(n)
        except _CAUGHT_EXCEPTIONS as e:
            self._translate_exception(e)
            return b""  # pragma: no cover - unreachable, _translate_exception always raises

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        assert self._inner is not None
        try:
            return self._inner.seek(offset, whence)
        except _CAUGHT_EXCEPTIONS as e:
            logger.error(f"Exception when seeking {self._inner}: {e}", exc_info=e)
            self._translate_exception(e)
            return (
                0  # pragma: no cover - unreachable, _translate_exception always raises
            )

    def tell(self) -> int:
        assert self._inner is not None
        return self._inner.tell()

    def readable(self) -> bool:
        assert self._inner is not None
        return self._inner.readable()

    def writable(self) -> bool:
        assert self._inner is not None
        return self._inner.writable()

    def seekable(self) -> bool:
        assert self._inner is not None
        return self._inner.seekable()

    def write(self, b: Any) -> int:
        assert self._inner is not None
        try:
            return self._inner.write(b)
        except _CAUGHT_EXCEPTIONS as e:
            self._translate_exception(cast(Exception, e))
            return (
                0  # pragma: no cover - unreachable, _translate_exception always raises
            )

    def writelines(self, lines: Any) -> None:
        assert self._inner is not None
        try:
            self._inner.writelines(lines)
        except _CAUGHT_EXCEPTIONS as e:
            self._translate_exception(cast(Exception, e))

    def close(self) -> None:
        try:
            if self._inner is not None:
                self._inner.close()
        except _CAUGHT_EXCEPTIONS as e:
            self._translate_exception(cast(Exception, e))
        super().close()

    def __str__(self) -> str:
        return f"ExceptionTranslatingIO({self._inner!s})"

    def __repr__(self) -> str:
        return f"ExceptionTranslatingIO({self._inner!r})"


class LazyOpenIO(io.RawIOBase, BinaryIO):
    """
    An I/O stream wrapper that defers the actual opening of an underlying stream
    until the first I/O operation (e.g., read, seek) is attempted.

    This is useful to avoid opening many file handles if only a few of them
    might actually be used, for instance, when iterating over archive members
    but only reading from some.
    """

    def __init__(
        self,
        open_fn: Callable[..., IO[bytes]],
        *args: Any,
        seekable: bool,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the LazyOpenIO wrapper.

        Args:
            open_fn: A callable (function or method) that, when called, returns
                the actual binary I/O stream to be used.
            *args: Positional arguments to be passed to `open_fn` when it's called.
            seekable: A boolean hint indicating whether the underlying stream is
                expected to be seekable. `seekable()` will return this value
                without actually opening the stream.
            **kwargs: Keyword arguments to be passed to `open_fn` when it's called.
        """
        super().__init__()
        self._open_fn = open_fn
        self._args = args
        self._kwargs = kwargs
        self._inner: IO[bytes] | None = None
        self._seekable = seekable

    def _ensure_open(self) -> IO[bytes]:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self._inner is None:
            self._inner = self._open_fn(*self._args, **self._kwargs)
        return self._inner

    # ------------------------------------------------------------------
    # Basic IO methods
    # ------------------------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        return self._ensure_open().read(n)

    def readable(self) -> bool:
        return True  # pragma: no cover - trivial

    def writable(self) -> bool:
        return False  # pragma: no cover - trivial

    def seekable(self) -> bool:
        return self._seekable  # pragma: no cover - trivial

    def close(self) -> None:  # pragma: no cover - simple delegation
        if self._inner is not None:
            self._inner.close()
        super().close()

    def __enter__(self) -> "LazyOpenIO":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@dataclass
class IOStats:
    """Simple container for I/O statistics."""

    bytes_read: int = 0
    seek_calls: int = 0
    read_ranges: list[list[int]] = field(default_factory=lambda: [[0, 0]])


class StatsIO(io.RawIOBase, BinaryIO):
    """
    An I/O stream wrapper that tracks statistics about read and seek operations
    performed on an underlying stream.

    This can be useful for debugging, performance analysis, or understanding
    access patterns.
    """

    def __init__(self, inner: BinaryIO, stats: IOStats) -> None:
        super().__init__()
        self._inner = inner
        self.stats = stats

    # Basic IO methods -------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        data = self._inner.read(n)
        self.stats.bytes_read += len(data)
        self.stats.read_ranges[-1][1] += len(data)
        return data

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        if isinstance(self._inner, io.BufferedIOBase):
            n = self._inner.readinto(b)
        else:
            logger.debug(f"Reading {len(b)} bytes into buffer")
            data = self._inner.read(len(b))
            assert len(data) <= len(b)
            b[: len(data)] = data
            n = len(data)

        self.stats.bytes_read += n
        self.stats.read_ranges[-1][1] += n
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        logger.debug(
            f"Seeking to {offset} whence={whence}, prev read range: {self.stats.read_ranges[-1]}"
        )
        self.stats.seek_calls += 1
        newpos = self._inner.seek(offset, whence)
        self.stats.read_ranges.append([newpos, 0])
        return newpos

    def readable(self) -> bool:  # pragma: no cover - trivial
        return self._inner.readable()

    def writable(self) -> bool:  # pragma: no cover - trivial
        return self._inner.writable()

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return self._inner.seekable()

    def write(self, b: Any) -> int:  # pragma: no cover - simple delegation
        return self._inner.write(b)

    def close(self) -> None:  # pragma: no cover - simple delegation
        self._inner.close()
        super().close()

    # Delegate unknown attributes --------------------------------------
    def __getattr__(self, item: str) -> Any:  # pragma: no cover - simple
        return getattr(self._inner, item)

    # Context manager support -----------------------------------------
    def __enter__(self) -> "StatsIO":  # pragma: no cover - trivial
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover - trivial
        self._inner.__exit__(exc_type, exc_val, exc_tb)
        self.close()
