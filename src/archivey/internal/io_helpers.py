"""Provides I/O helper classes, including exception translation and lazy opening."""

import io
import logging
from dataclasses import dataclass, field
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

from archivey.api.exceptions import ArchiveError

logger = logging.getLogger(__name__)


def is_seekable(stream: io.IOBase | IO[bytes]) -> bool:
    """Check if a stream is seekable."""
    try:
        return stream.seekable()
    except AttributeError as e:
        # Some streams (e.g. tarfile._Stream) don't have a seekable method, which seems
        # like a bug. Sometimes they are wrapped in other classes
        # (e.g. tarfile._FileInFile) that do have one and assume the inner ones also do.
        #
        # In the tarfile case specifically, _Stream actually does have a seek() method,
        # but calling seek() on the stream returned by tarfile will raise an exception,
        # as it's wrapped in a BufferedReader which calls seekable() when doing a seek().
        logger.debug(f"Stream {stream} does not have a seekable method: {e}")
        return False


@runtime_checkable
class ReadableBinaryStream(Protocol):
    def read(self, n: int = -1, /) -> bytes: ...


@runtime_checkable
class WritableBinaryStream(Protocol):
    def write(self, data: bytes, /) -> int: ...


BinaryStreamLike = Union[ReadableBinaryStream, WritableBinaryStream]


class BinaryIOWrapper(io.IOBase, BinaryIO):
    """
    Wraps an object that doesn't match the BinaryIO protocol, adding any missing
    methods to make the type checker happy.
    """

    def __init__(self, raw: BinaryStreamLike):
        self._raw = raw

    def read(self, size=-1, /):
        if not hasattr(self._raw, "read"):
            raise io.UnsupportedOperation("read not supported")
        self.read = self._raw.read  # type: ignore
        return self._raw.read(size)  # type: ignore

    def write(self, data, /):
        if not hasattr(self._raw, "write"):
            raise io.UnsupportedOperation("write not supported")
        self.write = self._raw.write  # type: ignore
        return self._raw.write(data)  # type: ignore

    def seek(self, offset, whence=io.SEEK_SET, /):
        if not hasattr(self._raw, "seek"):
            raise io.UnsupportedOperation("seek not supported")
        self.seek = self._raw.seek  # type: ignore
        return self._raw.seek(offset, whence)  # type: ignore

    def tell(self, /):
        if not hasattr(self._raw, "tell"):
            raise io.UnsupportedOperation("tell not supported")
        self.tell = self._raw.tell  # type: ignore
        return self._raw.tell()  # type: ignore

    def close(self):
        if hasattr(self._raw, "close"):
            return self._raw.close()  # type: ignore

    def flush(self):
        if hasattr(self._raw, "flush"):
            return self._raw.flush()  # type: ignore

    def readable(self):
        try:
            return self._raw.readable()  # type: ignore
        except AttributeError:
            return hasattr(self._raw, "read")  # type: ignore

    def writable(self):
        try:
            return self._raw.writable()  # type: ignore
        except AttributeError:
            return hasattr(self._raw, "write")  # type: ignore

    def seekable(self):
        return is_seekable(self._raw)  # type: ignore


ALL_IO_METHODS = (
    "read",
    "write",
    "seek",
    "tell",
    "__enter__",
    "__exit__",
    "close",
    "closed",
    "flush",
    "readable",
    "writable",
    "seekable",
    "readline",
    "readlines",
    "readinto",
    "write",
    "writelines",
)


def ensure_binaryio(obj: BinaryStreamLike) -> BinaryIO:
    """Some libraries return an object that doesn't match the BinaryIO protocol,
    so we need to ensure it does to make the type checker happy."""
    if all(callable(getattr(obj, m, None)) for m in ALL_IO_METHODS):
        return obj  # type: ignore

    logger.info(f"Object {obj!r} does not match the BinaryIO protocol, wrapping it in BinaryIOWrapper")
    return BinaryIOWrapper(obj)


# def ensure_bufferedio(obj: Any) -> BinaryIO:
#     bio = ensure_binaryio(obj)

#     if isinstance(bio, io.BufferedIOBase):
#         return bio

#     # Check if it supports read/write for bidirectional buffering
#     has_read = hasattr(bio, "read") and callable(bio.read)
#     has_write = hasattr(bio, "write") and callable(bio.write)

#     if has_read and has_write:
#         return io.BufferedRWPair(bio, bio)
#     elif has_read:
#         return io.BufferedReader(bio)
#     elif has_write:
#         return io.BufferedWriter(bio)

#     raise TypeError("ensure_binaryio returned an unbufferable object")


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


T = TypeVar("T")


def run_with_exception_translation(
    func: Callable[[], T],
    exception_translator: Callable[[Exception], Optional[ArchiveError]],
    archive_path: str | None = None,
    member_name: str | None = None,
) -> T:
    try:
        return func()
    except Exception as e:
        translated = exception_translator(e)
        if translated is not None:
            translated.archive_path = archive_path
            translated.member_name = member_name
            logger.debug(f"Translated exception: {repr(e)} -> {repr(translated)}")
            raise translated from e
        raise e


class ExceptionTranslatingIO(io.RawIOBase, BinaryIO):
    """
    Wraps an I/O stream to translate specific exceptions from an underlying library
    into ArchiveError subclasses.
    """

    def __init__(
        self,
        inner: IO[bytes] | Callable[[], IO[bytes]],
        exception_translator: Callable[[Exception], Optional[ArchiveError]],
        archive_path: str | None = None,
        member_name: str | None = None,
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
        self._inner: IO[bytes]
        self.archive_path = archive_path
        self.member_name = member_name

        if callable(inner):
            try:
                self._inner = inner()
            except Exception as e:
                # Here we do want to catch all exceptions, not just ArchiveError
                # subclasses, as the translation is intended exactly to convert
                # any exception raised by the underlying library into an ArchiveError.
                self._translate_exception(e)
        else:
            self._inner = inner

    def _translate_exception(self, e: Exception) -> NoReturn:
        translated = self._translate(e)
        if translated is not None:
            translated.archive_path = self.archive_path
            translated.member_name = self.member_name
            logger.debug(f"Translated exception: {repr(e)} -> {repr(translated)}")

            raise translated from e

        if not isinstance(e, ArchiveError):
            logger.error(f"Unknown exception when reading IO: {e}", exc_info=e)
        raise e

    def read(self, n: int = -1) -> bytes:
        # Some rarfile streams don't actually prevent reading after closing, so we
        # enforce that here.
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        try:
            return self._inner.read(n)
        except Exception as e:
            self._translate_exception(e)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        try:
            return self._inner.seek(offset, whence)
        except Exception as e:
            self._translate_exception(e)

    def tell(self) -> int:
        return self._inner.tell()

    def readable(self) -> bool:
        return self._inner.readable()

    def writable(self) -> bool:
        return self._inner.writable()

    def seekable(self) -> bool:
        return is_seekable(self._inner)

    def write(self, b: Any) -> int:
        try:
            return self._inner.write(b)
        except Exception as e:
            self._translate_exception(e)

    def writelines(self, lines: Any) -> None:
        try:
            self._inner.writelines(lines)
        except Exception as e:
            self._translate_exception(e)

    def close(self) -> None:
        # If the object raised an exception during initialization, it might not have
        # an _inner attribute. But IOBase.__del__() will eventually be called and may
        # call close() here. If we don't check that the attribute exists, we'll get an
        # spurious exception at that point.
        if not hasattr(self, "_inner"):
            return

        try:
            self._inner.close()
        except Exception as e:
            self._translate_exception(e)
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
    def read(self, n: int = -1, /) -> bytes:
        # Replace this method with the one from the underlying stream, to avoid
        # the overhead of an extra method call on future reads.
        self.read = self._ensure_open().read
        return self.read(n)

    def readable(self) -> bool:
        return True  # pragma: no cover - trivial

    def writable(self) -> bool:
        return False  # pragma: no cover - trivial

    def seekable(self) -> bool:
        return is_seekable(self._inner) if self._inner is not None else self._seekable

    def close(self) -> None:  # pragma: no cover - simple delegation
        if self._inner is not None:
            self._inner.close()
        super().close()

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        # Replace this method with the one from the underlying stream.
        self.seek = self._ensure_open().seek
        return self.seek(offset, whence)

    def tell(self) -> int:
        # Replace this method with the one from the underlying stream.
        self.tell = self._ensure_open().tell
        return self.tell()


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

    def __init__(self, inner: IO[bytes], stats: IOStats) -> None:
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
