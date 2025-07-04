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

from archivey.exceptions import ArchiveError

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
        logger.debug("Stream %s does not have a seekable method: %s", stream, e)
        return False


def read_exact(stream: IO[bytes], n: int) -> bytes:
    """Read exactly ``n`` bytes from ``stream``.

    Continues reading until ``n`` bytes are returned or raises ``EOFError``
    if the stream ends prematurely.
    """

    if n < 0:
        raise ValueError("n must be non-negative")

    data = bytearray()
    while len(data) < n:
        chunk = stream.read(n - len(data))
        if not chunk:
            raise EOFError(f"Expected {n} bytes, got {len(data)}")
        data.extend(chunk)
    return bytes(data)


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

    def _readinto_fallback(self, b: bytearray | memoryview, /) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def readinto(self, b: bytearray | memoryview, /) -> int:
        try:
            bytes_read = self._raw.readinto(b)  # type: ignore[attr-defined]
            # If readinto succeeded, we can use it for future reads
            self.readinto = self._raw.readinto  # type: ignore
            return bytes_read
        except NotImplementedError:
            # Some streams don't support readinto, so we fall back to read()
            self.readinto = self._readinto_fallback
            return self._readinto_fallback(b)

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
        return None

    def flush(self):
        if hasattr(self._raw, "flush"):
            return self._raw.flush()  # type: ignore
        return None

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

    logger.info(
        f"Object {obj!r} does not match the BinaryIO protocol, wrapping it in BinaryIOWrapper"
    )
    return BinaryIOWrapper(obj)


def ensure_buffered_io(obj: BinaryIO) -> BinaryIO:
    """Return ``obj`` wrapped in :class:`io.BufferedReader` if needed."""

    if isinstance(obj, io.BufferedReader):
        return obj

    return io.BufferedReader(obj)


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
            logger.debug(
                "Translated exception: %r -> %r",
                e,
                translated,
            )
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
            except Exception as e:  # noqa: BLE001
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
            logger.debug(
                "Translated exception: %r -> %r",
                e,
                translated,
            )

            raise translated from e

        if not isinstance(e, ArchiveError):
            logger.error("Unknown exception when reading IO: %s", e, exc_info=e)
        raise e

    def read(self, n: int = -1) -> bytes:
        # Some rarfile streams don't actually prevent reading after closing, so we
        # enforce that here.
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        try:
            return self._inner.read(n)
        except Exception as e:  # noqa: BLE001
            self._translate_exception(e)

    def _readinto_with_fallback(self, b: bytearray | memoryview) -> int:
        try:
            return self._inner.readinto(b)  # type: ignore[attr-defined]
        except NotImplementedError:
            # Some streams don't support readinto, so we fall back to read()
            data = self.read(len(b))
            b[: len(data)] = data
            return len(data)

    def readinto(self, b: bytearray | memoryview) -> int:
        try:
            return self._readinto_with_fallback(b)
        except Exception as e:  # noqa: BLE001
            self._translate_exception(e)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        try:
            return self._inner.seek(offset, whence)
        except Exception as e:  # noqa: BLE001
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
        except Exception as e:  # noqa: BLE001
            self._translate_exception(e)

    def writelines(self, lines: Any) -> None:
        try:
            self._inner.writelines(lines)
        except Exception as e:  # noqa: BLE001
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
        except Exception as e:  # noqa: BLE001
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

    def readinto(self, b: bytearray | memoryview, /) -> int:
        self.readinto = self._ensure_open().readinto  # type: ignore[attr-defined]
        return self.readinto(b)  # type: ignore[attr-defined]

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
        try:
            n = self._inner.readinto(b)  # type: ignore[attr-defined]
            self.stats.bytes_read += n
            self.stats.read_ranges[-1][1] += n
            return n
        except NotImplementedError:
            # Some streams don't support readinto, so we fall back to read()
            data = self.read(len(b))
            b[: len(data)] = data
            self.stats.bytes_read += len(data)
            self.stats.read_ranges[-1][1] += len(data)
            return len(data)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        newpos = self._inner.seek(offset, whence)
        if offset != 0 or whence != 1:
            # Called by IOBase.tell(), doesn't actually move the stream. Ignore these seeks.
            self.stats.seek_calls += 1
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


class RewindableNonSeekableStream(io.RawIOBase, BinaryIO):
    """Wrap a non-seekable stream that supports seeking via an internal buffer."""

    def __init__(self, inner: IO[bytes]):
        super().__init__()
        self._inner = inner
        self._buffer = bytearray()
        self._pos = 0
        self._stream_pos = 0
        self._recording = True

    def stop_recording(self) -> None:
        """Stop storing new data into the rewind buffer."""
        self._recording = False

    def _read_from_stream(self, n: int) -> bytes:
        chunk = self._inner.read(n)
        if self._recording:
            self._buffer.extend(chunk)
        self._stream_pos += len(chunk)
        return chunk

    def _advance_stream_to(self, target: int) -> None:
        while self._stream_pos < target:
            chunk = self._read_from_stream(target - self._stream_pos)
            if not chunk:
                break

    # Basic IO methods -------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        endpos = self._pos + n
        if n >= 0 and endpos <= len(self._buffer):
            # The data is fully in the buffer, so we can return it directly.
            data = bytes(self._buffer[self._pos : endpos])
            self._pos += n
            return data

        data = bytearray()
        remaining = n

        if self._pos < len(self._buffer):
            # Take the previously-read data from the buffer.
            data.extend(self._buffer[self._pos :])
            self._pos += len(data)
            if remaining != -1:
                remaining -= len(data)

        if self._pos < self._stream_pos:
            raise io.UnsupportedOperation(
                "cannot read from non buffered region. "
                f"pos={self._pos}, stream_pos={self._stream_pos}, buffer_size={len(self._buffer)}"
            )

        if self._pos > self._stream_pos:
            self._advance_stream_to(self._pos)

        assert self._stream_pos == self._pos

        if remaining == -1:
            while True:
                chunk = self._read_from_stream(-1)
                if not chunk:
                    break
                self._pos += len(chunk)
                data.extend(chunk)
            return bytes(data)

        chunk = self._read_from_stream(remaining)
        self._pos += len(chunk)
        data.extend(chunk)

        return bytes(data)

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    # Seek/Tell --------------------------------------------------------
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_END:
            raise io.UnsupportedOperation("seek to end")
        if whence == io.SEEK_CUR:
            offset = self._pos + offset
        elif whence != io.SEEK_SET:
            raise ValueError(f"Invalid whence: {whence}")

        if offset < 0:
            raise io.UnsupportedOperation("seek to negative position")

        if offset >= len(self._buffer) and offset < self._stream_pos:
            raise io.UnsupportedOperation(
                f"seek into non-cached region: buf={len(self._buffer)}, offset={offset}, stream={self._stream_pos}"
            )

        if offset > self._stream_pos:
            self._advance_stream_to(offset)

        self._pos = offset if offset <= self._stream_pos else self._stream_pos
        return self._pos

    def tell(self) -> int:
        return self._pos

    # Properties -------------------------------------------------------
    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return self._recording

    # Control methods --------------------------------------------------

    def close(self) -> None:  # pragma: no cover - simple delegation
        self._inner.close()
        super().close()

    # Delegate unknown attributes -------------------------------------
    def __getattr__(self, item: str) -> Any:  # pragma: no cover - simple
        return getattr(self._inner, item)
