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
    TypeGuard,
    TypeVar,
    Union,
    cast,
    runtime_checkable,
)

from archivey.exceptions import ArchiveError

logger = logging.getLogger(__name__)


@runtime_checkable
class ReadableBinaryStream(Protocol):
    def read(self, n: int = -1, /) -> bytes: ...


@runtime_checkable
class WritableBinaryStream(Protocol):
    def write(self, data: bytes, /) -> int: ...


@runtime_checkable
class CloseableStream(Protocol):
    def close(self) -> None: ...


BinaryStreamLike = Union[ReadableBinaryStream, WritableBinaryStream, CloseableStream]

ReadableStreamLikeOrSimilar = Union[ReadableBinaryStream, io.IOBase, IO[bytes]]


def read_exact(stream: ReadableBinaryStream, n: int) -> bytes:
    """Read exactly ``n`` bytes, or all available bytes if the file ends."""

    if n < 0:
        raise ValueError("n must be non-negative")

    data = bytearray()
    while len(data) < n:
        chunk = stream.read(n - len(data))
        if not chunk:
            break
        data.extend(chunk)
    return bytes(data)


def is_seekable(stream: io.IOBase | IO[bytes] | BinaryStreamLike) -> bool:
    """Check if a stream is seekable."""
    # When we wrap a RewindableNonSeekableStream in a BufferedReader, we want to check
    # if the inner stream is seekable, with the check below.
    if isinstance(stream, io.BufferedReader):
        return is_seekable(stream.raw)

    try:
        return stream.seekable() or False  # type: ignore[attr-defined]
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


class BinaryIOWrapper(io.RawIOBase, BinaryIO):
    """
    Wraps an object that doesn't match the BinaryIO protocol, adding any missing
    methods to make the type checker happy.
    """

    def __init__(self, raw: BinaryStreamLike):
        self._raw = raw

    def read(self, size=-1, /) -> bytes | None:
        if hasattr(self._raw, "read"):
            data = self._raw.read(size)  # type: ignore
            # If read succeeded, we can use it directly for future reads
            self.read = self._raw.read  # type: ignore
            return data

        return super().read(size)

    def write(self, data, /):
        if not hasattr(self._raw, "write"):
            raise io.UnsupportedOperation("write not supported")
        self.write = self._raw.write  # type: ignore
        return self._raw.write(data)  # type: ignore

    def _readinto_from_read(self, b: bytearray | memoryview, /) -> int | None:
        data = self.read(len(b))
        if data is None:
            return None
        b[: len(data)] = data
        return len(data)

    def readinto(self, b: bytearray | memoryview, /) -> int | None:
        if not hasattr(self._raw, "readinto"):
            self.readinto = self._readinto_from_read
            return self._readinto_from_read(b)

        try:
            bytes_read = self._raw.readinto(b)  # type: ignore[attr-defined]
            # If readinto succeeded, we can use it for future reads
            self.readinto = self._raw.readinto  # type: ignore
            return bytes_read
        except (NotImplementedError, io.UnsupportedOperation):
            # Some streams don't support readinto, so we fall back to read()
            self.readinto = self._readinto_from_read
            return self._readinto_from_read(b)

    def seek(self, offset, whence=io.SEEK_SET, /):
        if hasattr(self._raw, "seek"):
            pos = self._raw.seek(offset, whence)  # type: ignore
            # If seek succeeded, we can use it for future seeks
            self.seek = self._raw.seek  # type: ignore
            return pos

        raise io.UnsupportedOperation("seek")

    def tell(self, /):
        if hasattr(self._raw, "tell"):
            pos = self._raw.tell()  # type: ignore
            # If tell succeeded, we can use it for future tells
            self.tell = self._raw.tell  # type: ignore
            return pos
        raise io.UnsupportedOperation("tell")

    def close(self):
        super().close()
        if hasattr(self._raw, "close"):
            self._raw.close()  # type: ignore

    def flush(self):
        if hasattr(self._raw, "flush"):
            return self._raw.flush()  # type: ignore
        return None

    def readable(self):
        try:
            result = self._raw.readable()  # type: ignore
            # The result can be None if the class just extended BinaryIO and didn't
            # actually implement the method.
            if result is not None:
                return result

        except AttributeError:
            pass

        return hasattr(self._raw, "read") or hasattr(self._raw, "readinto")  # type: ignore

    def writable(self):
        try:
            result = self._raw.writable()  # type: ignore
            # The result can be None if the class just extended BinaryIO and didn't
            # actually implement the method.
            if result is not None:
                return result

        except AttributeError:
            return hasattr(self._raw, "write")  # type: ignore

    def seekable(self):
        return is_seekable(self._raw)  # type: ignore


ALL_IO_METHODS = {
    "read",
    "write",
    "seek",
    "tell",
    "__enter__",
    "__exit__",
    "close",
    "flush",
    "readable",
    "writable",
    "seekable",
    "readline",
    "readlines",
    "readinto",
    "write",
    "writelines",
}

ALL_IO_PROPERTIES = {
    "closed",
}


def is_stream(obj: Any) -> TypeGuard[BinaryIO]:
    """Check if an object matches the BinaryIO protocol."""

    logger.info("Checking if %s is a stream", obj)

    # First check if it's a standard IOBase instance
    is_iobase = isinstance(obj, io.IOBase)

    missing_methods = {m for m in ALL_IO_METHODS if not callable(getattr(obj, m, None))}
    missing_properties = {p for p in ALL_IO_PROPERTIES if not hasattr(obj, p)}
    has_all_interface = not missing_methods and not missing_properties

    if not has_all_interface:
        logger.debug(
            "Object %r does not match the BinaryIO protocol: missing methods %r, missing properties %r",
            obj,
            missing_methods,
            missing_properties,
        )

    if is_iobase != has_all_interface:
        logger.debug(
            "Object %r : is_iobase=%r, has_all_interface=%r",
            obj,
            is_iobase,
            has_all_interface,
        )

    return is_iobase or has_all_interface


def ensure_binaryio(obj: BinaryStreamLike) -> BinaryIO:
    """Some libraries return an object that doesn't match the BinaryIO protocol,
    so we need to ensure it does to make the type checker happy."""

    if is_stream(obj):
        return obj

    logger.info(
        f"Object {obj!r} does not match the BinaryIO protocol, wrapping it in BinaryIOWrapper."
    )
    return BinaryIOWrapper(obj)


StreamT = TypeVar("StreamT", bound=BinaryStreamLike)


class UncloseableStream:
    def __init__(self, inner: BinaryStreamLike):
        self._inner = inner

    def close(self):
        logger.error(
            "Closing UncloseableStream, not closing inner stream %s",
            self._inner,
            stack_info=True,
        )
        pass

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


def ensure_uncloseable(obj: StreamT) -> StreamT:
    """Return a stream that doesn't close the underlying stream when closed."""
    if isinstance(obj, UncloseableStream):
        return obj
    return cast("StreamT", UncloseableStream(obj))


def ensure_bufferedio(obj: BinaryStreamLike) -> io.BufferedIOBase:
    if isinstance(obj, io.BufferedIOBase):
        return obj

    if not isinstance(obj, io.RawIOBase):
        obj = BinaryIOWrapper(obj)

    # BufferedReader closes the underlying stream when closed or deleted. If
    # ensure_bufferedio is called to temporarily buffer a stream, we need to ensure
    # that the underlying stream is not closed when the BufferedReader is closed or
    # goes out of scope. The underlying stream will be closed when it's garbage
    # collected anyway, so we don't need to worry about it leaking.
    return io.BufferedReader(ensure_uncloseable(obj))


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


class LazyOpenIO(io.BufferedIOBase, BinaryIO):
    """
    An I/O stream wrapper that defers the actual opening of an underlying stream
    until the first I/O operation (e.g., read, seek) is attempted.

    This is useful to avoid opening many file handles if only a few of them
    might actually be used, for instance, when iterating over archive members
    but only reading from some.
    """

    def __init__(
        self,
        open_fn: Callable[..., io.BufferedIOBase],
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
        self._inner: io.BufferedIOBase | None = None
        self._seekable = seekable

    def _ensure_open(self) -> io.BufferedIOBase:
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


class RecordableStream(io.RawIOBase, BinaryIO):
    """Wrap a stream, caching all data read from it."""

    def __init__(self, inner: ReadableStreamLikeOrSimilar):
        super().__init__()
        self._inner = inner
        self._buffer = bytearray()
        self._pos = 0

    def get_all_data(self) -> bytes:
        """Return all data read so far."""
        return bytes(self._buffer)

    # Basic IO methods -------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if n == -1:
            data = self._buffer[self._pos :]
            self._pos = len(self._buffer)
            chunk = self._inner.read()
            self._buffer.extend(chunk)
            self._pos = len(self._buffer)
            return bytes(data) + chunk

        remaining = n
        data = bytearray()

        available = len(self._buffer) - self._pos
        if available > 0:
            take = min(available, remaining)
            data.extend(self._buffer[self._pos : self._pos + take])
            self._pos += take
            remaining -= take

        if remaining > 0:
            chunk = self._inner.read(remaining)
            self._buffer.extend(chunk)
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
        if whence == io.SEEK_CUR:
            offset = self._pos + offset
        elif whence == io.SEEK_END:
            raise io.UnsupportedOperation("seek to end")
        elif whence != io.SEEK_SET:
            raise ValueError(f"Invalid whence: {whence}")

        if offset < 0:
            raise io.UnsupportedOperation("seek outside recorded region")
        while offset > len(self._buffer):
            chunk = self._inner.read(offset - len(self._buffer))
            if not chunk:
                break
            self._buffer.extend(chunk)

        if offset > len(self._buffer):
            raise io.UnsupportedOperation("seek outside recorded region")

        self._pos = offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    # Properties -------------------------------------------------------
    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return True

    # Control methods --------------------------------------------------
    def close(self) -> None:  # pragma: no cover - simple delegation
        if hasattr(self._inner, "close"):
            self._inner.close()  # type: ignore
        super().close()


class ConcatenationStream(io.RawIOBase, BinaryIO):
    """Concatenate multiple streams sequentially."""

    def __init__(self, streams: list[ReadableStreamLikeOrSimilar]):
        super().__init__()
        self._streams = streams
        self._index = 0

    # Basic IO methods -------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        if n == -1:
            return b"".join(stream.read() for stream in self._streams)

        while self._index < len(self._streams):
            data = self._streams[self._index].read(n)
            if data:
                return data
            self._index += 1

        # All streams are exhausted.
        return b""

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    # Properties -------------------------------------------------------
    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return False

    def fileno(self) -> int:  # pragma: no cover - simple
        raise OSError("fileno")

    # Control methods --------------------------------------------------
    def close(self) -> None:  # pragma: no cover - simple delegation
        super().close()
