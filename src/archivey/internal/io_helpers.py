"""Provides I/O helper classes, including exception translation and lazy opening."""

import io
import logging
from dataclasses import dataclass, field
from typing import IO, Any, BinaryIO, Callable, NoReturn, Optional

from archivey.api.exceptions import ArchiveError

logger = logging.getLogger(__name__)


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


class ExceptionTranslatingIO(io.RawIOBase, BinaryIO):
    """
    Wraps an I/O stream to translate specific exceptions from an underlying library
    into ArchiveError subclasses.
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
        self._inner: io.IOBase | IO[bytes]

        if isinstance(inner, Callable):
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


class LimitedSeekStreamWrapper(io.RawIOBase, BinaryIO):
    """
    Wraps a non-seekable stream to provide limited backward seeking capability.

    This is achieved by maintaining a circular buffer of recently read data.
    Seeking forward beyond the current position will read from the underlying
    stream. Seeking backwards is only possible within the buffered range.
    """

    def __init__(self, stream: BinaryIO, buffer_size: int = 65536):
        super().__init__()
        if not hasattr(stream, "read"):
            raise TypeError("Stream must have a read method.")
        if buffer_size <= 0:
            raise ValueError("Buffer size must be positive.")

        self._stream = stream
        self._buffer_size = buffer_size
        self._buffer = bytearray()
        self._stream_pos = 0  # Current position in the underlying stream
        # self._buffer_offset = 0  # Offset of the start of the buffer relative to stream_pos -- seems unused
        self._current_pos = 0 # Current position of the user of this stream
        self._closed_internally = False # Custom flag to track if we've attempted to close underlying stream

    def readable(self) -> bool:
        if self._closed_internally: # Or self.closed from RawIOBase
            return False
        return self._stream.readable()

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._current_pos

    def read(self, size: int = -1) -> bytes:
        if self.closed or self._closed_internally: # Check our flag too
            raise ValueError("I/O operation on closed file.")

        # Determine how much data is needed
        bytes_needed = size if size >= 0 else float('inf')
        result = bytearray()

        # Try to satisfy read from buffer first
        buffer_available = len(self._buffer) - (self._current_pos - (self._stream_pos - len(self._buffer)))
        if self._current_pos >= (self._stream_pos - len(self._buffer)) and buffer_available > 0 :
            read_from_buffer = min(int(bytes_needed), buffer_available)

            buffer_start_index = self._current_pos - (self._stream_pos - len(self._buffer))
            result.extend(self._buffer[buffer_start_index : buffer_start_index+read_from_buffer])
            self._current_pos += read_from_buffer
            bytes_needed -= read_from_buffer

        if bytes_needed == 0:
            return bytes(result)

        # Read remaining from stream
        # If current_pos is behind stream_pos, it means we seeked back.
        # We need to advance stream_pos to current_pos before reading.
        if self._current_pos < self._stream_pos:
            # This case should ideally be handled by seeking, but as a fallback:
            # Discard buffer and read from stream. This might happen if we seek
            # back, then read past the buffer end.
            self._buffer = bytearray() # Clear buffer as it's no longer valid relative to stream_pos
            # Effectively, we are "catching up" the stream_pos to current_pos
            # This is complex because the original stream is non-seekable.
            # For simplicity in this wrapper, we assume reads are mostly sequential
            # or with limited seeks. If a read requires data before current buffer,
            # and current_pos is ahead of stream_pos (which means we read from stream already)
            # it's an issue.
            # However, if current_pos is where stream_pos is, we just read.
            # pass # This pass was causing an IndentationError


        # If current_pos is not at the end of the stream (stream_pos),
        # it implies a seek occurred. We need to read new data.
        if self._current_pos >= self._stream_pos:
            read_from_stream = int(bytes_needed if bytes_needed != float('inf') else self._buffer_size)

            # If size is -1 (read all), we can't know how much to read from a non-seekable stream
            # without potentially exhausting it. We'll read up to buffer_size chunks.
            # This is a limitation when size is -1 with this wrapper.
            # A true read-all would require reading in a loop.
            # For format detection, specific small reads are usually made.
            if size == -1:
                # Read until EOF in chunks
                while True:
                    chunk = self._stream.read(self._buffer_size) # Read a chunk
                    if not chunk:
                        break
                    result.extend(chunk)
                    self._buffer.extend(chunk)
                    self._stream_pos += len(chunk)
                    self._current_pos += len(chunk)
                     # Maintain buffer size
                    if len(self._buffer) > self._buffer_size:
                        self._buffer = self._buffer[len(self._buffer) - self._buffer_size:]
                return bytes(result)

            # If not size == -1, proceed with original logic for specific size reads
            data_read = self._stream.read(read_from_stream)
            if data_read:
                result.extend(data_read)

                self._buffer.extend(data_read)
                self._stream_pos += len(data_read)
                self._current_pos += len(data_read)

                # Maintain buffer size
                if len(self._buffer) > self._buffer_size:
                    self._buffer = self._buffer[len(self._buffer) - self._buffer_size:]
            elif not result and size != 0: # EOF and no data in result
                return b''


        return bytes(result)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

        new_pos: int
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._current_pos + offset
        elif whence == io.SEEK_END:
            # Seeking from end is not supported for non-seekable stream
            raise io.UnsupportedOperation(
                "Cannot seek from end of a non-seekable stream."
            )
        else:
            raise ValueError("invalid whence (%r, should be 0, 1 or 2)" % whence)

        if new_pos < 0:
            raise ValueError("Negative seek position")

        # Buffer start position in the absolute coordinate system
        buffer_abs_start = self._stream_pos - len(self._buffer)

        if new_pos >= buffer_abs_start and new_pos <= self._stream_pos :
            # Seek is within the current buffer
            self._current_pos = new_pos
        elif new_pos > self._stream_pos:
            # Seek forward, beyond the current buffered data and stream position
            # We need to read from the stream to reach this position.
            # This is like a read operation that discards the data.
            bytes_to_skip = new_pos - self._stream_pos

            # First, skip any remaining part of the buffer that's between current_pos and stream_pos
            if self._current_pos < self._stream_pos:
                 bytes_to_skip_in_buffer = min(bytes_to_skip, self._stream_pos - self._current_pos)
                 # This part is tricky as we are "consuming" buffer by seeking forward
                 # For simplicity, if seek is forward, we assume current_pos catches up to new_pos

            # Then, read and discard from the underlying stream
            # To avoid large reads if skipping far, read in chunks
            chunk_size = 8192
            while bytes_to_skip > 0:
                data_to_read = min(bytes_to_skip, chunk_size)
                data = self._stream.read(data_to_read)
                if not data: # EOF
                    # Cannot seek past EOF
                    self._current_pos = self._stream_pos # Update current_pos to where stream ended
                    # If new_pos was beyond EOF, this means we couldn't reach it.
                    # The Python file object behavior is to allow seek past EOF,
                    # but tell() will report the actual EOF position until a write happens.
                    # For a read-only stream, current_pos should be capped at EOF.
                    raise io.UnsupportedOperation("Cannot seek past EOF on this stream type when seeking forward by reading.")

                self._buffer.extend(data) # Add to buffer
                self._stream_pos += len(data)
                bytes_to_skip -= len(data)

                # Maintain buffer size
                if len(self._buffer) > self._buffer_size:
                    self._buffer = self._buffer[len(self._buffer) - self._buffer_size:]
            self._current_pos = new_pos
        else: # new_pos < buffer_abs_start
            # Seek is before the start of the buffer
            raise io.UnsupportedOperation(
                f"Cannot seek to position {new_pos}. Buffer starts at {buffer_abs_start} (stream_pos={self._stream_pos}, buffer_len={len(self._buffer)})"
            )

        return self._current_pos

    def close(self) -> None:
        if self._closed_internally:
            # If already called, super().close() might have been called.
            # Ensure RawIOBase.closed is also set if it wasn't.
            if not super().closed: # RawIOBase.closed
                 super().close()
            return

        try:
            if hasattr(self._stream, 'close'):
                self._stream.close()
        finally:
            # This block ensures that _closed_internally is set and super().close() is called
            # even if self._stream.close() raises an exception, marking the wrapper as closed.
            self._closed_internally = True
            if not super().closed: # RawIOBase.closed, in case of exceptions during _stream.close()
                super().close()

    def __str__(self) -> str:
        return f"LimitedSeekStreamWrapper({self._stream!s})"

    def __repr__(self) -> str:
        return f"LimitedSeekStreamWrapper({self._stream!r}, buffer_size={self._buffer_size})"
