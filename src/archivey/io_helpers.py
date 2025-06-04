import io
from dataclasses import dataclass
from typing import IO, Any, Callable, Optional, cast


class ErrorIOStream(io.RawIOBase, IO[bytes]):
    """A stream that raises an exception on read operations."""

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
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False


class ExceptionTranslatingIO(io.RawIOBase, IO[bytes]):
    """A wrapper around an IO object that translates exceptions during operations.

    Args:
        inner: The inner IO object to wrap
        exception_translator: A function that takes an exception and returns either
            a translated exception or None (in which case the original exception is raised)
    """

    def __init__(
        self,
        inner: IO[bytes],
        exception_translator: Callable[[Exception], Optional[Exception]],
    ):
        super().__init__()
        self._inner = inner
        self._translate = exception_translator

    def _translate_exception(self, e: Exception) -> None:
        translated = self._translate(e)
        if translated is not None:
            raise translated from e
        raise e

    def read(self, n: int = -1) -> bytes:
        try:
            return self._inner.read(n)
        except BaseException as e:
            self._translate_exception(cast(Exception, e))
            return b""  # This line will never be reached due to _translate_exception always raising

    def readable(self) -> bool:
        return self._inner.readable()

    def writable(self) -> bool:
        return self._inner.writable()

    def seekable(self) -> bool:
        return self._inner.seekable()

    def write(self, b: Any) -> int:
        try:
            return self._inner.write(b)
        except BaseException as e:
            self._translate_exception(cast(Exception, e))
            return 0  # This line will never be reached due to _translate_exception always raising

    def writelines(self, lines: Any) -> None:
        try:
            self._inner.writelines(lines)
        except BaseException as e:
            self._translate_exception(cast(Exception, e))

    def close(self) -> None:
        try:
            self._inner.close()
        except BaseException as e:
            self._translate_exception(cast(Exception, e))
        super().close()


class LazyOpenIO(io.RawIOBase, IO[bytes]):
    """A wrapper that defers opening of the underlying stream until needed.

    Args:
        open_fn: Function used to open the underlying stream.
        seekable: Optional hint whether the underlying stream is seekable.
            If provided, ``seekable()`` will return this value without opening
            the stream.
    """

    def __init__(
        self,
        open_fn: Callable[..., IO[bytes]],
        *args: Any,
        seekable: bool,
        **kwargs: Any,
    ) -> None:
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

    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:
        return self._seekable

    def close(self) -> None:  # pragma: no cover - simple delegation
        if self._inner is not None:
            self._inner.close()
        super().close()

    # Context manager support -------------------------------------------------
    def __enter__(self) -> "LazyOpenIO":  # pragma: no cover - trivial
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # pragma: no cover - trivial
        self.close()


@dataclass
class IOStats:
    """Simple container for I/O statistics."""

    bytes_read: int = 0
    seek_calls: int = 0


class StatsIO(io.RawIOBase, IO[bytes]):
    """Wraps another IO object and tracks read/seek statistics."""

    def __init__(self, inner: IO[bytes], stats: IOStats) -> None:
        super().__init__()
        self._inner = inner
        self.stats = stats

    # Basic IO methods -------------------------------------------------
    def read(self, n: int = -1) -> bytes:
        data = self._inner.read(n)
        self.stats.bytes_read += len(data)
        return data

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        n = self._inner.readinto(b)
        self.stats.bytes_read += n
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self.stats.seek_calls += 1
        return self._inner.seek(offset, whence)

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
