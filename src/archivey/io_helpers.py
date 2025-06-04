import io
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
