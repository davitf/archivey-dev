import io
from typing import IO, Any, Callable, Optional


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
        except Exception as e:
            self._translate_exception(e)
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
        except Exception as e:
            self._translate_exception(e)
            return 0  # This line will never be reached due to _translate_exception always raising

    def writelines(self, lines: Any) -> None:
        try:
            self._inner.writelines(lines)
        except Exception as e:
            self._translate_exception(e)

    def close(self) -> None:
        try:
            self._inner.close()
        except Exception as e:
            self._translate_exception(e)
        super().close()
