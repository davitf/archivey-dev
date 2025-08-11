from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, BinaryIO, Callable, cast

from typing_extensions import Buffer

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    PackageNotInstalledError,
)
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_binaryio,
    is_seekable,
    is_stream,
)
from archivey.types import StreamFormat

if TYPE_CHECKING:
    import pyzstd
    import zstandard

    from archivey.config import ArchiveyConfig
else:  # pragma: no cover - optional dependencies
    try:
        import zstandard
    except ImportError:
        zstandard = None  # type: ignore[assignment]

    try:
        import pyzstd
    except ImportError:
        pyzstd = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class ZstandardReopenOnBackwardsSeekIO(io.RawIOBase, BinaryIO):
    def __init__(self, archive_path: str | BinaryIO):
        super().__init__()
        self._archive_path = archive_path
        self._inner = cast("zstandard", zstandard).open(archive_path)
        self._size = None

    def _reopen_stream(self) -> None:
        self._inner.close()
        logger.warning(
            "Reopening Zstandard stream for backwards seeking: %s", self._archive_path
        )
        if is_stream(self._archive_path):
            self._archive_path.seek(0)
        self._inner = cast("zstandard", zstandard).open(self._archive_path)

    def seekable(self) -> bool:
        if is_stream(self._archive_path):
            return is_seekable(self._archive_path)
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def read(self, n: int = -1) -> bytes:
        return self._inner.read(n)

    def readinto(self, b: Buffer) -> int:
        return self._inner.readinto(b)  # type: ignore[attr-defined]

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._inner.tell() + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                while self._inner.read(65536):
                    pass
                self._size = self._inner.tell()
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        try:
            return self._inner.seek(new_pos)
        except OSError as e:
            if "cannot seek zstd decompression stream backwards" in str(e):
                self._reopen_stream()
                return self._inner.seek(new_pos)
            raise

    def close(self) -> None:
        self._inner.close()
        super().close()


def _translate_zstandard_exception(e: Exception) -> ArchiveCorruptedError | None:
    if zstandard is not None and isinstance(e, zstandard.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    return None


def open_zstandard_stream(path: str | BinaryIO) -> BinaryIO:
    if zstandard is None:
        raise PackageNotInstalledError(
            "zstandard package is not installed, required for Zstandard archives"
        ) from None
    return ZstandardReopenOnBackwardsSeekIO(path)


def _translate_pyzstd_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    if pyzstd is not None and isinstance(e, pyzstd.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"Zstandard file is truncated: {repr(e)}")
    return None


def open_pyzstd_stream(path: str | BinaryIO) -> BinaryIO:
    if pyzstd is None:
        raise PackageNotInstalledError(
            "pyzstd package is not installed, required for Zstandard archives"
        ) from None
    return ensure_binaryio(pyzstd.open(path))


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config.use_zstandard:
        return open_zstandard_stream, _translate_zstandard_exception
    return open_pyzstd_stream, _translate_pyzstd_exception


register_stream_handler(
    StreamFormat.ZSTD,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x28\xb5\x2f\xfd"],
    ),
)
