from __future__ import annotations

import io
import lzma
from typing import TYPE_CHECKING, BinaryIO, Callable

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveStreamNotSeekableError,
    PackageNotInstalledError,
)
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_binaryio,
)
from archivey.types import StreamFormat

if TYPE_CHECKING:
    import xz

    from archivey.config import ArchiveyConfig
else:  # pragma: no cover - optional dependency
    try:
        import xz
    except ImportError:
        xz = None  # type: ignore[assignment]


def _translate_lzma_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    if isinstance(e, lzma.LZMAError):
        return ArchiveCorruptedError(f"Error reading LZMA archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"LZMA file is truncated: {repr(e)}")
    return None


def open_lzma_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(lzma.open(path))


def _translate_python_xz_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveStreamNotSeekableError | None:
    if xz is not None and isinstance(e, xz.XZError):
        return ArchiveCorruptedError(f"Error reading XZ archive: {repr(e)}")
    if isinstance(e, ValueError) and "filename is not seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "Python XZ does not support non-seekable streams"
        )
    if isinstance(e, io.UnsupportedOperation) and "seek to end" in str(e):
        return ArchiveStreamNotSeekableError(
            "Python XZ does not support non-seekable streams"
        )
    return None


def open_python_xz_stream(path: str | BinaryIO) -> BinaryIO:
    if xz is None:
        raise PackageNotInstalledError(
            "python-xz package is not installed, required for XZ archives"
        ) from None
    return ensure_binaryio(xz.open(path))


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config.use_python_xz:
        return open_python_xz_stream, _translate_python_xz_exception
    return open_lzma_stream, _translate_lzma_exception


register_stream_handler(
    StreamFormat.XZ,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\xfd\x37\x7a\x58\x5a\x00"],
    ),
)
