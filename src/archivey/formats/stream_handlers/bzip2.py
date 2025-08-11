from __future__ import annotations

import bz2
import io
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
    from archivey.config import ArchiveyConfig

try:  # optional dependency
    import indexed_bzip2
except ImportError:
    indexed_bzip2 = None


def _translate_bz2_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    exc_text = str(e)
    if isinstance(e, OSError) and "Invalid data stream" in exc_text:
        return ArchiveCorruptedError(f"BZ2 file is corrupted: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"BZ2 file is truncated: {repr(e)}")
    return None


def open_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(bz2.open(path))


def _translate_indexed_bzip2_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveStreamNotSeekableError | None:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "Calculated CRC" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    if isinstance(e, RuntimeError) and exc_text in (
        "std::exception",
        "Unknown exception",
    ):
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    if isinstance(e, ValueError) and "[BZip2 block data]" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    if isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        return ArchiveStreamNotSeekableError(
            "indexed_bzip2 does not support non-seekable streams"
        )
    if isinstance(e, io.UnsupportedOperation) and "seek" in exc_text:
        return ArchiveStreamNotSeekableError(
            "indexed_bzip2 does not support non-seekable streams"
        )
    return None


def open_indexed_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    if indexed_bzip2 is None:
        raise PackageNotInstalledError(
            "indexed_bzip2 package is not installed, required for BZIP2 archives"
        ) from None
    return ensure_binaryio(indexed_bzip2.open(path, parallelization=0))


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config.use_indexed_bzip2:
        return open_indexed_bzip2_stream, _translate_indexed_bzip2_exception
    return open_bzip2_stream, _translate_bz2_exception


register_stream_handler(
    StreamFormat.BZIP2,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x42\x5a\x68"],
    ),
)
