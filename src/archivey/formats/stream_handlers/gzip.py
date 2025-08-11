from __future__ import annotations

import gzip
import io
import os
from typing import TYPE_CHECKING, BinaryIO, Callable

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveStreamNotSeekableError,
    PackageNotInstalledError,
)
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_binaryio,
    ensure_bufferedio,
    is_seekable,
)
from archivey.types import StreamFormat

if TYPE_CHECKING:
    from archivey.config import ArchiveyConfig

try:  # optional dependency
    import rapidgzip
except ImportError:
    rapidgzip = None


def _translate_gzip_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    if isinstance(e, gzip.BadGzipFile):
        return ArchiveCorruptedError(f"Error reading GZIP archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"GZIP file is truncated: {repr(e)}")
    return None  # pragma: no cover


def open_gzip_stream(path: str | BinaryIO) -> BinaryIO:
    if isinstance(path, (str, bytes, os.PathLike)):
        gz = gzip.open(path, mode="rb")
        underlying_seekable = True
    else:
        assert not path.closed
        gz = gzip.GzipFile(fileobj=ensure_bufferedio(path), mode="rb")
        assert not path.closed
        underlying_seekable = is_seekable(path)

    if not underlying_seekable:
        gz.seekable = lambda: False

        def _unsupported_seek(offset, whence=io.SEEK_SET):
            raise io.UnsupportedOperation("seek")

        gz.seek = _unsupported_seek

    return ensure_binaryio(gz)


def _translate_rapidgzip_exception(e: Exception) -> ArchiveError | None:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "IsalInflateWrapper" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    if isinstance(e, ValueError) and "Mismatching CRC32" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    if isinstance(e, ValueError) and "Failed to detect a valid file format" in str(e):
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    if isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        return ArchiveStreamNotSeekableError(
            "rapidgzip does not support non-seekable streams"
        )
    if isinstance(e, io.UnsupportedOperation) and "seek" in exc_text:
        return ArchiveStreamNotSeekableError(
            "rapidgzip does not support non-seekable streams"
        )
    if isinstance(e, RuntimeError) and "std::exception" in str(e):
        return ArchiveCorruptedError(
            f"Unknown error reading RapidGZIP archive: {repr(e)}"
        )
    if (
        isinstance(e, ValueError)
        and "End of file encountered when trying to read zero-terminated string"
        in exc_text
    ):
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    return None


def open_rapidgzip_stream(path: str | BinaryIO) -> BinaryIO:
    if rapidgzip is None:
        raise PackageNotInstalledError(
            "rapidgzip package is not installed, required for GZIP archives"
        ) from None
    return rapidgzip.open(path, parallelization=0)


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config.use_rapidgzip:
        return open_rapidgzip_stream, _translate_rapidgzip_exception
    return open_gzip_stream, _translate_gzip_exception


register_stream_handler(
    StreamFormat.GZIP,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x1f\x8b"],
    ),
)
