from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO, Callable, cast

from archivey.exceptions import (
    ArchiveStreamNotSeekableError,
    PackageNotInstalledError,
)
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import ExceptionTranslatorFn, ensure_binaryio
from archivey.types import StreamFormat

if TYPE_CHECKING:
    from archivey.config import ArchiveyConfig

try:  # optional dependency
    import uncompresspy
except ImportError:
    uncompresspy = None


def _translate_uncompresspy_exception(
    e: Exception,
) -> ArchiveStreamNotSeekableError | None:
    if isinstance(e, ValueError) and "must be seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "uncompresspy does not support non-seekable streams"
        )
    return None


def open_uncompresspy_stream(path: str | BinaryIO) -> BinaryIO:
    if uncompresspy is None:
        raise PackageNotInstalledError(
            "uncompresspy package is not installed, required for Unix compress archives"
        ) from None
    lzwfile = cast("BinaryIO", uncompresspy.open(path))
    return ensure_binaryio(lzwfile)


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    return open_uncompresspy_stream, _translate_uncompresspy_exception


register_stream_handler(
    StreamFormat.UNIX_COMPRESS,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x1f\x9d"],
    ),
)
