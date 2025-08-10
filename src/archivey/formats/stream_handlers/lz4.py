from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO, Callable, cast

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    PackageNotInstalledError,
)
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_binaryio,
)
from archivey.types import StreamFormat

if TYPE_CHECKING:
    import lz4.frame as lz4_frame

    from archivey.config import ArchiveyConfig
else:  # pragma: no cover - optional dependency
    try:
        import lz4.frame as lz4_frame
    except ImportError:
        lz4_frame = None  # type: ignore[assignment]


def _translate_lz4_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    if isinstance(e, RuntimeError) and str(e).startswith("LZ4"):
        return ArchiveCorruptedError(f"Error reading LZ4 archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"LZ4 file is truncated: {repr(e)}")
    return None


def open_lz4_stream(path: str | BinaryIO) -> BinaryIO:
    if lz4_frame is None:
        raise PackageNotInstalledError(
            "lz4 package is not installed, required for LZ4 archives"
        ) from None
    return ensure_binaryio(cast("BinaryIO", lz4_frame.open(path)))


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    return open_lz4_stream, _translate_lz4_exception


register_stream_handler(
    StreamFormat.LZ4,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x04\x22\x4d\x18"],
    ),
)
