from __future__ import annotations

import zlib
from typing import TYPE_CHECKING, BinaryIO, Callable

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError
from archivey.formats.decompressors import DecompressorStream
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import ExceptionTranslatorFn, ensure_binaryio
from archivey.types import StreamFormat

if TYPE_CHECKING:
    from archivey.config import ArchiveyConfig


class ZlibDecompressorStream(DecompressorStream):
    def _create_decompressor(self) -> "zlib._Decompress":
        return zlib.decompressobj()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.decompress(chunk)

    def _flush_decompressor(self) -> bytes:
        return self._decompressor.flush()

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.eof


def open_zlib_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(ZlibDecompressorStream(path))


def _translate_zlib_exception(
    e: Exception,
) -> ArchiveCorruptedError | ArchiveEOFError | None:
    if isinstance(e, zlib.error):
        if "incomplete" in str(e) or "truncated" in str(e):
            return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
        return ArchiveCorruptedError(f"Error reading Zlib archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
    return None


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    return open_zlib_stream, _translate_zlib_exception


register_stream_handler(
    StreamFormat.ZLIB,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[b"\x78\x01", b"\x78\x5e", b"\x78\x9c", b"\x78\xda"],
    ),
)
