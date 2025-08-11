from __future__ import annotations

from typing import IO, TYPE_CHECKING, Any, BinaryIO, Callable, cast

from archivey.exceptions import ArchiveCorruptedError, PackageNotInstalledError
from archivey.formats.decompressors import DecompressorStream
from archivey.formats.registry import StreamHandler, register_stream_handler
from archivey.internal.io_helpers import ExceptionTranslatorFn, ensure_binaryio
from archivey.types import StreamFormat

if TYPE_CHECKING:
    from archivey.config import ArchiveyConfig

try:  # optional dependency
    import brotli
except ImportError:
    brotli = None


class BrotliDecompressorStream(DecompressorStream):
    def _create_decompressor(self):
        return cast("Any", brotli).Decompressor()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.process(chunk)

    def _flush_decompressor(self) -> bytes:
        return b""

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()


def _translate_brotli_exception(e: Exception) -> ArchiveCorruptedError | None:
    if brotli is not None and isinstance(e, brotli.error):
        return ArchiveCorruptedError(f"Error reading Brotli archive: {repr(e)}")
    return None


def open_brotli_stream(path: str | BinaryIO) -> BinaryIO:
    if brotli is None:
        raise PackageNotInstalledError(
            "brotli package is not installed, required for Brotli archives"
        ) from None
    return ensure_binaryio(BrotliDecompressorStream(path))


def _is_brotli_stream(stream: IO[bytes]) -> bool:
    if brotli is None:
        return False
    try:
        sample = stream.read(256)
        decompressor = brotli.Decompressor()
        decompressor.process(sample)
        return True
    except brotli.error:
        return False


def _handler_factory(
    config: ArchiveyConfig,
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    return open_brotli_stream, _translate_brotli_exception


register_stream_handler(
    StreamFormat.BROTLI,
    StreamHandler(
        handler_factory=_handler_factory,
        magic_bytes=[],
        extra_detector=_is_brotli_stream,
    ),
)
