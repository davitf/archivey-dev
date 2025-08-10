import io
from typing import TYPE_CHECKING, BinaryIO, Optional

if TYPE_CHECKING:
    import brotli
else:
    try:
        import brotli
    except ImportError:
        brotli = None

from archivey.exceptions import (
    ArchiveCorruptedError,
    PackageNotInstalledError,
    ArchiveError,
)
from archivey.internal.io_helpers import ensure_binaryio
from archivey.formats.compressed_streams import DecompressorStream
from archivey.formats.registry import Format, registry

class BrotliDecompressorStream(DecompressorStream):
    """Wrap a file-like object and decompress it using ``brotli``."""

    def _create_decompressor(self) -> "brotli.Decompressor":
        if brotli is None:
            raise PackageNotInstalledError("brotli")
        return brotli.Decompressor()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.process(chunk)

    def _flush_decompressor(self) -> bytes:
        # brotli's decompressor doesn't have a flush method.
        # The remaining data is processed when `process` is called with an empty chunk,
        # but our `_read_decompressed_chunk` in the base class handles the EOF case.
        return b""

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()


def _translate_brotli_exception(e: Exception) -> Optional[ArchiveError]:
    if brotli and isinstance(e, brotli.error):
        return ArchiveCorruptedError(f"Error reading Brotli archive: {repr(e)}")
    return None


def open_brotli_stream(path: str | BinaryIO) -> BinaryIO:
    if brotli is None:
        raise PackageNotInstalledError(
            "brotli package is not installed, required for Brotli archives"
        ) from None
    return ensure_binaryio(BrotliDecompressorStream(path))

def _is_brotli_stream(stream: BinaryIO) -> bool:
    """Attempt to decompress a small chunk to see if it is Brotli."""
    if brotli is None:
        return False
    try:
        sample = stream.read(256)
        # If the stream is empty, it's not a brotli stream
        if not sample:
            return False
        decompressor = brotli.Decompressor()
        decompressor.process(sample)
        return True
    except brotli.error:
        return False
    finally:
        if hasattr(stream, 'seekable') and stream.seekable():
            stream.seek(0)


from archivey.types import ArchiveFormat


brotli_format = Format(
    format=ArchiveFormat.BROTLI,
    extensions=[".br"],
    open=open_brotli_stream,
    exception_translator=_translate_brotli_exception,
    detector=_is_brotli_stream,
)

registry.register(brotli_format)
