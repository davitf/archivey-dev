import bz2
import gzip
import io
import zlib
import lzma
import os
import _compression
from typing import TYPE_CHECKING, BinaryIO, Callable, Optional, Any, cast

from typing_extensions import Buffer

from archivey.config import ArchiveyConfig, get_archivey_config
from archivey.internal.archive_stream import ArchiveStream
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_bufferedio,
    is_seekable,
    is_stream,
)
from archivey.types import ArchiveFormat

if TYPE_CHECKING:
    import brotli
    import indexed_bzip2
    import lz4.frame
    import pyzstd
    import rapidgzip
    import uncompresspy
    import xz
    import zstandard
else:
    try:
        import lz4.frame
    except ImportError:
        lz4 = None

    try:
        import zstandard
    except ImportError:
        zstandard = None

    try:
        import pyzstd
    except ImportError:
        pyzstd = None

    try:
        import rapidgzip
    except ImportError:
        rapidgzip = None

    try:
        import indexed_bzip2
    except ImportError:
        indexed_bzip2 = None

    try:
        import xz
    except ImportError:
        xz = None

    try:
        import uncompresspy
    except ImportError:
        uncompresspy = None

    try:
        import brotli
    except ImportError:
        brotli = None


import logging

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveStreamNotSeekableError,
    PackageNotInstalledError,
)
from archivey.internal.io_helpers import ensure_binaryio

logger = logging.getLogger(__name__)


def _translate_gzip_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, gzip.BadGzipFile):
        return ArchiveCorruptedError(f"Error reading GZIP archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"GZIP file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


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
        # GzipFile always returns True for seekable, even if the underlying stream
        # is not seekable.
        gz.seekable = lambda: False

        def _unsupported_seek(offset, whence=io.SEEK_SET):
            raise io.UnsupportedOperation("seek")

        gz.seek = _unsupported_seek

    return ensure_binaryio(gz)


def _translate_rapidgzip_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "IsalInflateWrapper" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    if isinstance(e, ValueError) and "Mismatching CRC32" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    if isinstance(e, ValueError) and "Failed to detect a valid file format" in str(e):
        # If we have opened a gzip stream, the magic bytes are there. So if the library
        # fails to detect a valid format, it's because the file is truncated.
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    if isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        # Rapidgzip tries to look at the underlying stream's fileno if it's not
        # seekable.
        return ArchiveStreamNotSeekableError(
            "rapidgzip does not support non-seekable streams"
        )
    if isinstance(e, io.UnsupportedOperation) and "seek" in exc_text:
        return ArchiveStreamNotSeekableError(
            "rapidgzip does not support non-seekable streams"
        )
    # This happens in some rapidgzip builds, not all.
    if isinstance(e, RuntimeError) and "std::exception" in str(e):
        return ArchiveCorruptedError(
            f"Unknown error reading RapidGZIP archive: {repr(e)}"
        )

    # Found in rapidgzip 0.11.0
    if (
        isinstance(e, ValueError)
        and "End of file encountered when trying to read zero-terminated string"
        in exc_text
    ):
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_rapidgzip_stream(path: str | BinaryIO) -> BinaryIO:
    if rapidgzip is None:
        raise PackageNotInstalledError(
            "rapidgzip package is not installed, required for GZIP archives"
        ) from None  # pragma: no cover -- rapidgzip is installed for main tests

    return rapidgzip.open(path, parallelization=0)


def _translate_bz2_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, OSError) and "Invalid data stream" in exc_text:
        return ArchiveCorruptedError(f"BZ2 file is corrupted: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"BZ2 file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(bz2.open(path))


def _translate_indexed_bzip2_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "Calculated CRC" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    # Unspecified exception in the indexed_bzip2 native code, likely when dealing with
    # corrupted data.
    if isinstance(e, RuntimeError) and exc_text in (
        "std::exception",  # Seen in Linux with non-prebuilt wheels
        "Unknown exception",  # Seen in Windows Github actions tests
    ):
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    if isinstance(e, ValueError) and "[BZip2 block data]" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    if isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        # Indexed BZIP2 tries to look at the underlying stream's fileno if it's not
        # seekable.
        return ArchiveStreamNotSeekableError(
            "indexed_bzip2 does not support non-seekable streams"
        )
    if isinstance(e, io.UnsupportedOperation) and "seek" in exc_text:
        return ArchiveStreamNotSeekableError(
            "indexed_bzip2 does not support non-seekable streams"
        )
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_indexed_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    if indexed_bzip2 is None:
        raise PackageNotInstalledError(
            "indexed_bzip2 package is not installed, required for BZIP2 archives"
        ) from None  # pragma: no cover -- indexed_bzip2 is installed for main tests

    return indexed_bzip2.open(path, parallelization=0)


def _translate_lzma_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, lzma.LZMAError):
        return ArchiveCorruptedError(f"Error reading LZMA archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"LZMA file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_lzma_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(lzma.open(path))


def _translate_python_xz_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, xz.XZError):
        return ArchiveCorruptedError(f"Error reading XZ archive: {repr(e)}")
    if isinstance(e, ValueError) and "filename is not seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "Python XZ does not support non-seekable streams"
        )
    # Raised by RecordableStream (used to wrap non-seekable streams during format
    # detection) when the library tries to seek to the end.
    if isinstance(e, io.UnsupportedOperation) and "seek to end" in str(e):
        return ArchiveStreamNotSeekableError(
            "Python XZ does not support non-seekable streams"
        )

    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_python_xz_stream(path: str | BinaryIO) -> BinaryIO:
    if xz is None:
        raise PackageNotInstalledError(
            "python-xz package is not installed, required for XZ archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests

    return ensure_binaryio(xz.open(path))


class WrappedDecompressReader(_compression.DecompressReader, BinaryIO):
    """Wrap :class:`_compression.DecompressReader` and close the underlying stream."""

    def __init__(
        self,
        path: str | BinaryIO,
        decomp_factory: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if isinstance(path, (str, bytes, os.PathLike)):
            fp = open(path, "rb")
            self._should_close = True
        else:
            fp = ensure_bufferedio(path)
            self._should_close = False

        super().__init__(fp, decomp_factory, *args, **kwargs)
        self._fp = fp

    def writable(self) -> bool:  # pragma: no cover - not used
        return False

    def close(self) -> None:  # pragma: no cover - simple
        try:
            super().close()
        finally:
            if self._should_close:
                self._fp.close()


class BrotliDecompressorAdapter:
    """Adapter exposing the interface expected by :class:`_compression.DecompressReader`."""

    def __init__(self) -> None:
        assert brotli is not None
        self._inner = brotli.Decompressor()
        self.unused_data = b""

    def decompress(self, data: bytes, _=None) -> bytes:
        return self._inner.process(data)

    @property
    def eof(self) -> bool:
        return self._inner.is_finished()

    @property
    def needs_input(self) -> bool:
        return True


class ZstdDecompressorAdapter:
    """Adapter for :mod:`zstandard` exposing the ``DecompressReader`` interface."""

    def __init__(self) -> None:
        assert zstandard is not None
        self._inner = zstandard.ZstdDecompressor().decompressobj()

    def decompress(self, data: bytes, _=None) -> bytes:
        return self._inner.decompress(data)

    @property
    def eof(self) -> bool:
        return self._inner.eof

    @property
    def needs_input(self) -> bool:
        return self._inner.unconsumed_tail == b""

    @property
    def unused_data(self) -> bytes:
        return self._inner.unused_data


def _translate_zstandard_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, zstandard.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_zstandard_stream(path: str | BinaryIO) -> BinaryIO:
    if zstandard is None:
        raise PackageNotInstalledError(
            "zstandard package is not installed, required for Zstandard archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests
    return ensure_binaryio(WrappedDecompressReader(path, ZstdDecompressorAdapter))


def _translate_pyzstd_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, pyzstd.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"Zstandard file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_pyzstd_stream(path: str | BinaryIO) -> BinaryIO:
    if pyzstd is None:
        raise PackageNotInstalledError(
            "pyzstd package is not installed, required for Zstandard archives"
        ) from None  # pragma: no cover -- pyzstd is installed for main tests
    return ensure_binaryio(pyzstd.open(path))


def _translate_lz4_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, RuntimeError) and str(e).startswith("LZ4"):
        return ArchiveCorruptedError(f"Error reading LZ4 archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"LZ4 file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_lz4_stream(path: str | BinaryIO) -> BinaryIO:
    if lz4 is None:
        raise PackageNotInstalledError(
            "lz4 package is not installed, required for LZ4 archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests

    return ensure_binaryio(cast("lz4.frame.LZ4FrameFile", lz4.frame.open(path)))


def open_zlib_stream(path: str | BinaryIO) -> BinaryIO:
    return ensure_binaryio(WrappedDecompressReader(path, zlib._ZlibDecompressor))


def _translate_zlib_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, zlib.error):
        if "incomplete" in str(e) or "truncated" in str(e):
            return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
        return ArchiveCorruptedError(f"Error reading Zlib archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
    return None




def _translate_brotli_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, brotli.error):
        return ArchiveCorruptedError(f"Error reading Brotli archive: {repr(e)}")
    return None


def open_brotli_stream(path: str | BinaryIO) -> BinaryIO:
    if brotli is None:
        raise PackageNotInstalledError(
            "brotli package is not installed, required for Brotli archives"
        ) from None
    return ensure_binaryio(WrappedDecompressReader(path, BrotliDecompressorAdapter))


def _translate_uncompresspy_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, ValueError) and "must be seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "uncompresspy does not support non-seekable streams"
        )
    return None


def open_uncompresspy_stream(path: str | BinaryIO) -> BinaryIO:
    if uncompresspy is None:
        raise PackageNotInstalledError(
            "uncompresspy package is not installed, required for Unix compress archives"
        ) from None  # pragma: no cover -- uncompresspy is installed for main tests

    lzwfile = cast("uncompresspy.LZWFile", uncompresspy.open(path))
    return ensure_binaryio(lzwfile)


def get_stream_open_fn(
    format: ArchiveFormat, config: ArchiveyConfig | None = None
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config is None:
        config = get_archivey_config()
    if format == ArchiveFormat.GZIP:
        if config.use_rapidgzip:
            return open_rapidgzip_stream, _translate_rapidgzip_exception
        return open_gzip_stream, _translate_gzip_exception

    if format == ArchiveFormat.BZIP2:
        if config.use_indexed_bzip2:
            return open_indexed_bzip2_stream, _translate_indexed_bzip2_exception
        return open_bzip2_stream, _translate_bz2_exception

    if format == ArchiveFormat.XZ:
        if config.use_python_xz:
            return open_python_xz_stream, _translate_python_xz_exception
        return open_lzma_stream, _translate_lzma_exception

    if format == ArchiveFormat.LZ4:
        return open_lz4_stream, _translate_lz4_exception

    if format == ArchiveFormat.ZLIB:
        return open_zlib_stream, _translate_zlib_exception

    if format == ArchiveFormat.BROTLI:
        return open_brotli_stream, _translate_brotli_exception

    if format == ArchiveFormat.ZSTD:
        if config.use_zstandard:
            return open_zstandard_stream, _translate_zstandard_exception
        return open_pyzstd_stream, _translate_pyzstd_exception

    if format == ArchiveFormat.UNIX_COMPRESS:
        return open_uncompresspy_stream, _translate_uncompresspy_exception

    raise ValueError(f"Unsupported archive format: {format}")  # pragma: no cover


def open_stream(
    format: ArchiveFormat,
    path_or_stream: str | BinaryIO,
    config: ArchiveyConfig,
) -> BinaryIO:
    open_fn, exception_translator = get_stream_open_fn(format, config)
    return ArchiveStream(
        open_fn=lambda: open_fn(path_or_stream),
        exception_translator=exception_translator,
        lazy=False,
        archive_path=path_or_stream if isinstance(path_or_stream, str) else None,
        member_name="<stream>",
        seekable=True,
    )
