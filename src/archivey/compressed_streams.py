import bz2
import gzip
import io
import lzma
from typing import IO, TYPE_CHECKING, Optional

from archivey.config import ArchiveyConfig
from archivey.types import ArchiveFormat

if TYPE_CHECKING:
    import indexed_bzip2
    import lz4.frame
    import rapidgzip
    import xz
    import zstandard
else:
    try:
        import lz4.frame
    except ImportError:
        lz4.frame = None

    try:
        import zstandard
    except ImportError:
        zstandard = None

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


from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    PackageNotInstalledError,
)
from archivey.io_helpers import ExceptionTranslatingIO


def _translate_gzip_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, gzip.BadGzipFile):
        return ArchiveCorruptedError(f"Error reading GZIP archive: {e}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError("GZIP file is truncated: {e}")
    return None


def open_gzip_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(gzip.open(path, mode="rb"), _translate_gzip_exception)


def _translate_rapidgzip_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "IsalInflateWrapper" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {e}")
    elif isinstance(e, ValueError) and "Mismatching CRC32" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {e}")
    return None


def open_rapidgzip_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(
        rapidgzip.open(path, parallelization=0), _translate_rapidgzip_exception
    )


def _translate_bz2_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, OSError):
        return ArchiveCorruptedError("BZ2 file is corrupted")
    elif isinstance(e, EOFError):
        return ArchiveEOFError("BZ2 file is truncated")
    elif isinstance(e, ValueError):
        return ArchiveFormatError("No valid BZ2 stream found")
    return None


def open_bzip2_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(bz2.open(path), _translate_bz2_exception)


def _translate_indexed_bzip2_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "Calculated CRC" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {e}")
    elif isinstance(e, RuntimeError) and exc_text == "std::exception":
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {e}")
    elif isinstance(e, ValueError) and "[BZip2 block data]" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {e}")

    # elif isinstance(e, EOFError):
    #     return ArchiveEOFError("Indexed BZIP2 file is truncated: {e}")
    # if isinstance(e, indexed_bzip2.IndexedBzip2Error):
    #     return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {e}")
    return None


def open_indexed_bzip2_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(
        indexed_bzip2.open(path, parallelization=0), _translate_indexed_bzip2_exception
    )


def _translate_lzma_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, lzma.LZMAError):
        return ArchiveCorruptedError(f"Error reading LZMA archive: {e}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError("LZMA file is truncated: {e}")
    return None


def open_lzma_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(lzma.open(path), _translate_lzma_exception)


def _translate_python_xz_exception(e: Exception) -> Optional[ArchiveError]:
    import logging

    logger = logging.getLogger(__name__)
    logger.info("TRANSLATING XZ EXCEPTION", exc_info=e)
    if isinstance(e, xz.XZError):
        return ArchiveCorruptedError(f"Error reading XZ archive: {e}")
    else:
        logger.error(f"Unexpected exception: {e}", exc_info=e)
    return None


def open_python_xz_stream(path: str) -> IO[bytes]:
    if xz is None:
        raise PackageNotInstalledError(
            "xz package is not installed, required for XZ archives"
        ) from None

    return ExceptionTranslatingIO(lambda: xz.open(path), _translate_python_xz_exception)


def _translate_zstd_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, zstandard.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {e}")
    return None


def open_zstd_stream(path: str) -> IO[bytes]:
    if zstandard is None:
        raise PackageNotInstalledError(
            "zstandard package is not installed, required for Zstandard archives"
        ) from None
    def _open() -> IO[bytes]:
        with open(path, "rb") as f:
            data = f.read()
        decompressed = zstandard.decompress(data)
        return io.BytesIO(decompressed)

    return ExceptionTranslatingIO(_open, _translate_zstd_exception)


def _translate_lz4_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, RuntimeError) and str(e).startswith("LZ4"):
        return ArchiveCorruptedError(f"Error reading LZ4 archive: {e}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError("LZ4 file is truncated: {e}")
    return None


def open_lz4_stream(path: str) -> IO[bytes]:
    return ExceptionTranslatingIO(lz4.frame.open(path), _translate_lz4_exception)


def open_stream(format: ArchiveFormat, path: str, config: ArchiveyConfig) -> IO[bytes]:
    if format == ArchiveFormat.GZIP:
        if config.use_rapidgzip:
            return open_rapidgzip_stream(path)
        else:
            return open_gzip_stream(path)

    elif format == ArchiveFormat.BZIP2:
        if config.use_indexed_bzip2:
            return open_indexed_bzip2_stream(path)
        else:
            return open_bzip2_stream(path)

    elif format == ArchiveFormat.XZ:
        if config.use_python_xz:
            return open_python_xz_stream(path)
        else:
            return open_lzma_stream(path)

    elif format == ArchiveFormat.LZ4:
        return open_lz4_stream(path)

    elif format == ArchiveFormat.ZSTD:
        return open_zstd_stream(path)

    raise ValueError(f"Unsupported archive format: {format}")
