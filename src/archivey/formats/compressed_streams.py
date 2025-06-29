import bz2
import gzip
import io
import lzma
import os
from typing import IO, TYPE_CHECKING, BinaryIO, Optional, cast

from archivey.api.config import ArchiveyConfig
from archivey.api.types import ArchiveFormat
from archivey.internal.io_helpers import is_seekable
from archivey.internal.utils import is_stream

if TYPE_CHECKING:
    import indexed_bzip2
    import lz4.frame
    import pyzstd
    import rapidgzip
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


import logging

from archivey.api.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveStreamNotSeekableError,
    PackageNotInstalledError,
)
from archivey.internal.io_helpers import ExceptionTranslatingIO, ensure_binaryio

logger = logging.getLogger(__name__)


def _translate_gzip_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, gzip.BadGzipFile):
        return ArchiveCorruptedError(f"Error reading GZIP archive: {repr(e)}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError(f"GZIP file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_gzip_stream(path: str | BinaryIO) -> BinaryIO:
    def _open() -> IO[bytes]:
        if isinstance(path, (str, bytes, os.PathLike)):
            gz = gzip.open(path, mode="rb")
            underlying_seekable = True
        else:
            gz = gzip.GzipFile(fileobj=path, mode="rb")
            underlying_seekable = is_seekable(path)

        if not underlying_seekable:
            gz.seekable = lambda: False  # type: ignore[assignment]

            def _unsupported_seek(offset: int, whence: int = io.SEEK_SET) -> int:
                raise io.UnsupportedOperation("seek")

            gz.seek = _unsupported_seek  # type: ignore[assignment]

        return ensure_binaryio(gz)

    return ExceptionTranslatingIO(_open, _translate_gzip_exception)


def _translate_rapidgzip_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "IsalInflateWrapper" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    elif isinstance(e, ValueError) and "Mismatching CRC32" in exc_text:
        return ArchiveCorruptedError(f"Error reading RapidGZIP archive: {repr(e)}")
    elif isinstance(e, ValueError) and "Failed to detect a valid file format" in str(e):
        # If we have opened a gzip stream, the magic bytes are there. So if the library
        # fails to detect a valid format, it's because the file is truncated.
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    elif isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        # Rapidgzip tries to look at the underlying stream's fileno if it's not
        # seekable.
        return ArchiveStreamNotSeekableError(
            "rapidgzip does not support non-seekable streams"
        )

    # Found in rapidgzip 0.11.0
    elif (
        isinstance(e, ValueError)
        and "End of file encountered when trying to read zero-terminated string"
        in exc_text
    ):
        return ArchiveEOFError(f"Possibly truncated GZIP stream: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_rapidgzip_stream(path: str | BinaryIO) -> BinaryIO:
    return ExceptionTranslatingIO(
        lambda: rapidgzip.open(path, parallelization=0), _translate_rapidgzip_exception
    )


def _translate_bz2_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, OSError) and "Invalid data stream" in exc_text:
        return ArchiveCorruptedError(f"BZ2 file is corrupted: {repr(e)}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError(f"BZ2 file is truncated: {repr(e)}")
    # elif isinstance(e, ValueError):
    #     return ArchiveFormatError("No valid BZ2 stream found")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    return ExceptionTranslatingIO(lambda: bz2.open(path), _translate_bz2_exception)


def _translate_indexed_bzip2_exception(e: Exception) -> Optional[ArchiveError]:
    exc_text = str(e)
    if isinstance(e, RuntimeError) and "Calculated CRC" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    elif isinstance(e, RuntimeError) and exc_text == "std::exception":
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    elif isinstance(e, ValueError) and "[BZip2 block data]" in exc_text:
        return ArchiveCorruptedError(f"Error reading Indexed BZIP2 archive: {repr(e)}")
    elif isinstance(e, ValueError) and "has no valid fileno" in exc_text:
        # Indexed BZIP2 tries to look at the underlying stream's fileno if it's not
        # seekable.
        return ArchiveStreamNotSeekableError(
            "Indexed BZIP2 does not support non-seekable streams"
        )
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_indexed_bzip2_stream(path: str | BinaryIO) -> BinaryIO:
    return ExceptionTranslatingIO(
        lambda: indexed_bzip2.open(path, parallelization=0),
        _translate_indexed_bzip2_exception,
    )


def _translate_lzma_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, lzma.LZMAError):
        return ArchiveCorruptedError(f"Error reading LZMA archive: {repr(e)}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError(f"LZMA file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_lzma_stream(path: str | BinaryIO) -> BinaryIO:
    return ExceptionTranslatingIO(lambda: lzma.open(path), _translate_lzma_exception)


def _translate_python_xz_exception(e: Exception) -> Optional[ArchiveError]:
    import logging

    logger = logging.getLogger(__name__)
    logger.debug("TRANSLATING XZ EXCEPTION", exc_info=e)
    if isinstance(e, xz.XZError):
        return ArchiveCorruptedError(f"Error reading XZ archive: {repr(e)}")
    elif isinstance(e, ValueError) and "filename is not seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "Python XZ does not support non-seekable streams"
        )
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_python_xz_stream(path: str | BinaryIO) -> BinaryIO:
    if xz is None:
        raise PackageNotInstalledError(
            "python-xz package is not installed, required for XZ archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests

    return ExceptionTranslatingIO(
        lambda: ensure_binaryio(xz.open(path)), _translate_python_xz_exception
    )


class ZstandardReopenOnBackwardsSeekIO(io.RawIOBase, BinaryIO):
    """Wrap a stream that supports seeking backwards, and reopen it if a backwards seek is attempted."""

    def __init__(self, archive_path: str | BinaryIO):
        super().__init__()
        self._archive_path = archive_path
        self._inner = zstandard.open(archive_path)

    def _reopen_stream(self) -> None:
        self._inner.close()
        logger.warning(
            "Reopening Zstandard stream for backwards seeking: {self._archive_path}"
        )
        if is_stream(self._archive_path):
            self._archive_path.seek(0)
        self._inner = zstandard.open(self._archive_path)

    def seekable(self) -> bool:
        if is_stream(self._archive_path):
            return self._archive_path.seekable()
        return True

    def read(self, n: int = -1) -> bytes:
        return self._inner.read(n)

    def readinto(self, b: bytearray | memoryview) -> int:
        return self._inner.readinto(b)  # type: ignore[attr-defined]

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        new_pos: int
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._inner.tell() + offset
        elif whence == io.SEEK_END:
            raise io.UnsupportedOperation(
                "seek backwards from end of stream in Zstandard "
            )
        else:
            raise ValueError(f"Invalid whence: {whence}")

        try:
            return self._inner.seek(offset, whence)
        except OSError as e:
            if "cannot seek zstd decompression stream backwards" in str(e):
                self._reopen_stream()
                return self._inner.seek(new_pos)
            raise

    def close(self) -> None:
        self._inner.close()
        super().close()


def _translate_zstandard_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, zstandard.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_zstandard_stream(path: str | BinaryIO) -> BinaryIO:
    if zstandard is None:
        raise PackageNotInstalledError(
            "zstandard package is not installed, required for Zstandard archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests

    return ExceptionTranslatingIO(
        lambda: ZstandardReopenOnBackwardsSeekIO(path), _translate_zstandard_exception
    )


def _translate_pyzstd_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, pyzstd.ZstdError):
        return ArchiveCorruptedError(f"Error reading Zstandard archive: {repr(e)}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError(f"Zstandard file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_pyzstd_stream(path: str | BinaryIO) -> BinaryIO:
    if pyzstd is None:
        raise PackageNotInstalledError(
            "pyzstd package is not installed, required for Zstandard archives"
        ) from None  # pragma: no cover -- pyzstd is installed for main tests
    return ExceptionTranslatingIO(
        lambda: ensure_binaryio(pyzstd.open(path)), _translate_pyzstd_exception
    )


def _translate_lz4_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, RuntimeError) and str(e).startswith("LZ4"):
        return ArchiveCorruptedError(f"Error reading LZ4 archive: {repr(e)}")
    elif isinstance(e, EOFError):
        return ArchiveEOFError(f"LZ4 file is truncated: {repr(e)}")
    return None  # pragma: no cover -- all possible exceptions should have been handled


def open_lz4_stream(path: str | BinaryIO) -> BinaryIO:
    if lz4 is None:
        raise PackageNotInstalledError(
            "lz4 package is not installed, required for LZ4 archives"
        ) from None  # pragma: no cover -- lz4 is installed for main tests

    return ExceptionTranslatingIO(
        lambda: ensure_binaryio(cast(lz4.frame.LZ4FrameFile, lz4.frame.open(path))),
        _translate_lz4_exception,
    )


def open_stream(
    format: ArchiveFormat, path_or_stream: str | BinaryIO, config: ArchiveyConfig
) -> BinaryIO:
    if format == ArchiveFormat.GZIP:
        if config.use_rapidgzip:
            return open_rapidgzip_stream(path_or_stream)
        else:
            return open_gzip_stream(path_or_stream)

    elif format == ArchiveFormat.BZIP2:
        if config.use_indexed_bzip2:
            return open_indexed_bzip2_stream(path_or_stream)
        else:
            return open_bzip2_stream(path_or_stream)

    elif format == ArchiveFormat.XZ:
        if config.use_python_xz:
            return open_python_xz_stream(path_or_stream)
        else:
            return open_lzma_stream(path_or_stream)

    elif format == ArchiveFormat.LZ4:
        return open_lz4_stream(path_or_stream)

    elif format == ArchiveFormat.ZSTD:
        if config.use_zstandard:
            return open_zstandard_stream(path_or_stream)
        else:
            return open_pyzstd_stream(path_or_stream)

    raise ValueError(f"Unsupported archive format: {format}")  # pragma: no cover
