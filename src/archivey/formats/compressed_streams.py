import abc
import bz2
import gzip
import io
import lzma
import os
import struct
import zlib
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    Generic,
    Optional,
    TypeVar,
    cast,
)

from typing_extensions import Buffer

from archivey.config import ArchiveyConfig, get_archivey_config
from archivey.internal.archive_stream import ArchiveStream
from archivey.internal.io_helpers import (
    ExceptionTranslatorFn,
    ensure_bufferedio,
    is_seekable,
    is_stream,
)
from archivey.types import StreamFormat

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


class ZstandardReopenOnBackwardsSeekIO(io.RawIOBase, BinaryIO):
    """Wrap a stream that supports seeking backwards, and reopen it if a backwards seek is attempted."""

    def __init__(self, archive_path: str | BinaryIO):
        super().__init__()
        self._archive_path = archive_path
        self._inner = zstandard.open(archive_path)
        self._size = None

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
            return is_seekable(self._archive_path)
        return True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def read(self, n: int = -1) -> bytes:
        return self._inner.read(n)

    def readinto(self, b: Buffer) -> int:
        return self._inner.readinto(b)  # type: ignore[attr-defined]

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        new_pos: int
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._inner.tell() + offset
        elif whence == io.SEEK_END:
            # Very inefficient, but we don't have a way to get the size of the stream
            # without reading it. This is the way _compression.DecompressReader does it.
            if self._size is None:
                while self._inner.read(65536):
                    pass
                self._size = self._inner.tell()
            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        try:
            return self._inner.seek(new_pos)
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

    return ZstandardReopenOnBackwardsSeekIO(path)


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


def _translate_lzip_exception(e: Exception) -> Optional[ArchiveError]:
    return None


def open_lzip_stream(path: str | BinaryIO) -> BinaryIO:
    return LzipDecompressorStream(path)


def open_zlib_stream(path: str | BinaryIO) -> BinaryIO:
    return ZlibDecompressorStream(path)


def _translate_zlib_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, zlib.error):
        if "incomplete" in str(e) or "truncated" in str(e):
            return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
        return ArchiveCorruptedError(f"Error reading Zlib archive: {repr(e)}")
    if isinstance(e, EOFError):
        return ArchiveEOFError(f"Zlib file is truncated: {repr(e)}")
    return None


DecompressorT = TypeVar("DecompressorT")


class DecompressorStream(io.RawIOBase, BinaryIO, Generic[DecompressorT]):
    """
    A base class for decompressor streams that follow the `_compression.DecompressReader` model.
    It supports seeking by re-reading the stream from the beginning.
    """

    def __init__(self, path: str | BinaryIO) -> None:
        super().__init__()
        if isinstance(path, (str, bytes, os.PathLike)):
            self._inner = open(path, "rb")
            self._should_close = True
        else:
            self._inner = ensure_bufferedio(path)
            self._should_close = False
        self._decompressor: DecompressorT = self._create_decompressor()
        self._buffer = bytearray()
        self._eof = False
        self._pos = 0
        self._size: int | None = None

    @abc.abstractmethod
    def _create_decompressor(self) -> DecompressorT: ...

    @abc.abstractmethod
    def _decompress_chunk(self, chunk: bytes) -> bytes: ...

    @abc.abstractmethod
    def _flush_decompressor(self) -> bytes: ...

    @abc.abstractmethod
    def _is_decompressor_finished(self) -> bool: ...

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:  # pragma: no cover - not used
        return False

    def seekable(self) -> bool:
        return self._inner.seekable()

    def _rewind(self) -> None:
        self._inner.seek(0)
        self._decompressor = self._create_decompressor()
        self._buffer.clear()
        self._eof = False
        self._pos = 0
        self._size = None

    def _read_decompressed_chunk(self) -> bytes:
        chunk = self._inner.read(65536)
        if not chunk:
            self._eof = True
            leftover = self._flush_decompressor()
            logger.info("EOF reached, leftover: %d", len(leftover))
            if not self._is_decompressor_finished():
                raise ArchiveEOFError("File is truncated")
            self._size = self._pos + len(self._buffer) + len(leftover)
            logger.info("EOF reached, size: %d", self._size)
            return leftover
        return self._decompress_chunk(chunk)

    def _seek_to_pos(self, pos: int) -> None:
        if pos == self._pos:
            return

        if pos < self._pos:
            self._rewind()
            assert self._pos == 0

        if self._pos + len(self._buffer) >= pos:
            del self._buffer[: pos - self._pos]
            self._pos = pos
            return

        self._pos += len(self._buffer)
        self._buffer.clear()

        while not self._eof:
            decompressed = self._read_decompressed_chunk()
            if self._pos + len(decompressed) >= pos:
                self._buffer.extend(decompressed[pos - self._pos :])
                self._pos = pos
                return
            self._pos += len(decompressed)

        # The position is past EOF
        self._pos = pos

    def readall(self) -> bytes:
        while not self._eof:
            self._buffer.extend(self._read_decompressed_chunk())

        data = bytes(self._buffer)
        self._pos += len(data)
        if self._size is not None:
            assert self._size == self._pos
        self._size = self._pos
        self._buffer.clear()
        return data

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n is None or n < 0:
            return self.readall()

        if len(self._buffer) < n and not self._eof:
            # Read only one more block
            self._buffer.extend(self._read_decompressed_chunk())

        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        self._pos += len(data)
        return data

    def readinto(self, b: bytearray | memoryview) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def close(self) -> None:
        if self._should_close:
            self._inner.close()
        super().close()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                # Read until EOF to get the size.
                self.readall()
                assert self._size is not None

            new_pos = self._size + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")

        if new_pos < 0:
            raise ValueError(f"Invalid offset: {offset}")

        self._seek_to_pos(new_pos)
        return self._pos

    def tell(self) -> int:
        return self._pos


_LZIP_MAGIC = b"LZIP"
_LZIP_HEADER_SIZE = 6
_LZIP_TRAILER_SIZE = 20
# lzip always uses lc=3, lp=0, pb=2: props_byte = (pb*5+lp)*9+lc = 0x5D
_LZIP_PROPS_BYTE = bytes([0x5D])


class _LzipState:
    """State machine for streaming multi-member lzip decompression (no external package)."""

    _NEED_HEADER = 0
    _IN_MEMBER = 1
    _NEED_TRAILER = 2

    def __init__(self) -> None:
        self._state = self._NEED_HEADER
        self._buf = bytearray()
        self._dec: lzma.LZMADecompressor | None = None
        self._crc = 0
        self._member_size = 0
        self._finished = False

    def reset(self) -> None:
        self.__init__()

    def feed(self, data: bytes) -> bytes:
        self._buf.extend(data)
        return self._process()

    def _process(self) -> bytes:
        output = bytearray()
        while True:
            if self._state == self._NEED_HEADER:
                if len(self._buf) < _LZIP_HEADER_SIZE:
                    break
                header = bytes(self._buf[:_LZIP_HEADER_SIZE])
                del self._buf[:_LZIP_HEADER_SIZE]
                self._start_member(header)
                self._state = self._IN_MEMBER

            elif self._state == self._IN_MEMBER:
                if not self._buf:
                    break
                chunk = bytes(self._buf)
                self._buf.clear()
                try:
                    assert self._dec is not None
                    plain = self._dec.decompress(chunk)
                except lzma.LZMAError as e:
                    raise ArchiveCorruptedError(
                        f"Error reading Lzip archive: {e}"
                    ) from e
                if plain:
                    self._crc = zlib.crc32(plain, self._crc)
                    self._member_size += len(plain)
                    output.extend(plain)
                if self._dec.eof:
                    unused = self._dec.unused_data
                    self._dec = None
                    self._buf[0:0] = unused
                    self._state = self._NEED_TRAILER

            elif self._state == self._NEED_TRAILER:
                if len(self._buf) < _LZIP_TRAILER_SIZE:
                    break
                trailer = bytes(self._buf[:_LZIP_TRAILER_SIZE])
                del self._buf[:_LZIP_TRAILER_SIZE]
                self._verify_trailer(trailer)
                self._state = self._NEED_HEADER

        return bytes(output)

    def flush(self) -> bytes:
        if self._state == self._NEED_HEADER and not self._buf:
            self._finished = True
            return b""
        raise ArchiveEOFError("Lzip file is truncated")

    def is_finished(self) -> bool:
        return self._finished

    def _start_member(self, header: bytes) -> None:
        if header[:4] != _LZIP_MAGIC:
            raise ArchiveCorruptedError(f"Invalid lzip magic: {header[:4]!r}")
        if header[4] != 1:
            raise ArchiveCorruptedError(f"Unsupported lzip version: {header[4]}")
        dict_size = 1 << (header[5] & 0x1F)
        lzma_header = _LZIP_PROPS_BYTE + struct.pack("<I", dict_size) + b"\xff" * 8
        self._dec = lzma.LZMADecompressor(format=lzma.FORMAT_ALONE)
        self._dec.decompress(lzma_header)
        self._crc = 0
        self._member_size = 0

    def _verify_trailer(self, trailer: bytes) -> None:
        crc32_stored, data_size, _member_size = struct.unpack_from("<IQQ", trailer, 0)
        if (self._crc & 0xFFFFFFFF) != crc32_stored:
            raise ArchiveCorruptedError(
                f"Lzip CRC32 mismatch: stored {crc32_stored:#010x}, "
                f"computed {self._crc & 0xFFFFFFFF:#010x}"
            )
        if self._member_size != data_size:
            raise ArchiveCorruptedError(
                f"Lzip size mismatch: stored {data_size}, actual {self._member_size}"
            )


class LzipDecompressorStream(DecompressorStream[_LzipState]):
    def _create_decompressor(self) -> _LzipState:
        return _LzipState()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.feed(chunk)

    def _flush_decompressor(self) -> bytes:
        return self._decompressor.flush()

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()


class ZlibDecompressorStream(DecompressorStream):
    def _create_decompressor(self) -> "zlib._Decompress":
        return zlib.decompressobj()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.decompress(chunk)

    def _flush_decompressor(self) -> bytes:
        return self._decompressor.flush()

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.eof


class BrotliDecompressorStream(DecompressorStream):
    """Wrap a file-like object and decompress it using ``brotli``."""

    def _create_decompressor(self) -> "brotli.Decompressor":
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
    if isinstance(e, brotli.error):
        return ArchiveCorruptedError(f"Error reading Brotli archive: {repr(e)}")
    return None


def open_brotli_stream(path: str | BinaryIO) -> BinaryIO:
    if brotli is None:
        raise PackageNotInstalledError(
            "brotli package is not installed, required for Brotli archives"
        ) from None
    return BrotliDecompressorStream(path)


def _translate_uncompresspy_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, ValueError) and "must be seekable" in str(e):
        return ArchiveStreamNotSeekableError(
            "uncompresspy does not support non-seekable streams"
        )
    return None


if uncompresspy is not None:

    class UncompresspyStream(uncompresspy.LZWFile):
        def __init__(self, path: str | BinaryIO) -> None:
            super().__init__(path)
            self._total_size = None

        def _find_total_size(self) -> int:
            if self._total_size is not None:
                return self._total_size

            # uncompresspy keeps checkpoints, so we can jump directly to the last known
            # position to avoid re-decompressing data before it.
            current_pos = self.tell()
            if self._checkpoints_uncompressed:
                max_known_pos = self._checkpoints_uncompressed[-1]
                if max_known_pos > current_pos:
                    self.seek(max_known_pos)
                    current_pos = max_known_pos

            while True:
                chunk = self.read(65536)
                if not chunk:
                    break
                current_pos += len(chunk)

            assert current_pos == self.tell()
            self._total_size = current_pos
            return self._total_size

        def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
            # Override the seek method to allow seeking from the end.
            if whence == io.SEEK_END:
                # Find the end of the stream.
                total_size = self._find_total_size()
                return super().seek(total_size + offset)

            return super().seek(offset, whence)


def open_uncompresspy_stream(path: str | BinaryIO) -> BinaryIO:
    if uncompresspy is None:
        raise PackageNotInstalledError(
            "uncompresspy package is not installed, required for Unix compress archives"
        ) from None  # pragma: no cover -- uncompresspy is installed for main tests

    return ensure_binaryio(UncompresspyStream(path))


def get_stream_open_fn(
    format: StreamFormat, config: ArchiveyConfig | None = None
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config is None:
        config = get_archivey_config()
    if format == StreamFormat.GZIP:
        if config.use_rapidgzip:
            return open_rapidgzip_stream, _translate_rapidgzip_exception
        return open_gzip_stream, _translate_gzip_exception

    if format == StreamFormat.BZIP2:
        if config.use_indexed_bzip2:
            return open_indexed_bzip2_stream, _translate_indexed_bzip2_exception
        return open_bzip2_stream, _translate_bz2_exception

    if format == StreamFormat.XZ:
        if config.use_python_xz:
            return open_python_xz_stream, _translate_python_xz_exception
        return open_lzma_stream, _translate_lzma_exception

    if format == StreamFormat.LZ4:
        return open_lz4_stream, _translate_lz4_exception

    if format == StreamFormat.LZIP:
        return open_lzip_stream, _translate_lzip_exception

    if format == StreamFormat.ZLIB:
        return open_zlib_stream, _translate_zlib_exception

    if format == StreamFormat.BROTLI:
        return open_brotli_stream, _translate_brotli_exception

    if format == StreamFormat.ZSTD:
        if config.use_zstandard:
            return open_zstandard_stream, _translate_zstandard_exception
        return open_pyzstd_stream, _translate_pyzstd_exception

    if format == StreamFormat.UNIX_COMPRESS:
        return open_uncompresspy_stream, _translate_uncompresspy_exception

    raise ValueError(f"Unsupported archive format: {format}")  # pragma: no cover


def open_stream(
    format: StreamFormat,
    path_or_stream: str | BinaryIO,
    config: ArchiveyConfig,
) -> BinaryIO:
    logger.debug(
        f"open_stream: format={format} path_or_stream={path_or_stream} config={config}"
    )
    open_fn, exception_translator = get_stream_open_fn(format, config)
    return ArchiveStream(
        open_fn=lambda: open_fn(path_or_stream),
        exception_translator=exception_translator,
        lazy=False,
        archive_path=path_or_stream if isinstance(path_or_stream, str) else None,
        member_name="<stream>",
        seekable=True,
    )
