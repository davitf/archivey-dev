import bz2
import gzip
import io
import logging
import lzma
import os
import struct
from datetime import datetime
from typing import Iterator, List

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
)
from archivey.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    MemberType,
)

logger = logging.getLogger(__name__)


def _read_null_terminated_bytes(f: io.BufferedReader) -> bytes:
    """Reads bytes from a file object until a null byte or EOF is encountered.

    Args:
        f: A buffered binary reader object.

    Returns:
        The bytes read, excluding the null terminator.
    """
    str_bytes = bytearray()
    while True:
        b = f.read(1)
        if not b or b == b"\x00": # EOF or null byte
            break
        str_bytes.extend(b)
    return bytes(str_bytes)


def read_gzip_metadata(
    path: str, member: ArchiveMember, use_stored_metadata: bool = False
) -> None:
    """Extracts metadata from a GZIP file and updates an ArchiveMember object.

    Reads GZIP header and trailer to extract information like original filename,
    modification time, CRC32, uncompressed size, etc., without decompressing
    the entire file.

    Args:
        path: Path to the GZIP file.
        member: The `ArchiveMember` object to update with extracted metadata.
        use_stored_metadata: If True, updates `member.filename` and `member.mtime`
                             with values from the GZIP header if present.

    Raises:
        ArchiveFormatError: If the file is not a valid GZIP file.
        FileNotFoundError: If the `path` does not exist.
    """
    extra_fields: dict[str, Any] = {}

    with open(path, "rb") as f:
        # Read the fixed 10-byte GZIP header
        header = f.read(10)
        if len(header) != 10 or header[:2] != b"\x1f\x8b":
            raise ArchiveFormatError("Not a valid GZIP file")

        # Parse header fields
        id1, id2, cm, flg, mtime_timestamp, xfl, os = struct.unpack("<4BIBB", header)

        if mtime_timestamp != 0:
            extra_fields["mtime"] = datetime.fromtimestamp(mtime_timestamp)
            logger.info(
                f"GZIP metadata: mtime_timestamp={mtime_timestamp}, mtime={extra_fields['mtime']}"
            )
            if use_stored_metadata:
                member.mtime = extra_fields["mtime"]

        # Add compression method and level
        extra_fields["compress_type"] = cm  # 8 = deflate, consistent with ZIP
        extra_fields["compress_level"] = xfl  # Compression level (0-9)

        # Add operating system
        extra_fields["create_system"] = os  # 0 = FAT, 3 = Unix, etc.

        # Handle optional fields
        if flg & 0x04:  # FEXTRA
            # The extra field contains a 2-byte length and then the data
            xlen = struct.unpack("<H", f.read(2))[0]
            extra_fields["extra"] = f.read(xlen)  # Store raw extra field data

        if flg & 0x08:  # FNAME
            # The filename is a null-terminated string
            name_bytes = _read_null_terminated_bytes(f)
            extra_fields["original_filename"] = name_bytes.decode(
                "utf-8", errors="replace"
            )
            if use_stored_metadata:
                member.filename = extra_fields["original_filename"]

        if flg & 0x10:  # FCOMMENT
            comment_bytes = _read_null_terminated_bytes(f)
            extra_fields["comment"] = comment_bytes.decode("utf-8", errors="replace")

        if flg & 0x02:  # FHCRC
            f.read(2)  # Skip CRC16

        if extra_fields:
            if member.extra is None:
                member.extra = {}
            member.extra.update(extra_fields)

        # Now seek to trailer and read CRC32 and ISIZE
        f.seek(-8, 2)
        crc32, isize = struct.unpack("<II", f.read(8))
        member.crc32 = crc32
        member.size = isize


def _read_xz_multibyte_integer(data: bytes, offset: int) -> tuple[int, int]:
    """Reads a multi-byte integer from XZ format data.

    Args:
        data: Byte string containing the XZ index or header data.
        offset: Current offset in `data` to start reading from.

    Returns:
        A tuple containing the decoded integer value and the new offset
        after reading the integer.

    Raises:
        IndexError: If reading goes beyond the bounds of `data`.
    """
    value = 0
    shift = 0
    while True:
        if offset >= len(data): # pragma: no cover
            raise IndexError("Read past end of XZ multi-byte integer data.")
        byte_val = data[offset]
        offset += 1
        value |= (byte_val & 0x7F) << shift
        if byte_val & 0x80 == 0: # Most significant bit is 0, indicates end of number
            break
        shift += 7
        if shift > 63: # Protect against malformed data leading to infinite loop / too large shift
             raise ValueError("XZ multi-byte integer is too large or malformed.")
    return value, offset


XZ_MAGIC_FOOTER = b"YZ"
XZ_STREAM_HEADER_MAGIC = b"\xfd7zXZ\x00" # Stream Header magic


def read_xz_metadata(path: str, member: ArchiveMember) -> None:
    """Extracts uncompressed size from an XZ file and updates an ArchiveMember.

    Reads the XZ footer and index to determine the total uncompressed size
    of the data within one or more blocks.

    Args:
        path: Path to the XZ file.
        member: The `ArchiveMember` object to update with the uncompressed size.

    Raises:
        ValueError: If the XZ file format is invalid or parts are missing.
        FileNotFoundError: If `path` does not exist.
    """
    logger.debug(f"Reading XZ metadata for {path}")
    try:
        with open(path, "rb") as f:
            # Check footer magic first
            f.seek(-len(XZ_MAGIC_FOOTER), 2)
            if f.read(len(XZ_MAGIC_FOOTER)) != XZ_MAGIC_FOOTER: # pragma: no cover
                raise ValueError("Invalid XZ footer magic bytes.")

            f.seek(-12, 2)  # Footer is fixed at 12 bytes
            footer_data = f.read(12)

            if footer_data[-2:] != XZ_MAGIC_FOOTER: # Double check, also ensures read was successful
                 raise ValueError("Invalid XZ footer structure or missing magic.") # pragma: no cover

            # Backward Size (4 bytes, little-endian) in the footer points to the Index.
            # It's the size of the Index in 4-byte units, minus 1.
            backward_size_field = struct.unpack("<I", footer_data[4:8])[0]
            index_size = (backward_size_field + 1) * 4
            logger.debug(
                f"XZ metadata: index_size={index_size}, backward_size_field={backward_size_field}"
            )

            # Seek to the start of the Index
            # The Index is located `index_size` bytes before the footer.
            # Footer starts 12 bytes from EOF. Start of Index is `index_size` bytes before that.
            f.seek(-12 - index_size, 2)
            index_data = f.read(index_size)

            if not index_data or len(index_data) != index_size: # pragma: no cover
                raise ValueError(f"Could not read the full XZ index. Expected {index_size} bytes.")

            # Index Indicator (first byte of Index) must be 0x00.
            if index_data[0] != 0x00: # pragma: no cover
                raise ValueError("Invalid XZ index indicator byte.")

            # Number of Records (Blocks) is a multi-byte integer starting at offset 1.
            offset = 1
            number_of_records, offset = _read_xz_multibyte_integer(index_data, offset)

            total_uncompressed_size = 0
            # For each record, there's Unpadded Size and Uncompressed Size. We need the latter.
            for _ in range(number_of_records):
                if offset >= len(index_data): # pragma: no cover
                    raise ValueError("XZ index data truncated before reading all records.")
                _unpadded_size, offset = _read_xz_multibyte_integer(index_data, offset) # Skip Unpadded Size
                if offset >= len(index_data): # pragma: no cover
                    raise ValueError("XZ index data truncated before reading uncompressed size.")
                uncompressed_size, offset = _read_xz_multibyte_integer(index_data, offset)
                total_uncompressed_size += uncompressed_size

            member.size = total_uncompressed_size
            logger.debug(
                f"XZ metadata: total_uncompressed_size={total_uncompressed_size}, num_records={number_of_records}"
            )
    except FileNotFoundError: # pragma: no cover
        raise
    except Exception as e: # pragma: no cover
        logger.error(f"Error reading XZ metadata for {path}: {e}")
        # Do not set member.size if metadata reading fails, leave it as None.
        # Or raise a more specific ArchiveFormatError. For now, just log.


class BZ2Wrapper(io.IOBase):
    """Wrapper for BZ2 file objects to translate specific exceptions.

    This wrapper catches `OSError` during read operations, which can occur
    with corrupted BZ2 files, and re-raises it as `ArchiveCorruptedError`.
    It also translates `EOFError` to `ArchiveEOFError`.

    Args:
        fileobj: The BZ2 file object (e.g., from `bz2.open`).
    """
    def __init__(self, fileobj: io.IOBase):
        self._fileobj = fileobj

    def read(self, size: int = -1) -> bytes:
        """Reads data from the BZ2 stream.

        Args:
            size: Number of bytes to read. -1 for all.

        Returns:
            Bytes read from the stream.

        Raises:
            ArchiveCorruptedError: If an OSError occurs during reading.
            ArchiveEOFError: If an EOFError occurs during reading.
        """
        try:
            return self._fileobj.read(size)
        except OSError as e: # pragma: no cover
            # bz2 module can raise OSError for various corruption issues
            raise ArchiveCorruptedError(f"BZ2 file is corrupted or unreadable: {e}") from e
        except EOFError as e: # pragma: no cover
            # bz2.BZ2File.read can raise EOFError if stream ends unexpectedly
            raise ArchiveEOFError("BZ2 file is truncated or stream ended prematurely.") from e

    def close(self) -> None:
        """Closes the underlying BZ2 file object."""
        self._fileobj.close()

    # Ensure other necessary IOBase methods are proxied if used by shutil.copyfileobj or consumers
    def readable(self) -> bool: # pragma: no cover
        return self._fileobj.readable()

    def seekable(self) -> bool: # pragma: no cover
        return self._fileobj.seekable()

    def writable(self) -> bool: # pragma: no cover
        return self._fileobj.writable()

    @property
    def closed(self) -> bool: # pragma: no cover
        return self._fileobj.closed


class SingleFileReader(ArchiveReader):
    """Reader for single-file compressed archives (e.g., .gz, .bz2, .xz).

    This reader treats a single compressed file as an archive containing one
    member. It uses standard library modules like `gzip`, `bz2`, `lzma`,
    and can also use `zstandard` or `lz4` if installed.

    The "member name" is typically derived from the compressed filename by
    stripping its compression extension. For GZIP files, if an original
    filename is stored in the header, it can be used instead.
    """

    def __init__(
        self,
        archive_path: str,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None, # Kept for API consistency, but not used
        use_stored_metadata: bool = False,
        **kwargs,
    ):
        """Initializes the SingleFileReader.

        Args:
            archive_path: Path to the compressed file.
            format: The `ArchiveFormat` of the file (e.g., GZIP, BZIP2).
            pwd: Password for decryption. Not supported for these formats;
                 a ValueError will be raised if provided.
            use_stored_metadata: For GZIP files, if True, attempts to use
                                 metadata (like original filename and mtime)
                                 stored within the GZIP header to populate
                                 the `ArchiveMember` details.
            **kwargs: Additional options (currently ignored).

        Raises:
            ValueError: If `pwd` is provided (passwords not supported).
            RuntimeError: If a required decompression library (zstandard, lz4)
                          is not installed for the specified format.
            ArchiveError: If the format is unsupported.
        """
        super().__init__(archive_path, format, **kwargs)
        if pwd is not None: # pragma: no cover
            raise ValueError("Single-file compressed formats do not support password protection.")

        # self.archive_path is set by super()
        self.ext = os.path.splitext(self.archive_path)[1].lower()
        self._use_stored_metadata = use_stored_metadata # For GZIP

        # Derive member name: basename without the compression extension.
        # e.g., "file.txt.gz" -> "file.txt", "archive.tar.gz" -> "archive.tar"
        self._member_name = os.path.splitext(os.path.basename(self.archive_path))[0]

        self._decompressor_open_func: Any # Stores the open function (e.g. gzip.open)

        if format == ArchiveFormat.GZIP:
            self._decompressor_open_func = gzip.open
        elif format == ArchiveFormat.BZIP2:
            self._decompressor_open_func = bz2.open
        elif format == ArchiveFormat.XZ:
            self._decompressor_open_func = lzma.open
        elif format == ArchiveFormat.ZSTD:
            try:
                import zstandard # type: ignore
                self._decompressor_open_func = zstandard.open
            except ImportError: # pragma: no cover
                raise RuntimeError(
                    "The 'zstandard' module is not installed, which is required for .zst files."
                ) from None
        elif format == ArchiveFormat.LZ4:
            try:
                import lz4.frame # type: ignore
                self._decompressor_open_func = lz4.frame.open
            except ImportError: # pragma: no cover
                raise RuntimeError(
                    "The 'lz4' module is not installed, which is required for .lz4 files."
                ) from None
        else: # pragma: no cover
            raise ArchiveError(f"Unsupported single-file compression format: {format.value}")

        # Initialize the single ArchiveMember object
        # Some fields like size and crc32 might be populated by format-specific metadata readers
        stat_info = os.stat(self.archive_path)
        mtime = datetime.fromtimestamp(stat_info.st_mtime)
        
        self._member = ArchiveMember(
            filename=self._member_name, # Initial name, may be updated by GZIP metadata
            size=None,  # Attempt to fill this with format-specific metadata
            mtime=mtime, # File system mtime, may be updated by GZIP metadata
            type=MemberType.FILE,
            compression_method=format.value, # e.g., "gz", "bz2"
            crc32=None, # Attempt to fill for GZIP
            extra={}, # For storing format-specific header fields
            permissions=stat.S_IMODE(stat_info.st_mode)
        )

        # Populate metadata if available for the format
        try:
            if format == ArchiveFormat.GZIP:
                read_gzip_metadata(self.archive_path, self._member, self._use_stored_metadata)
            elif format == ArchiveFormat.XZ:
                read_xz_metadata(self.archive_path, self._member)
            # BZIP2, ZSTD, LZ4 do not have standardized, easily accessible embedded
            # metadata like filename or uncompressed size in their common stream formats
            # without beginning decompression or more complex parsing.
        except Exception as e: # pragma: no cover
            logger.warning(f"Could not read embedded metadata for {self.archive_path} (format {format.value}): {e}")


    def close(self) -> None:
        """Closes the reader. For SingleFileReader, this is a no-op
        as files are opened and closed within the `open` method.
        """
        pass # No persistent file object to close at this level

    def get_members(self) -> List[ArchiveMember]:
        """Returns a list containing the single `ArchiveMember` object.

        Returns:
            A list with one `ArchiveMember` element.
        """
        return [self._member]

    def get_archive_info(self) -> ArchiveInfo:
        """Returns information about the "archive" (the compressed file itself).

        Returns:
            An `ArchiveInfo` object.
        """
        return ArchiveInfo(
            format=self.get_format(), # The compression format
            is_solid=False, # Not applicable in the same way as multi-file archives
            comment=self._member.extra.get("comment") if self._member.extra else None, # From GZIP comment
            extra=self._member.extra, # Contains GZIP header fields if applicable
        )

    def open(self, member: ArchiveMember, *, pwd: bytes | str | None = None) -> io.IOBase:
        """Opens the single compressed member for reading its decompressed content.

        Args:
            member: The `ArchiveMember` to open. Must be the one member
                    managed by this reader.
            pwd: Password for decryption. Not supported; will raise ValueError.

        Returns:
            A file-like object providing the decompressed data stream.

        Raises:
            ValueError: If `pwd` is provided or if the requested `member`
                        is not the one associated with this file.
            ArchiveCorruptedError: If the compressed data is corrupted.
            ArchiveEOFError: If the file is truncated.
        """
        if pwd is not None: # pragma: no cover
            raise ValueError("Password protection is not supported for single compressed files.")
        if member is not self._member: # pragma: no cover
            # Ensure the user is asking for the *exact* ArchiveMember object we have.
            # This is stricter than comparing filenames, which might be ambiguous.
            raise ValueError("Requested member is not the one managed by this SingleFileReader.")

        try:
            # `self._decompressor_open_func` is set in __init__ (e.g., gzip.open)
            file_obj = self._decompressor_open_func(self.archive_path, "rb")
        except Exception as e: # pragma: no cover
            # Catch broad exceptions here as different libs might raise different things
            # on file open failure (e.g. if file removed after init)
            raise ArchiveError(f"Failed to open compressed file {self.archive_path} with {self._decompressor_open_func.__module__}: {e}") from e


        if self.get_format() == ArchiveFormat.BZIP2:
            return BZ2Wrapper(file_obj) # Apply error translation wrapper for BZ2
        return file_obj # type: ignore

    def iter_members(self) -> Iterator[ArchiveMember]:
        """Returns an iterator yielding the single `ArchiveMember`.

        Yields:
            The single `ArchiveMember` object.
        """
        yield self._member
