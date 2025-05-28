import io
import os
import gzip
import bz2
import lzma
import zstd
import lz4
from datetime import datetime
import struct
from typing import Iterator, List

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveEOFError,
    ArchiveError,
    ArchiveCorruptedError,
    ArchiveFormatError,
)
from archivey.types import (
    ArchiveInfo,
    ArchiveMember,
    ArchiveFormat,
    MemberType,
)


def _read_null_terminated_bytes(f: io.BufferedReader) -> bytes:
    str_bytes = bytearray()
    while True:
        b = f.read(1)
        if not b or b == b"\x00":
            break
        str_bytes.extend(b)
    return str_bytes


def read_gzip_metadata(path: str, member: ArchiveMember):
    """
    Extract metadata from a .gz file without decompressing and update the ArchiveMember:
    - original internal filename (if present) goes into extra field
    - modification time (as POSIX timestamp)
    - CRC32 of uncompressed data
    - uncompressed size (modulo 2^32)
    - compression method
    - compression level
    - operating system
    - extra field data
    """

    extra_fields = {}

    with open(path, "rb") as f:
        # Read the fixed 10-byte GZIP header
        header = f.read(10)
        if len(header) != 10 or header[:2] != b"\x1f\x8b":
            raise ArchiveFormatError("Not a valid GZIP file")

        # Parse header fields
        id1, id2, cm, flg, mtime_timestamp, xfl, os = struct.unpack("<3BIBBB", header)

        member.mtime = datetime.fromtimestamp(mtime_timestamp)

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


class BZ2Wrapper(io.IOBase):
    """Wrapper for bz2 file objects that converts OSError to ArchiveCorruptedError."""

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def read(self, size=-1):
        try:
            return self._fileobj.read(size)
        except OSError as e:
            raise ArchiveCorruptedError("BZ2 file is corrupted") from e
        except EOFError as e:
            raise ArchiveEOFError("BZ2 file is truncated") from e

    def close(self):
        self._fileobj.close()


class CompressedReader(ArchiveReader):
    """Reader for raw compressed files (gz, bz2, xz)."""

    def __init__(self, archive_path: str, *, pwd: bytes | None = None, **kwargs):
        """Initialize the reader.

        Args:
            archive_path: Path to the compressed file
            pwd: Password for decryption (not supported for compressed files)
            **kwargs: Additional options (ignored)
        """
        if pwd is not None:
            raise ValueError("Compressed files do not support password protection")
        self.archive_path = archive_path
        self.ext = os.path.splitext(archive_path)[1].lower()

        # Get the base name without compression extension
        self.member_name = os.path.splitext(os.path.basename(archive_path))[0]

        # Open the appropriate decompressor based on file extension
        if self.ext == ".gz":
            self.format = ArchiveFormat.GZIP
            self.decompressor = gzip.open
        elif self.ext == ".bz2":
            self.format = ArchiveFormat.BZIP2
            self.decompressor = bz2.open
        elif self.ext == ".xz":
            self.format = ArchiveFormat.XZ
            self.decompressor = lzma.open
        elif self.ext == ".zstd":
            self.format = ArchiveFormat.ZSTD
            self.decompressor = zstd.open
        elif self.ext == ".lz4":
            self.format = ArchiveFormat.LZ4
            self.decompressor = lz4.open
        else:
            raise ArchiveError(f"Unsupported compression format: {self.ext}")

        # Get file metadata
        self.mtime = datetime.fromtimestamp(os.path.getmtime(archive_path))

        # Create a single member representing the decompressed file
        self.member = ArchiveMember(
            filename=self.member_name,
            size=-1,  # This will be updated when we read the file
            mtime=self.mtime,
            type=MemberType.FILE,
            compression_method=self.format.value,
            crc32=None,
            extra=None,
        )

        if self.ext == ".gz":
            read_gzip_metadata(archive_path, self.member)

    def close(self) -> None:
        """Close the archive and release any resources."""
        pass

    def get_members(self) -> List[ArchiveMember]:
        """Get a list of all members in the archive."""
        return [self.member]

    def get_format(self) -> ArchiveFormat:
        return self.format

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format."""
        return ArchiveInfo(
            format=self.format.value,
            is_solid=True,  # Single-file compressed formats are effectively solid
            extra=None,
        )

    def open(self, member: ArchiveMember, *, pwd: bytes | None = None) -> io.IOBase:
        if pwd is not None:
            raise ValueError("Compressed files do not support password protection")
        if member != self.member:
            raise ValueError("Requested member is not part of this archive")
        fileobj = self.decompressor(self.archive_path)
        if self.ext == ".bz2":
            return BZ2Wrapper(fileobj)
        return fileobj

    def iter_members(self) -> Iterator[ArchiveMember]:
        return iter([self.member])
