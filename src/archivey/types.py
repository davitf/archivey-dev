import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

from dataclasses import dataclass
from typing import Optional, Any, Tuple
from datetime import datetime


class ArchiveFormat(StrEnum):
    """Supported compression formats."""

    ZIP = "zip"
    RAR = "rar"
    SEVENZIP = "7z"

    GZIP = "gz"
    BZIP2 = "bz2"
    XZ = "xz"
    ZSTD = "zstd"
    LZ4 = "lz4"

    TAR = "tar"
    TAR_GZ = "tar.gz"
    TAR_BZ2 = "tar.bz2"
    TAR_XZ = "tar.xz"
    TAR_ZSTD = "tar.zstd"
    TAR_LZ4 = "tar.lz4"

    UNKNOWN = "unknown"


SINGLE_FILE_COMPRESSED_FORMATS = [ArchiveFormat.GZIP, ArchiveFormat.BZIP2, ArchiveFormat.XZ, ArchiveFormat.ZSTD, ArchiveFormat.LZ4]
TAR_COMPRESSED_FORMATS = [ArchiveFormat.TAR_GZ, ArchiveFormat.TAR_BZ2, ArchiveFormat.TAR_XZ, ArchiveFormat.TAR_ZSTD, ArchiveFormat.TAR_LZ4]

COMPRESSION_FORMAT_TO_TAR_FORMAT = {
    ArchiveFormat.GZIP: ArchiveFormat.TAR_GZ,
    ArchiveFormat.BZIP2: ArchiveFormat.TAR_BZ2,
    ArchiveFormat.XZ: ArchiveFormat.TAR_XZ,
    ArchiveFormat.ZSTD: ArchiveFormat.TAR_ZSTD,
    ArchiveFormat.LZ4: ArchiveFormat.TAR_LZ4,
}

class MemberType(StrEnum):
    FILE = "file"
    DIR = "dir"
    LINK = "link"
    OTHER = "other"


@dataclass
class ArchiveInfo:
    """Detailed information about an archive's format."""

    format: str  # Will be ArchiveFormat from formats.py
    version: Optional[str] = None  # e.g. "4" for RAR4, "5" for RAR5
    is_solid: bool = False
    extra: Optional[dict[str, Any]] = None
    comment: Optional[str] = None


@dataclass
class ArchiveMember:
    """Represents a file within an archive."""

    filename: str
    size: Optional[int]
    mtime: Optional[datetime]
    type: MemberType
    permissions: Optional[int] = None
    crc32: Optional[int] = None
    compression_method: Optional[str] = None  # e.g. "deflate", "lzma", etc.
    comment: Optional[str] = None
    encrypted: bool = False
    extra: Optional[dict[str, Any]] = None
    link_target: Optional[str] = None
    link_target_type: Optional[MemberType] = None

    # The raw info from the archive reader
    raw_info: Optional[Any] = None

    # Properties for zipfile compatibility (and others, as much as possible)
    @property
    def date_time(self) -> Optional[Tuple[int, int, int, int, int, int]]:
        """Returns the date and time as a tuple."""
        if self.mtime is None:
            return None
        return (
            self.mtime.year,
            self.mtime.month,
            self.mtime.day,
            self.mtime.hour,
            self.mtime.minute,
            self.mtime.second,
        )

    @property
    def is_file(self) -> bool:
        return self.type == MemberType.FILE

    @property
    def is_dir(self) -> bool:
        return self.type == MemberType.DIR

    @property
    def is_link(self) -> bool:
        return self.type == MemberType.LINK

    @property
    def is_other(self) -> bool:
        return self.type == MemberType.OTHER
