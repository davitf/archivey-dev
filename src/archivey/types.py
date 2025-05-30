import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Tuple


class ArchiveFormat(StrEnum):
    """Enumeration of supported archive and compression formats.

    This enum provides standardized string identifiers for various archive
    types (like ZIP, RAR, TAR) and single-file compression formats (like GZIP,
    BZIP2). It also includes combined formats such as TAR_GZ.
    """

    ZIP = "zip"  #: ZIP archive format.
    RAR = "rar"  #: RAR archive format.
    SEVENZIP = "7z"  #: 7-Zip archive format.

    GZIP = "gz"  #: GZIP single-file compression.
    BZIP2 = "bz2"  #: BZIP2 single-file compression.
    XZ = "xz"  #: XZ single-file compression.
    ZSTD = "zstd"  #: Zstandard single-file compression.
    LZ4 = "lz4"  #: LZ4 single-file compression.

    TAR = "tar"  #: TAR archive format (uncompressed).
    TAR_GZ = "tar.gz"  #: GZIP compressed TAR archive.
    TAR_BZ2 = "tar.bz2"  #: BZIP2 compressed TAR archive.
    TAR_XZ = "tar.xz"  #: XZ compressed TAR archive.
    TAR_ZSTD = "tar.zst"  #: Zstandard compressed TAR archive.
    TAR_LZ4 = "tar.lz4"  #: LZ4 compressed TAR archive.

    UNKNOWN = "unknown"  #: Unknown or unsupported format.


#: List of `ArchiveFormat` members that represent single-file compression types.
SINGLE_FILE_COMPRESSED_FORMATS = [
    ArchiveFormat.GZIP,
    ArchiveFormat.BZIP2,
    ArchiveFormat.XZ,
    ArchiveFormat.ZSTD,
    ArchiveFormat.LZ4,
]

#: List of `ArchiveFormat` members that represent compressed TAR archives.
TAR_COMPRESSED_FORMATS = [
    ArchiveFormat.TAR_GZ,
    ArchiveFormat.TAR_BZ2,
    ArchiveFormat.TAR_XZ,
    ArchiveFormat.TAR_ZSTD,
    ArchiveFormat.TAR_LZ4,
]

#: Mapping from single-file compression formats to their corresponding TAR-compressed formats.
COMPRESSION_FORMAT_TO_TAR_FORMAT = {
    ArchiveFormat.GZIP: ArchiveFormat.TAR_GZ,
    ArchiveFormat.BZIP2: ArchiveFormat.TAR_BZ2,
    ArchiveFormat.XZ: ArchiveFormat.TAR_XZ,
    ArchiveFormat.ZSTD: ArchiveFormat.TAR_ZSTD,
    ArchiveFormat.LZ4: ArchiveFormat.TAR_LZ4,
}


class MemberType(StrEnum):
    """Enumeration of archive member types."""
    FILE = "file"    #: Regular file.
    DIR = "dir"      #: Directory.
    LINK = "link"    #: Symbolic link or hard link.
    OTHER = "other"  #: Other special file type (e.g., device, FIFO).


@dataclass
class ArchiveInfo:
    """Represents detailed information about an archive file itself.

    This class stores metadata pertaining to the overall archive, such as its
    format, version, whether it's a solid archive, and any archive-level
    comments or extra format-specific data.

    Attributes:
        format: The detected `ArchiveFormat` of the archive (as a string value
                from the `ArchiveFormat` enum).
        version: Optional string indicating the specific version of the archive
                 format (e.g., "4" for RAR4, "5" for RAR5).
        is_solid: Boolean indicating if the archive is solid. Solid archives
                  compress multiple files together, potentially improving
                  compression ratios but requiring sequential extraction.
        extra: An optional dictionary for storing any additional format-specific
               metadata about the archive (e.g., header encryption status).
        comment: An optional string containing the global comment for the archive,
                 if supported by the format and present.
    """
    format: str
    version: Optional[str] = None
    is_solid: bool = False
    extra: Optional[dict[str, Any]] = None
    comment: Optional[str | bytes] = None # Comment can be bytes for some formats initially


@dataclass
class ArchiveMember:
    """Represents a single member (file, directory, link) within an archive.

    This dataclass holds common metadata for an archive member, aiming for
    compatibility with `zipfile.ZipInfo` where appropriate, while also
    providing fields relevant to other archive formats.

    Attributes:
        filename: The name or path of the member within the archive.
        size: The uncompressed size of the member in bytes. `None` if unknown
              or not applicable (e.g., for some directories or links).
        mtime: The modification time of the member as a `datetime` object.
               `None` if not available. Assumed to be naive (no timezone).
        type: The type of the member, represented by the `MemberType` enum
              (e.g., FILE, DIR, LINK).
        permissions: Optional integer representing the file permissions/mode
                     (e.g., from `stat.S_IMODE`). `None` if not available.
        crc32: Optional integer representing the CRC32 checksum of the
               uncompressed member data. `None` if not available or not applicable.
        compression_method: Optional string describing the compression method
                            used for this member (e.g., "deflate", "lzma").
                            `None` if uncompressed or unknown.
        comment: Optional string containing a comment specific to this member.
                 `None` if not available.
        encrypted: Boolean indicating if the member is encrypted.
        extra: An optional dictionary for storing any additional format-specific
               metadata about the member (e.g., UID, GID for TAR).
        link_target: If the member is a symbolic or hard link (`type` is LINK),
                     this field stores the target path of the link as a string.
                     `None` otherwise.
        link_target_type: Optional `MemberType` indicating the type of the linked
                          target, if known. (Currently not widely populated).
        raw_info: Optional storage for the original member info object from the
                  underlying archive library (e.g., `zipfile.ZipInfo`,
                  `tarfile.TarInfo`). This can be used to access format-specific
                  attributes not covered by the common fields.
    """
    filename: str
    size: Optional[int]
    mtime: Optional[datetime]
    type: MemberType
    permissions: Optional[int] = None
    crc32: Optional[int] = None
    compression_method: Optional[str] = None
    comment: Optional[str | bytes] = None # Comment can be bytes
    encrypted: bool = False
    extra: Optional[dict[str, Any]] = None
    link_target: Optional[str] = None
    link_target_type: Optional[MemberType] = None # Future use: e.g. is link to dir or file?

    raw_info: Optional[Any] = None

    @property
    def date_time(self) -> Optional[Tuple[int, int, int, int, int, int]]:
        """Modification time as a tuple: (year, month, day, hour, minute, second).

        This is provided for compatibility with `zipfile.ZipInfo.date_time`.

        Returns:
            A 6-tuple representing the modification date and time, or `None`
            if `mtime` is not set.
        """
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
        """Returns True if the member is a regular file."""
        return self.type == MemberType.FILE

    @property
    def is_dir(self) -> bool:
        """Returns True if the member is a directory."""
        return self.type == MemberType.DIR

    @property
    def is_link(self) -> bool:
        """Returns True if the member is a symbolic or hard link."""
        return self.type == MemberType.LINK

    @property
    def is_other(self) -> bool: # pragma: no cover
        """Returns True if the member is of another special type."""
        return self.type == MemberType.OTHER
