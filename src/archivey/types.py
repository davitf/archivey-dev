"""
Types and enums used in Archivey.

The main types can be accessed from the :mod:`archivey` module, but any others that
are needed can be imported from here.
"""

import io  # Required for ReadableStreamLikeOrSimilar
import sys
from typing import (
    IO,
    TYPE_CHECKING,
    Callable,
    Protocol,
    Union,
    overload,
    runtime_checkable,
)

if TYPE_CHECKING or sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import IntEnum
from typing import Any, Optional, Tuple


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

    ISO = "iso"
    FOLDER = "folder"

    UNKNOWN = "unknown"


SINGLE_FILE_COMPRESSED_FORMATS = [
    ArchiveFormat.GZIP,
    ArchiveFormat.BZIP2,
    ArchiveFormat.XZ,
    ArchiveFormat.ZSTD,
    ArchiveFormat.LZ4,
]
TAR_COMPRESSED_FORMATS = [
    ArchiveFormat.TAR_GZ,
    ArchiveFormat.TAR_BZ2,
    ArchiveFormat.TAR_XZ,
    ArchiveFormat.TAR_ZSTD,
    ArchiveFormat.TAR_LZ4,
]

COMPRESSION_FORMAT_TO_TAR_FORMAT = {
    ArchiveFormat.GZIP: ArchiveFormat.TAR_GZ,
    ArchiveFormat.BZIP2: ArchiveFormat.TAR_BZ2,
    ArchiveFormat.XZ: ArchiveFormat.TAR_XZ,
    ArchiveFormat.ZSTD: ArchiveFormat.TAR_ZSTD,
    ArchiveFormat.LZ4: ArchiveFormat.TAR_LZ4,
}

TAR_FORMAT_TO_COMPRESSION_FORMAT = {
    v: k for k, v in COMPRESSION_FORMAT_TO_TAR_FORMAT.items()
}


class MemberType(StrEnum):
    FILE = "file"
    DIR = "dir"
    SYMLINK = "symlink"
    HARDLINK = "hardlink"
    OTHER = "other"


class CreateSystem(IntEnum):
    """Operating system on which the archive member was created."""

    FAT = 0
    AMIGA = 1
    VMS = 2
    UNIX = 3
    VM_CMS = 4
    ATARI_ST = 5
    OS2_HPFS = 6
    MACINTOSH = 7
    Z_SYSTEM = 8
    CPM = 9
    TOPS20 = 10
    NTFS = 11
    QDOS = 12
    ACORN_RISCOS = 13
    UNKNOWN = 255


@dataclass
class ArchiveInfo:
    """Detailed information about an archive's format."""

    format: ArchiveFormat = field(metadata={"description": "The archive format type"})
    version: Optional[str] = field(
        default=None,
        metadata={
            "description": 'The version of the archive format. Format-dependent (e.g. "4" for RAR4, "5" for RAR5).'
        },
    )
    is_solid: bool = field(
        default=False,
        metadata={
            "description": "Whether the archive is solid, i.e. decompressing a member may require decompressing others before it."
        },
    )
    extra: Optional[dict[str, Any]] = field(
        default=None,
        metadata={
            "description": "Extra format-specific information about the archive."
        },
    )
    comment: Optional[str] = field(
        default=None,
        metadata={
            "description": "A comment associated with the archive. Supported by some formats."
        },
    )


@dataclass
class ArchiveMember:
    """Represents a file within an archive."""

    filename: str = field(
        metadata={
            "description": "The name of the member. Directory names always end with a slash."
        }
    )
    file_size: Optional[int] = field(
        metadata={"description": "The size of the member's data in bytes, if known."}
    )
    compress_size: Optional[int] = field(
        metadata={
            "description": "The size of the member's compressed data in bytes, if known."
        }
    )
    mtime_with_tz: Optional[datetime] = field(
        metadata={
            "description": "The modification time of the member. May include a timezone (likely UTC) if the archive format uses global time, or be a naive datetime if the archive format uses local time."
        }
    )
    type: MemberType = field(metadata={"description": "The type of the member."})
    mode: Optional[int] = field(
        default=None, metadata={"description": "Unix permissions of the member."}
    )
    crc32: Optional[int] = field(
        default=None,
        metadata={"description": "The CRC32 checksum of the member's data, if known."},
    )
    compression_method: Optional[str] = field(
        default=None,
        metadata={
            "description": "The compression method used for the member, if known. Format-dependent."
        },
    )
    comment: Optional[str] = field(
        default=None,
        metadata={
            "description": "A comment associated with the member. Supported by some formats."
        },
    )
    create_system: Optional[CreateSystem] = field(
        default=None,
        metadata={
            "description": "The operating system on which the member was created, if known."
        },
    )
    encrypted: bool = field(
        default=False,
        metadata={"description": "Whether the member's data is encrypted, if known."},
    )
    extra: dict[str, Any] = field(
        default_factory=dict,
        metadata={"description": "Extra format-specific information about the member."},
    )
    link_target: Optional[str] = field(
        default=None,
        metadata={
            "description": "The target of the link, if the member is a symbolic or hard link. For hard links, this is the path of another file in the archive; for symbolic links, this is the target path relative to the directory containing the link. In some formats, the link target is stored in the member's data, and may not be available when getting the member list, and/or may be encrypted. In those cases, the link target will be filled when iterating through the archive."
        },
    )
    raw_info: Optional[Any] = field(
        default=None,
        metadata={"description": "The raw info object returned by the archive reader."},
    )
    _member_id: Optional[int] = field(
        default=None,
    )

    # A flag indicating whether the member has been modified by a filter.
    _edited_by_filter: bool = field(
        default=False,
    )

    @property
    def mtime(self) -> Optional[datetime]:
        """Convenience alias for :pyattr:`mtime_with_tz` without timezone information."""
        if self.mtime_with_tz is None:
            return None
        return self.mtime_with_tz.replace(tzinfo=None)

    @property
    def member_id(self) -> int:
        """A unique identifier for this member within the archive.

        Increasing in archive order, this can be used to distinguish
        members with the same filename and preserve ordering.
        """
        if self._member_id is None:
            raise ValueError("Member index not yet set")
        return self._member_id

    _archive_id: Optional[str] = field(
        default=None,
    )

    @property
    def archive_id(self) -> str:
        """A unique identifier for the archive. Used to distinguish between archives."""
        if self._archive_id is None:
            raise ValueError("Archive ID not yet set")
        return self._archive_id

    # Properties for zipfile compatibility (and others, as much as possible)
    @property
    def date_time(self) -> Optional[Tuple[int, int, int, int, int, int]]:
        """Returns the date and time as a tuple of (year, month, day, hour, minute, second), for `zipfile` compatibility."""
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
        """Convenience property returning ``True`` if the member is a regular file."""
        return self.type == MemberType.FILE

    @property
    def is_dir(self) -> bool:
        """Convenience property returning ``True`` if the member represents a directory."""
        return self.type == MemberType.DIR

    @property
    def is_link(self) -> bool:
        """Convenience property returning ``True`` if the member is a symbolic or hard link."""
        return self.type == MemberType.SYMLINK or self.type == MemberType.HARDLINK

    @property
    def is_other(self) -> bool:
        """Convenience property returning ``True`` if the member's type is neither file, directory nor link."""
        return self.type == MemberType.OTHER

    @property
    def CRC(self) -> Optional[int]:
        """Alias for `crc32`, for `zipfile` compatibility."""
        return self.crc32

    def replace(self, **kwargs: Any) -> "ArchiveMember":
        """Return a new instance with selected fields updated.

        This is primarily used by extraction filters to create modified
        versions of a member without mutating the original object.
        """
        replaced = replace(self, **kwargs)
        replaced._edited_by_filter = True
        return replaced


ExtractFilterFunc = Callable[[ArchiveMember, str], ArchiveMember | None]

IteratorFilterFunc = Callable[[ArchiveMember], ArchiveMember | None]


# A type that must match both ExtractFilterFunc and IteratorFilterFunc
# The callable must be able to handle both one and two arguments
class FilterFunc(Protocol):
    @overload
    def __call__(self, member: ArchiveMember) -> ArchiveMember | None: ...

    @overload
    def __call__(
        self, member: ArchiveMember, dest_path: str
    ) -> ArchiveMember | None: ...

    def __call__(
        self, member: ArchiveMember, dest_path: str | None = None
    ) -> ArchiveMember | None: ...


class ExtractionFilter(StrEnum):
    FULLY_TRUSTED = "fully_trusted"
    TAR = "tar"
    DATA = "data"


# Stream type definitions moved here to break circular import
@runtime_checkable
class ReadableBinaryStream(Protocol):
    """Protocol for a readable binary stream."""

    def read(self, n: int = -1, /) -> bytes: ...


ReadableStreamLikeOrSimilar = Union[ReadableBinaryStream, io.IOBase, IO[bytes]]
"""Type alias for objects that are like readable binary streams."""
