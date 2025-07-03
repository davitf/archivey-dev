from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Optional, Tuple

from .enums import ArchiveFormat, CreateSystem, MemberType

# Hide internal implementation details from the generated documentation
__pdoc__ = {
    "ArchiveMember.__init__": False,
    "ArchiveMember._member_id": False,
    "ArchiveMember._archive_id": False,
    "ArchiveMember._edited_by_filter": False,
}


@dataclass
class ArchiveInfo:
    """Detailed information about an archive's format."""

    format: ArchiveFormat
    version: Optional[str] = None
    """The version of the archive format. Format-dependent (e.g. "4" for RAR4, "5" for RAR5)."""

    is_solid: bool = False
    """Whether the archive is solid, i.e. decompressing a member may require decompressing others before it."""

    extra: Optional[dict[str, Any]] = None
    """Extra format-specific information about the archive."""

    comment: Optional[str] = None
    """A comment associated with the archive. Supported by some formats."""


@dataclass
class ArchiveMember:
    """Represents a file within an archive."""

    filename: str
    """The name of the member. Directory names always end with a slash."""

    file_size: Optional[int]
    """The size of the member's data in bytes, if known."""

    compress_size: Optional[int]
    """The size of the member's compressed data in bytes, if known."""

    mtime_with_tz: Optional[datetime]
    """The modification time of the member. May include a timezone (likely UTC) if the archive format uses global time, or be a naive datetime if the archive format uses local time."""

    type: MemberType
    """The type of the member."""

    mode: Optional[int] = None
    """Unix permissions of the member."""

    crc32: Optional[int] = None
    """The CRC32 checksum of the member's data, if known."""

    compression_method: Optional[str] = None
    """The compression method used for the member, if known. Format-dependent."""

    comment: Optional[str] = None
    """A comment associated with the member. Supported by some formats."""

    create_system: Optional[CreateSystem] = None
    """The operating system on which the member was created, if known."""

    encrypted: bool = False
    """Whether the member's data is encrypted, if known."""

    extra: dict[str, Any] = field(default_factory=dict)
    """Extra format-specific information about the member."""

    link_target: Optional[str] = None
    """The target of the link, if the member is a symbolic or hard link. For hard links, this is the path of another file in the archive; for symbolic links, this is the target path relative to the directory containing the link. In some formats, the link target is stored in the member's data, and may not be available when getting the member list, and/or may be encrypted. In those cases, the link target will be filled when iterating through the archive."""

    raw_info: Optional[Any] = None
    """The raw info object returned by the archive reader."""

    _member_id: Optional[int] = None

    # A flag indicating whether the member has been modified by a filter.
    _edited_by_filter: bool = False

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

    _archive_id: Optional[str] = None

    @property
    def archive_id(self) -> str:
        """A unique identifier for the archive. Used to distinguish between archives."""
        if self._archive_id is None:
            raise ValueError("Archive ID not yet set")
        return self._archive_id

    # Properties for zipfile compatibility (and others, as much as possible)
    @property
    def date_time(self) -> Optional[Tuple[int, int, int, int, int, int]]:
        """Returns the date and time as a tuple of (year, month, day, hour, minute, second).

        This accessor is provided for :mod:`zipfile` compatibility."""
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
        """Alias for :pyattr:`crc32` for zipfile compatibility."""
        return self.crc32

    def replace(self, **kwargs: Any) -> "ArchiveMember":
        """Return a new instance with selected fields updated.

        This is primarily used by extraction filters to create modified
        versions of a member without mutating the original object.
        """
        replaced = replace(self, **kwargs)
        replaced._edited_by_filter = True
        return replaced
