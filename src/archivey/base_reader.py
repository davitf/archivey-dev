import abc
from typing import IO, Iterator, List
from archivey.types import ArchiveMember, ArchiveInfo, ArchiveFormat


class ArchiveReader(abc.ABC):
    """Abstract base class for archive readers."""

    def __init__(self, format: ArchiveFormat):
        """Initialize the archive reader.

        Args:
            format: The format of the archive
        """
        self._format = format

    @abc.abstractmethod
    def close(self) -> None:
        """Close the archive and release any resources."""
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """Get a list of all members in the archive. May not be available for stream archives."""
        pass

    @abc.abstractmethod
    def open(
        self, member: ArchiveMember, *, pwd: bytes | str | None = None
    ) -> IO[bytes]:
        """Open a member for reading.

        Args:
            member: The member to open
            pwd: Password to use for decryption

        Returns:
            A file-like object for reading the member's contents

        Raises:
            ArchiveError: For other archive-related errors
        """
        pass

    @abc.abstractmethod
    def iter_members(self) -> Iterator[ArchiveMember]:
        """Iterate over all members in the archive."""
        pass

    def get_format(self) -> ArchiveFormat:
        """Get the compression format of the archive.

        Returns:
            The format of the archive
        """
        return self._format

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information including compression method
        """
        pass
