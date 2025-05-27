import abc
from typing import IO, Iterator, List
from archivey.types import ArchiveMember, ArchiveInfo


class ArchiveReader(abc.ABC):
    """Abstract base class for archive readers."""
    
    @abc.abstractmethod
    def close(self) -> None:
        """Close the archive and release any resources."""
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """Get a list of all members in the archive. May not be available for stream archives."""
        pass

    @abc.abstractmethod
    def open(self, member: ArchiveMember, *, pwd: bytes | None = None) -> IO[bytes]:
        """Open a member for reading.
        
        Args:
            member: The member to open
            
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

    @abc.abstractmethod
    def get_format(self) -> str:  # Will be CompressionFormat from formats.py
        """Get the compression format of the archive.
        
        Returns:
            The format of the archive
        """
        pass

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.
        
        Returns:
            ArchiveInfo: Detailed format information including compression method
        """
        pass 