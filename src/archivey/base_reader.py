import abc
import logging
import os
from typing import IO, Callable, Iterator, List

from archivey.io_helpers import ErrorIOStream
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember

logger = logging.getLogger(__name__)


class ArchiveReader(abc.ABC):
    """Abstract base class for archive streams."""

    def __init__(self, format: ArchiveFormat, archive_path: str | bytes | os.PathLike):
        """Initialize the archive reader.

        Args:
            format: The format of the archive
        archive_path: The path to the archive file
        """
        self.format = format
        self.archive_path = (
            archive_path.decode("utf-8")
            if isinstance(archive_path, bytes)
            else str(archive_path)
        )

    @abc.abstractmethod
    def close(self) -> None:
        """Close the archive stream and release any resources."""
        pass

    @abc.abstractmethod
    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """Get a list of all members in the archive, or None if not available. May not be available for stream archives."""
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """Get a list of all members in the archive. May need to read the archive to get the members."""
        pass

    @abc.abstractmethod
    def iter_members(
        self, filter: Callable[[ArchiveMember], bool] | None = None
    ) -> Iterator[tuple[ArchiveMember, IO[bytes] | None]]:
        """Iterate over all members in the archive.

        Args:
            filter: A filter function to apply to each member. If specified, only
            members for which the filter returns True will be yielded.
            The filter may be called for all members either before or during the
            iteration, so don't rely on any specific behavior.

        Returns:
            A (ArchiveMember, IO[bytes]) iterator over the members. Each stream should
            be read before the next member is retrieved. The stream may be None if the
            member is not a file.
        """
        pass

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive.

        Returns:
            ArchiveInfo: Detailed format information including compression method
        """
        pass


class BaseArchiveReaderStreamingAccess(ArchiveReader):
    """Abstract base class for archive readers which are read as streams."""

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        return None

    def open(
        self, member: ArchiveMember, *, pwd: bytes | str | None = None
    ) -> IO[bytes]:
        raise ValueError(
            "This archive reader does not support opening specific members."
        )


class BaseArchiveReaderRandomAccess(ArchiveReader):
    """Abstract base class for archive readers which support random member access."""

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        return self.get_members()

    def iter_members(
        self, filter: Callable[[ArchiveMember], bool] | None = None
    ) -> Iterator[tuple[ArchiveMember, IO[bytes] | None]]:
        """Default implementation of iter_members for random access archives."""
        for member in self.get_members():
            if filter is None or filter(member):
                try:
                    stream = self.open(member)
                    yield member, stream
                    stream.close()
                except Exception as e:
                    logger.info(f"Error opening member {member.filename}: {e}")
                    # The caller should only get the exception if it actually tries
                    # to read from the stream.
                    yield member, ErrorIOStream(e)

    @abc.abstractmethod
    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> IO[bytes]:
        """Open a member for reading.

        Args:
            member: The member to open
            pwd: Password to use for decryption
        """
        pass

    def _build_member_map(self) -> dict[str, ArchiveMember]:
        if self._member_map is None:
            self._member_map = {
                member.filename: member for member in self.get_members()
            }
        return self._member_map

    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
        if isinstance(member_or_filename, ArchiveMember):
            return member_or_filename

        return self._build_member_map()[member_or_filename]
