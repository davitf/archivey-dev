import abc
from typing import IO, Iterator, List

from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember


class ArchiveReader(abc.ABC):
    """Abstract base class for archive readers.

    This class defines the common interface that all archive reader
    implementations must adhere to. It provides methods for listing members,
    opening members for reading, and retrieving archive metadata.

    Attributes:
        _format (ArchiveFormat): The format of the archive being read.
    """

    def __init__(self, archive_path: str, format: ArchiveFormat, **kwargs):
        """Initializes the archive reader.

        Args:
            archive_path: Path to the archive file.
            format: The `ArchiveFormat` enum member representing the archive type.
            **kwargs: Additional keyword arguments for the specific reader implementation.
        """
        self._archive_path = archive_path
        self._format = format

    @abc.abstractmethod
    def close(self) -> None:
        """Closes the archive file and releases any acquired resources.

        This method should be idempotent, meaning calling it multiple times
        should not cause errors.
        """
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """Retrieves a list of all members (files and directories) in the archive.

        For some archive formats or streaming scenarios, this might involve
        reading a significant portion of the archive. If iterating over members
        is preferred, use `iter_members()`.

        Returns:
            A list of `ArchiveMember` objects.

        Raises:
            ArchiveError: If there's an issue reading the archive structure.
            NotImplementedError: If the reader does not support listing all
                                 members at once (e.g., pure stream readers).
        """
        pass

    @abc.abstractmethod
    def open(
        self, member: ArchiveMember, *, pwd: bytes | str | None = None
    ) -> IO[bytes]:
        """Opens a specific member within the archive for reading its content.

        Args:
            member: The `ArchiveMember` object representing the member to open.
            pwd: Password to use for decryption if the member is encrypted.
                 This can be `str` or `bytes`.

        Returns:
            A buffered binary I/O stream (file-like object) from which the
            member's content can be read.

        Raises:
            ArchiveMemberNotFoundError: If the specified member cannot be found
                                      (e.g., if the archive structure changed).
            ArchiveEncryptedError: If the member is encrypted and no password
                                   is provided or the provided password is incorrect.
            ArchiveCorruptedError: If the member data is corrupted.
            ArchiveError: For other archive-related errors during opening.
        """
        pass

    @abc.abstractmethod
    def iter_members(self) -> Iterator[ArchiveMember]:
        """Returns an iterator over all `ArchiveMember` objects in the archive.

        This method is generally preferred for large archives as it can load
        member information on demand, reducing memory usage compared to
        `get_members()`.

        Yields:
            `ArchiveMember` objects from the archive.

        Raises:
            ArchiveError: If there's an issue iterating through the archive.
        """
        pass

    def get_format(self) -> ArchiveFormat:
        """Returns the format of the archive being read.

        Returns:
            The `ArchiveFormat` enum member.
        """
        return self._format

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        """Retrieves detailed information about the archive itself.

        This includes metadata like archive-level comments, specific compression
        methods used (if applicable), or other format-specific details.

        Returns:
            An `ArchiveInfo` object containing archive metadata.
        """
        pass
