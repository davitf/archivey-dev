import abc
import os
from typing import BinaryIO, Callable, Collection, Iterator, List

from archivey.internal.io_helpers import is_stream
from archivey.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    ExtractFilterFunc,
    ExtractionFilter,
    IteratorFilterFunc,
)


class ArchiveReader(abc.ABC):
    """
    Abstract base class defining the interface for an archive reader.

    This class provides a consistent way to interact with different archive
    formats. Subclasses must implement the abstract methods to provide
    format-specific functionality.
    """

    path_or_stream: str | BinaryIO
    path_str: str | None

    def __init__(
        self,
        archive_path: BinaryIO | str | bytes | os.PathLike,
        format: ArchiveFormat,
    ):
        if is_stream(archive_path):
            self.path_str = None
            self.path_or_stream = archive_path

        elif isinstance(archive_path, (str, os.PathLike)):
            self.path_or_stream = self.path_str = str(archive_path)
        elif isinstance(archive_path, bytes):
            self.path_or_stream = self.path_str = archive_path.decode("utf-8")
        else:
            raise ValueError(
                f"Expected a stream, str, or bytes, got {type(archive_path)} {archive_path!r}"
            )

        self.format = format

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close the archive and release any underlying resources.

        This method is idempotent (callable multiple times without error).
        It is automatically called when the reader is used as a context manager.
        """
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """
        Return a list of all ArchiveMember objects in the archive.

        This method guarantees returning the full list of members. However, for
        some archive types without a central directory (e.g. TAR) or streaming
        modes, this might involve processing the entire archive if the member list
        isn't available upfront (e.g., iterating through a tar stream).

        This method always raises ValueError if the archive is opened in streaming
        mode, to help avoid programming errors.

        Returns:
            A list of all ArchiveMember objects.

        Raises:
            ArchiveError: If there's an issue reading member information.
            ValueError: If the archive is opened in streaming-only mode.
        """
        pass

    @abc.abstractmethod
    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """
        Return a list of all ArchiveMember objects if readily available.

        For most archive formats (e.g., ZIP with a central directory), the full
        list of members can be obtained quickly without reading the entire archive.
        For others, especially stream-based formats (e.g. TAR), this might not be
        possible or efficient.

        Returns:
            A list of ArchiveMember objects, or None if the list is not readily
            available without significant processing.
        """
        pass

    @abc.abstractmethod
    def iter_members_with_io(
        self,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: IteratorFilterFunc | ExtractionFilter | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        """
        Iterate over members in the archive, yielding a tuple of (ArchiveMember, BinaryIO_stream).

        The returned stream is for reading the content of the member. The stream
        will be None for non-file members (e.g., directories, symlinks if not
        dereferenced to content).

        Args:
            members: Optional. A collection of specific member names (str) or
                ArchiveMember objects to iterate over. If None, iterates over all
                members. Can also be a callable that takes an ArchiveMember and
                returns True if it should be included.
            pwd: Optional password (str or bytes) for decrypting members, if the
                archive or specific members are encrypted.
            filter: Optional callable applied to each member. It may accept an
                optional destination path argument with default ``None``. When
                used with ``iter_members_with_io`` the path is ``None``. Return
                the member to include it, or ``None`` to skip.

        Yields:
            tuple[ArchiveMember, BinaryIO | None]:
                Each yielded item is a tuple containing the ``ArchiveMember``
                object and a binary I/O stream for reading its content.  The
                stream is ``None`` for non-file entries.  Streams are closed
                automatically when iteration advances to the next member or when
                the generator is closed, so they should be consumed before
                requesting another member.

        Raises:
            ArchiveEncryptedError: If a member is encrypted and `pwd` is incorrect
                                   or not provided.
            ArchiveCorruptedError: If member data is found to be corrupt during iteration.
            ArchiveIOError: For other I/O related issues during member access.
        """
        pass

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        """
        Return an ArchiveInfo object containing metadata about the archive itself.

        This includes information like the archive format, whether it's solid,
        any archive-level comments, etc.

        Returns:
            An ArchiveInfo object.
        """
        pass

    @abc.abstractmethod
    def has_random_access(self) -> bool:
        """
        Return True if the archive supports random access to its members.

        Random access means methods like `open()`, `extract()` can be used to
        access individual members directly without iterating through the entire
        archive from the beginning. Returns False for streaming-only access
        (e.g., reading from a non-seekable stream or some tar variants).

        Returns:
            bool: True if random access is supported, False otherwise.
        """
        pass

    @abc.abstractmethod
    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
        """
        Retrieve a specific ArchiveMember object by its name or by an existing ArchiveMember.

        If `member_or_filename` is an ArchiveMember instance, this method might
        be used to refresh its state or confirm its presence in the archive.
        If it's a string, it's treated as the filename of the member to find.

        Args:
            member_or_filename: The filename (str) of the member to retrieve, or
                an ArchiveMember object.

        Returns:
            The ArchiveMember object for the specified entry.

        Raises:
            ArchiveMemberNotFoundError: If no member with the given name is found.
        """
        pass

    @abc.abstractmethod
    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        """
        Open a specific member of the archive for reading and return a binary I/O stream.

        This method is typically available if `has_random_access()` returns True.
        For symlinks, this should open the target file's content.

        Args:
            member_or_filename: The ArchiveMember object or the filename (str) of
                the member to open.
            pwd: Optional password (str or bytes) for decrypting the member if it's
                encrypted.

        Returns:
            A binary I/O stream (BinaryIO) for reading the member's content.

        Raises:
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveMemberCannotBeOpenedError: If the member is a type that cannot be
                                            opened (e.g., a directory).
            ArchiveEncryptedError: If the member is encrypted and `pwd` is incorrect
                                   or not provided.
            ArchiveCorruptedError: If the member data is found to be corrupt.
            NotImplementedError: If random access `open()` is not supported by this reader.
        """
        pass

    @abc.abstractmethod
    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | os.PathLike | None = None,
        pwd: bytes | str | None = None,
    ) -> str | None:
        """Extract a member to a path.

        Args:
            member: The member to extract
            path: The path to extract to
            pwd: Password to use for decryption, if needed and different from the one
            used when opening the archive.
        """
        pass

    @abc.abstractmethod
    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: ExtractFilterFunc | ExtractionFilter | None = None,
    ) -> dict[str, ArchiveMember]:
        """
        Extract all (or a specified subset of) members to the given path.

        Args:
            path: Target directory for extraction. Defaults to the current working
                directory if None. The directory will be created if it doesn't exist.
            members: Optional. A collection of member names (str) or ArchiveMember
                objects to extract. If None, all members are extracted. Can also be
                a callable that takes an ArchiveMember and returns True if it should
                be extracted.
            pwd: Optional password (str or bytes) for decrypting members if the
                archive or specific members are encrypted.
            filter: Optional callable that takes an ArchiveMember and the
                destination path. It should return the (possibly modified)
                ArchiveMember if it should be extracted, or ``None`` to skip the
                member. This is applied after the ``members`` selection.

        Returns:
            A dictionary mapping extracted file paths (absolute) to their
            corresponding ArchiveMember objects.

        Raises:
            ArchiveEncryptedError: If a member is encrypted and `pwd` is incorrect
                                   or not provided.
            ArchiveCorruptedError: If member data is found to be corrupt during extraction.
            ArchiveIOError: For other I/O related issues during extraction.
            SameFileError: If an extraction would overwrite a file that is part of
                           the archive itself (not yet implemented).
        """
        pass

    @abc.abstractmethod
    def resolve_link(self, member: ArchiveMember) -> ArchiveMember | None:
        """
        Resolve a link member to its ultimate target ArchiveMember.

        If the given member is not a link, it should typically return the member itself
        (or None if strict link-only resolution is desired, though returning self is safer).
        If the member is a link (symlink or hardlink), this method will attempt
        to find the final, non-link target it points to.

        Args:
            member: The ArchiveMember to resolve. This member should belong to this archive.

        Returns:
            The resolved ArchiveMember if the target exists and is found,
            or None if the link target cannot be resolved (e.g., broken link,
            target not found, or if the input member is not a link and strict
            resolution is applied).
        """
        pass

    # Context manager support
    def __enter__(self) -> "ArchiveReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
