import abc
import io
import logging
import os
import shutil
from typing import IO, Callable, Iterator, List

from archivey.exceptions import ArchiveMemberNotFoundError
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
        self._member_map: dict[str, ArchiveMember] | None = None

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
    def iter_members_with_io(
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

    # Context manager support
    def __enter__(self) -> "ArchiveReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


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

    def iter_members_with_io(
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

    def getinfo(self, name: str) -> ArchiveMember:
        for member in self.get_members():
            if member.filename == name:
                return member
        raise ArchiveMemberNotFoundError(f"Member not found: {name}")

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _write_member(
        self,
        root_path: str,
        member: ArchiveMember,
        preserve_links: bool,
        stream: IO[bytes] | None,
    ) -> str | None:
        target_path = os.path.join(root_path, member.filename)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        if member.is_dir:
            os.makedirs(target_path, exist_ok=True)
        elif member.is_link:
            if not preserve_links:
                return None
            assert stream is not None
            link_target = stream.read().decode("utf-8")
            os.symlink(link_target, target_path)
        elif member.is_file:
            if stream is None:
                stream = io.BytesIO(b"")
            with open(target_path, "wb") as dst:
                shutil.copyfileobj(stream, dst)

        if member.mtime:
            os.utime(target_path, (member.mtime.timestamp(), member.mtime.timestamp()))

        if member.mode:
            os.chmod(target_path, member.mode)

        return target_path

    def extract(
        self,
        member: ArchiveMember | str,
        root_path: str | None = None,
        preserve_links: bool = True,
    ) -> str | None:
        if root_path is None:
            root_path = os.getcwd()

        # Prefer direct open for random access readers
        if isinstance(self, BaseArchiveReaderRandomAccess):
            member_obj = self.get_member(member)
            with self.open(member_obj) as stream:
                return self._write_member(root_path, member_obj, preserve_links, stream)

        member_name = member.filename if isinstance(member, ArchiveMember) else member
        for m, stream in self.iter_members_with_io():
            if m.filename == member_name:
                result = self._write_member(root_path, m, preserve_links, stream)
                if stream is not None:
                    stream.close()
                return result

        raise ArchiveMemberNotFoundError(f"Member not found: {member_name}")

    def extractall(self, path: str | None = None, preserve_links: bool = True) -> None:
        if path is None:
            path = os.getcwd()
        for member, stream in self.iter_members_with_io():
            self._write_member(path, member, preserve_links, stream)
            if stream is not None:
                stream.close()
