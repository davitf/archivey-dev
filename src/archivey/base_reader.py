import abc
import functools
import io
import logging
import os
import shutil
from typing import BinaryIO, Callable, Iterable, Iterator, List, Union

from archivey.config import ArchiveyConfig, get_default_config
from archivey.exceptions import ArchiveError, ArchiveMemberNotFoundError
from archivey.io_helpers import ErrorIOStream, LazyOpenIO
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

logger = logging.getLogger(__name__)


def _set_member_metadata(member: ArchiveMember, target_path: str) -> None:
    if member.mtime:
        os.utime(target_path, (member.mtime.timestamp(), member.mtime.timestamp()))

    if member.mode:
        os.chmod(target_path, member.mode)


def apply_members_metadata(members: Iterable[ArchiveMember], root_path: str) -> None:
    """Apply stored metadata for a list of members extracted to ``root_path``."""

    for member in members:
        target_path = os.path.join(root_path, member.filename)
        if os.path.exists(target_path):
            logger.info(f"Setting metadata for {target_path}")
            _set_member_metadata(member, target_path)
        else:
            logger.info(f"Skipping metadata for {target_path} (not found)")


def _write_member(
    root_path: str,
    member: ArchiveMember,
    preserve_links: bool,
    stream: BinaryIO | None,
) -> str | None:
    file_to_write_path = os.path.join(root_path, member.filename)
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(file_to_write_path), exist_ok=True)

    hardlink_source_path = None
    if member.type == MemberType.HARDLINK and member.link_target:
        hardlink_source_path = os.path.join(root_path, member.link_target)

    # Handle pre-existing file/directory/link at the target location
    perform_unlink = True
    if member.type == MemberType.HARDLINK and \
       hardlink_source_path == file_to_write_path and \
       os.path.lexists(file_to_write_path) and \
       not os.path.isdir(file_to_write_path): # Ensure it's a file/link, not a dir
        logger.info(f"Hardlink {member.filename} target {member.link_target} already exists at {file_to_write_path}. Skipping removal.")
        perform_unlink = False # Do not remove the existing file, it's the hardlink source

    if perform_unlink and os.path.lexists(file_to_write_path):
        if os.path.isdir(file_to_write_path) and not os.path.islink(file_to_write_path): # It's a real directory
            if member.type != MemberType.DIR:
                logger.info(f"Removing existing directory at {file_to_write_path} to replace with {member.type.value}")
                shutil.rmtree(file_to_write_path)
        else: # It's a file or a symlink
            logger.info(f"Removing existing file/symlink at {file_to_write_path} to replace with {member.type.value}")
            os.unlink(file_to_write_path)

    if member.type == MemberType.DIR:
        os.makedirs(file_to_write_path, exist_ok=True)
    elif member.type == MemberType.SYMLINK:
        if not preserve_links:
            return None
        # stream is not used for symlinks, but link_target is crucial
        link_target = member.link_target
        if not link_target:
            # Log a warning or raise error, consistent with how tarfile handles this
            logger.warning(f"Symlink target is empty for {member.filename}, skipping.")
            return None

        logger.info(f"Creating symlink: {file_to_write_path} -> {link_target}")
        os.symlink(link_target, file_to_write_path)
    elif member.type == MemberType.HARDLINK:
        if not preserve_links:
            return None
        # stream is not used for hardlinks
        link_target = member.link_target
        if not link_target:
            logger.warning(f"Hardlink target is empty for {member.filename}, skipping.")
            return None

        # Hardlink target path needs to be relative to the root_path, or absolute.
        # tarfile makes linkname relative to the extraction directory.
        # Assuming link_target is stored appropriately (e.g., relative to archive root).
        # The actual source file for the hardlink must exist.
        # hardlink_source_path is already defined and calculated before pre-existing checks.

        # If the source and target are the same and the file already exists (handled by perform_unlink = False),
        # we don't need to do anything here.
        if not (hardlink_source_path == file_to_write_path and \
                os.path.exists(file_to_write_path) and \
                not os.path.isdir(file_to_write_path)): # ensure it is not a directory
            logger.info(f"Creating hardlink: {file_to_write_path} -> {hardlink_source_path}")
            try:
                os.link(hardlink_source_path, file_to_write_path)
            except OSError as e:
                logger.error(f"Failed to create hardlink {file_to_write_path} -> {hardlink_source_path}: {e}. "
                             "The target file might not have been extracted yet or is outside the extraction root.")
                return None
        else:
            logger.info(f"Skipping os.link for hardlink {file_to_write_path} as source and target are identical and file exists.")

    elif member.type == MemberType.FILE:
        if stream is None:
            # This case should ideally not happen for a file member if iter_members_with_io is correct.
            # However, to be safe, create an empty file.
            logger.warning(f"No stream provided for file member {member.filename}, creating empty file.")
            with open(file_to_write_path, "wb") as dst:
                pass # Creates an empty file
        else:
            with open(file_to_write_path, "wb") as dst:
                shutil.copyfileobj(stream, dst)
    else: # MemberType.OTHER or unhandled
        logger.warning(f"Unsupported member type {member.type} for {member.filename}, skipping.")
        return None


    return file_to_write_path


class ArchiveReader(abc.ABC):
    """Abstract base class for archive streams."""

    def __init__(
        self,
        format: ArchiveFormat,
        archive_path: str | bytes | os.PathLike,
    ):
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
        self.config: ArchiveyConfig = get_default_config()
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
    def iter_members_with_io(
        self,
        filter: Callable[[ArchiveMember], bool] | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        """Iterate over all members in the archive.

        Args:
            filter: A filter function to apply to each member. If specified, only
            members for which the filter returns True will be yielded.
            The filter may be called for all members either before or during the
            iteration, so don't rely on any specific behavior.
            pwd: Password to use for decryption, if needed and different from the one
            used when opening the archive. May not be supported by all archive formats.

        Returns:
            A (ArchiveMember, BinaryIO) iterator over the members. Each stream should
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

    @abc.abstractmethod
    def has_random_access(self):
        """Check if opening members is possible (i.e. not streaming-only access)."""
        pass

    def extractall(self, path: str | os.PathLike | None = None, members: Union[List[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None] = None, pwd: bytes | str | None = None, *, filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None, preserve_links: bool = True) -> dict[str, str]:
        written_paths: dict[str, str] = {}

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        current_filter_for_iterator: Callable[[ArchiveMember], bool] | None
        if callable(members) and not isinstance(members, list):
            current_filter_for_iterator = members
        elif isinstance(members, list):
            if not members:  # Checks if the list is empty
                return {}
            selected_filenames = {
                member.filename if isinstance(member, ArchiveMember) else member
                for member in members
            }

            def _list_based_filter(member_obj: ArchiveMember) -> bool:
                return member_obj.filename in selected_filenames
            current_filter_for_iterator = _list_based_filter
        else:  # members is None
            current_filter_for_iterator = None

        written_members_metadata = []
        for member, stream in self.iter_members_with_io(filter=current_filter_for_iterator, pwd=pwd):
            member_to_extract = member
            if filter:  # New tarfile-style filter
                result = filter(member)  # Call the tarfile-style filter
                if result is None:
                    if stream:
                        stream.close()
                    continue
                member_to_extract = result  # Use the potentially modified member

            logger.info(f"Writing member {member_to_extract.filename}")
            written_path = _write_member(path, member_to_extract, preserve_links, stream)
            if written_path is not None:
                written_paths[member_to_extract.filename] = written_path
                written_members_metadata.append(member_to_extract)

            if stream:
                stream.close()

        apply_members_metadata(written_members_metadata, path)
        return written_paths

    # Context manager support
    def __enter__(self) -> "ArchiveReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # Methods only available for random access readers

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """Get a list of all members in the archive. May need to read the archive to get the members."""
        pass

    @abc.abstractmethod
    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        """Open a member for reading.

        Args:
            member: The member to open
            pwd: Password to use for decryption, if needed and different from the one
            used when opening the archive.
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

        member_map = self._build_member_map()
        if member_or_filename not in member_map:
            raise ArchiveMemberNotFoundError(f"Member not found: {member_or_filename}")
        return member_map[member_or_filename]

    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | None = None,
        pwd: bytes | str | None = None,
        preserve_links: bool = True,
    ) -> str | None:
        # Try using open(). Assume that, if it's possible to open a member,
        # get_member() is also available.
        if self.has_random_access():
            member = self.get_member(member_or_filename)
            stream = self.open(member, pwd=pwd)
            return _write_member(path or os.getcwd(), member, preserve_links, stream)

        # Fall back to extractall().
        logger.warning(
            "extract() may be slow for streaming archives, use extractall instead if possible. ()"
        )
        d = self.extractall(
            path=path,
            members=[member_or_filename],
            pwd=pwd,
            preserve_links=preserve_links,
        )
        return list(d.values())[0] if len(d) else None


class BaseArchiveReaderRandomAccess(ArchiveReader):
    """Abstract base class for archive readers which support random member access."""

    def __init__(
        self,
        format: ArchiveFormat,
        archive_path: str | bytes | os.PathLike,
    ):
        super().__init__(format, archive_path)

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        return self.get_members()

    def has_random_access(self) -> bool:
        return True

    def iter_members_with_io(
        self,
        filter: Callable[[ArchiveMember], bool] | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        """Default implementation of iter_members for random access archives."""
        for member in self.get_members():
            if filter is None or filter(member):
                stream: LazyOpenIO | None = None
                try:
                    # TODO: some libraries support fast seeking for files with no
                    # compression, so we should use that if possible.
                    actual_open = functools.partial(self.open, pwd=pwd)
                    stream = LazyOpenIO(actual_open, member, seekable=False)
                    yield member, stream
                except (ArchiveError, OSError) as e:
                    logger.warning(
                        "Error opening member %s", member.filename, exc_info=True
                    )
                    # The caller should only get the exception if it actually tries
                    # to read from the stream.
                    yield member, ErrorIOStream(e)
                finally:
                    if stream is not None:
                        stream.close()

    def getinfo(self, name: str) -> ArchiveMember:
        for member in self.get_members():
            if member.filename == name:
                return member
        raise ArchiveMemberNotFoundError(f"Member not found: {name}")


class StreamingOnlyArchiveReaderWrapper(ArchiveReader):
    """Wrapper for archive readers that only support streaming access."""

    def __init__(self, reader: ArchiveReader):
        self.reader = reader
        self.format = reader.format
        self.archive_path = reader.archive_path
        self.config = reader.config

    def close(self) -> None:
        self.reader.close()

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        return self.reader.get_members_if_available()

    def iter_members_with_io(
        self,
        filter: Callable[[ArchiveMember], bool] | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        return self.reader.iter_members_with_io(filter=filter, pwd=pwd)

    def get_archive_info(self) -> ArchiveInfo:
        return self.reader.get_archive_info()

    def has_random_access(self) -> bool:
        return False

    def extractall(
        self,
        path: str | None = None,
        members: list[ArchiveMember | str] | None = None,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], bool] | None = None,
        preserve_links: bool = True,
    ) -> dict[str, str]:
        return self.reader.extractall(
            path,
            members,
            pwd=pwd,
            filter=filter,
            preserve_links=preserve_links,
        )

    # Unsupported methods for streaming-only readers

    def get_members(self) -> List[ArchiveMember]:
        raise ValueError(
            "Streaming-only archive reader does not support get_members()."
        )

    def open(
        self, member: ArchiveMember, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        raise ValueError("Streaming-only archive reader does not support open().")

    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | None = None,
        pwd: bytes | str | None = None,
        preserve_links: bool = True,
    ) -> str | None:
        raise ValueError("Streaming-only archive reader does not support extract().")
