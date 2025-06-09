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


def _ensure_parent_dir_and_clean_path(
    target_full_path: str,
    member_type_to_write: MemberType,
    hardlink_source_path_for_skip_check: str | None = None
) -> None:
    os.makedirs(os.path.dirname(target_full_path), exist_ok=True)

    perform_unlink = True
    if member_type_to_write == MemberType.HARDLINK and \
       hardlink_source_path_for_skip_check == target_full_path and \
       os.path.lexists(target_full_path) and \
       not os.path.isdir(target_full_path):
        logger.info(f"Path {target_full_path} is a hardlink source/target match. Skipping removal.")
        perform_unlink = False

    if perform_unlink and os.path.lexists(target_full_path):
        if os.path.isdir(target_full_path) and not os.path.islink(target_full_path):
            if member_type_to_write != MemberType.DIR: # Don't remove if we are writing a dir to a dir
                logger.info(f"Removing existing directory at {target_full_path} to replace with {member_type_to_write.value}")
                shutil.rmtree(target_full_path)
        else: # It's a file or a symlink
            logger.info(f"Removing existing file/symlink at {target_full_path} to replace with {member_type_to_write.value}")
            os.unlink(target_full_path)

def _create_directory_internal(target_full_path: str) -> str | None:
    try:
        os.makedirs(target_full_path, exist_ok=True)
        logger.info(f"Successfully created directory: {target_full_path}")
        return target_full_path
    except OSError as e:
        logger.error(f"Failed to create directory {target_full_path}: {e}", exc_info=True)
        return None

def _create_symlink_internal(member_filename: str, link_target_attr: str | None, target_full_path: str) -> str | None:
    if not link_target_attr:
        logger.warning(f"Symlink target is empty for {member_filename}, skipping.")
        return None
    try:
        os.symlink(link_target_attr, target_full_path)
        logger.info(f"Successfully created symlink: {target_full_path} -> {link_target_attr}")
        return target_full_path
    except OSError as e:
        logger.error(f"Failed to create symlink {target_full_path} -> {link_target_attr}: {e}", exc_info=True)
        return None

def _create_hardlink_internal(member_filename: str, link_target_attr: str | None, target_full_path: str, root_path: str) -> str | None:
    if not link_target_attr:
        logger.warning(f"Hardlink target is empty for {member_filename}, skipping.")
        return None

    hardlink_source_path = os.path.join(root_path, link_target_attr)

    # This specific check for identical source/target AND target existence should be handled before path cleaning.
    # However, _ensure_parent_dir_and_clean_path now has logic to avoid unlinking in this case.
    # So, we proceed to attempt the link. If _ensure_parent_dir_and_clean_path correctly skipped unlinking,
    # and the file is already there and is the target, os.link might fail if it's truly identical (errno EEXIST often for hardlinks),
    # or it might succeed if it's just another name for the same inode.
    # If the file exists because it's the source, we effectively want to "skip" linking.

    if hardlink_source_path == target_full_path and os.path.exists(target_full_path):
        logger.info(f"Skipping os.link for hardlink {target_full_path} as source and target are identical and file exists.")
        return target_full_path # File is already in place

    try:
        os.link(hardlink_source_path, target_full_path)
        logger.info(f"Successfully created hardlink: {target_full_path} -> {hardlink_source_path}")
        return target_full_path
    except OSError as e:
        logger.error(f"Failed to create hardlink {target_full_path} -> {hardlink_source_path}: {e}. "
                     "The target file might not have been extracted yet or is outside the extraction root.", exc_info=True)
        return None

def _write_file_internal(member_filename: str, target_full_path: str, stream: BinaryIO | None) -> str | None:
    try:
        if stream is None:
            logger.warning(f"No stream provided for file member {member_filename}, creating empty file.")
            with open(target_full_path, "wb") as dst:
                pass # Creates an empty file
        else:
            with open(target_full_path, "wb") as dst:
                shutil.copyfileobj(stream, dst)
        logger.info(f"Successfully wrote file: {target_full_path}")
        return target_full_path
    except OSError as e:
        logger.error(f"Failed to write file {target_full_path}: {e}", exc_info=True)
        # Attempt to clean up partially written file
        if os.path.exists(target_full_path):
            try:
                os.remove(target_full_path)
                logger.info(f"Removed partially written file: {target_full_path}")
            except OSError as roe:
                logger.error(f"Failed to remove partially written file {target_full_path}: {roe}", exc_info=True)
        return None

def _write_member(
    root_path: str,
    member: ArchiveMember,
    preserve_links: bool,
    stream: BinaryIO | None,
) -> str | None:
    file_to_write_path = os.path.join(root_path, member.filename)

    hardlink_source_path_for_cleaner: str | None = None
    if member.type == MemberType.HARDLINK and member.link_target:
        hardlink_source_path_for_cleaner = os.path.join(root_path, member.link_target)

    _ensure_parent_dir_and_clean_path(file_to_write_path, member.type, hardlink_source_path_for_cleaner)

    if member.type == MemberType.DIR:
        return _create_directory_internal(file_to_write_path)
    elif member.type == MemberType.SYMLINK:
        if not preserve_links:
            return None
        return _create_symlink_internal(member.filename, member.link_target, file_to_write_path)
    elif member.type == MemberType.HARDLINK:
        if not preserve_links:
            return None
        return _create_hardlink_internal(member.filename, member.link_target, file_to_write_path, root_path)
    elif member.type == MemberType.FILE:
        return _write_file_internal(member.filename, file_to_write_path, stream)
    else:
        logger.warning(f"Unsupported member type {member.type} for {member.filename} in _write_member, skipping.")
        return None

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

    def extractall(self, path: str | os.PathLike | None = None, members: Union[List[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None] = None, pwd: bytes | str | None = None, *, filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None, preserve_links: bool = True) -> dict[str, str]:
        # 1. Path Setup
        if path is None:
            current_path = os.getcwd()
        else:
            current_path = str(path)
        written_paths: dict[str, str] = {}

        # 2. Get and Filter Members
        all_members_objects = self.get_members()

        candidate_members: List[ArchiveMember]
        if isinstance(members, list):
            if not members:
                return {}
            selected_filenames = {
                member.filename if isinstance(member, ArchiveMember) else member
                for member in members
            }
            candidate_members = [m for m in all_members_objects if m.filename in selected_filenames]
        elif callable(members): # boolean filter function
            candidate_members = [m for m in all_members_objects if members(m)]
        else: # members is None
            candidate_members = all_members_objects

        final_members_to_extract: List[ArchiveMember] = []
        if filter: # tarfile-style filter
            for m in candidate_members:
                filtered_m = filter(m)
                if filtered_m is not None:
                    final_members_to_extract.append(filtered_m)
        else:
            final_members_to_extract = candidate_members

        if not final_members_to_extract:
            return {}

        # 3. Categorize Members
        directories: List[ArchiveMember] = []
        symlinks: List[ArchiveMember] = []
        hardlinks: List[ArchiveMember] = []
        regular_files: List[ArchiveMember] = []

        for member in final_members_to_extract:
            if member.type == MemberType.DIR:
                directories.append(member)
            elif member.type == MemberType.SYMLINK:
                symlinks.append(member)
            elif member.type == MemberType.HARDLINK:
                hardlinks.append(member)
            elif member.type == MemberType.FILE:
                regular_files.append(member)
            else:
                logger.warning(f"Unsupported member type {member.type} for {member.filename} during categorized extraction, skipping.")


        # 4. Create Directories
        # Sort by path depth to ensure parent dirs are created first
        directories.sort(key=lambda d: len(d.filename.split(os.sep)))
        for d_member in directories:
            dir_path = os.path.join(current_path, d_member.filename)
            try:
                os.makedirs(dir_path, exist_ok=True)
                written_paths[d_member.filename] = dir_path
            except OSError as e:
                logger.error(f"Failed to create directory {dir_path}: {e}")

        # 5. Extract Regular Files
        self._extract_files_batch(regular_files, current_path, pwd, written_paths)

        # 6. Create Links (Placeholders)
        if preserve_links:
            for s_member in symlinks:
                link_path = os.path.join(current_path, s_member.filename)
                _ensure_parent_dir_and_clean_path(link_path, MemberType.SYMLINK, None)
                result_path = _create_symlink_internal(s_member.filename, s_member.link_target, link_path)
                if result_path:
                    written_paths[s_member.filename] = result_path

            for h_member in hardlinks:
                link_path = os.path.join(current_path, h_member.filename)
                hardlink_source_on_fs = None
                if h_member.link_target:
                    hardlink_source_on_fs = os.path.join(current_path, h_member.link_target)

                _ensure_parent_dir_and_clean_path(link_path, MemberType.HARDLINK,
                                                  hardlink_source_path_for_skip_check=hardlink_source_on_fs)

                result_path = _create_hardlink_internal(h_member.filename, h_member.link_target, link_path, current_path)
                if result_path:
                    written_paths[h_member.filename] = result_path

        # 7. & 8. Apply Metadata
        successfully_written_members = [
            m for m in final_members_to_extract if m.filename in written_paths
        ]
        apply_members_metadata(successfully_written_members, current_path)

        # 9. Return written_paths
        return written_paths

    def _extract_files_batch(
        self,
        files_to_extract: List[ArchiveMember],
        target_path: str, # This is the root extraction path
        pwd: bytes | str | None,
        written_paths: dict[str, str] # To be updated with paths of successfully extracted files
    ) -> None:
        for member in files_to_extract:
            if member.type != MemberType.FILE:
                logger.warning(f"Skipping non-file member {member.filename} in _extract_files_batch")
                continue

            file_to_write_path = os.path.join(target_path, member.filename)

            # Prepare path by ensuring parent directory exists and cleaning any pre-existing file/dir
            # For files, hardlink_source_path_for_skip_check is None as it's not a hardlink.
            _ensure_parent_dir_and_clean_path(file_to_write_path, MemberType.FILE, None)

            try:
                # Open stream for the member
                with self.open(member, pwd=pwd) as stream:
                    # Write the file using the internal helper
                    if _write_file_internal(member.filename, file_to_write_path, stream):
                        written_paths[member.filename] = file_to_write_path
                    # _write_file_internal handles its own logging for success/failure & partial file cleanup
            except Exception as e:
                # Log error for opening stream or if _write_file_internal itself raises an unexpected error
                # _write_file_internal is expected to catch OS PError during write and return None,
                # but self.open() could raise, or other unexpected issues.
                logger.error(f"Failed to process or open stream for file {member.filename}: {e}", exc_info=True)
                # _write_file_internal attempts cleanup if it started writing.
                # If self.open failed, there's no file to clean from this operation.


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
