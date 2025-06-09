import abc
import functools
import io
import logging
import os
import shutil
from dataclasses import replace
from typing import BinaryIO, Callable, Iterable, Iterator, List

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
    os.makedirs(os.path.dirname(file_to_write_path), exist_ok=True)

    if member.is_dir:
        os.makedirs(file_to_write_path, exist_ok=True)
    elif member.is_link:
        if not preserve_links:
            return None
        link_target = member.link_target
        if not link_target:
            raise ValueError(f"Link target is empty for {member.filename}")

        if os.path.lexists(file_to_write_path):
            os.unlink(file_to_write_path)

        if member.type == MemberType.SYMLINK:
            logger.info(
                f"Writing symlink src={link_target} to dst={file_to_write_path}"
            )
            os.symlink(link_target, file_to_write_path)
        else:
            # Hard link
            link_target_path = os.path.join(root_path, link_target)
            if os.path.exists(link_target_path):
                os.link(link_target_path, file_to_write_path)
            else:
                # Fallback to copying the data if target does not exist
                logger.info(
                    "Hardlink target missing, writing file contents instead"
                )
                if stream is None:
                    stream = io.BytesIO(b"")
                with open(file_to_write_path, "wb") as dst:
                    shutil.copyfileobj(stream, dst)
    elif member.is_file:
        if os.path.exists(file_to_write_path):
            os.unlink(file_to_write_path)
        if stream is None:
            stream = io.BytesIO(b"")
        with open(file_to_write_path, "wb") as dst:
            shutil.copyfileobj(stream, dst)

    return file_to_write_path


def create_member_filter(
    members: Iterable[ArchiveMember | str] | Callable[[ArchiveMember], bool] | None,
    filter: Callable[[ArchiveMember], bool] | None,
) -> Callable[[ArchiveMember], bool] | None:
    if callable(members):
        members_filter = members
        if filter is None:
            return members_filter
        return lambda m: members_filter(m) and filter(m)

    if members is None:
        return filter

    members_to_write = {
        member.filename if isinstance(member, ArchiveMember) else member
        for member in members
    }

    if filter is None:
        return lambda m: m.filename in members_to_write
    return lambda m: m.filename in members_to_write and filter(m)


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

    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: list[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
        preserve_links: bool = True,
    ) -> dict[str, str]:
        written_paths: dict[str, str] = {}

        for m in self.get_members():
            logger.info(
                f"Member {m.filename} is_file: {m.is_file}, is_dir: {m.is_dir}, is_link: {m.is_link}"
            )

        bool_filter = members if callable(members) else None
        member_list = None if callable(members) else members
        member_filter = create_member_filter(member_list, bool_filter)

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        written_members = []
        for member, stream in self.iter_members_with_io(filter=member_filter, pwd=pwd):
            if filter is not None:
                new_member = filter(member)
                if new_member is None:
                    if stream is not None:
                        stream.close()
                    continue
                member = new_member

            logger.info(f"Writing member {member.filename}")
            written_path = _write_member(path, member, preserve_links, stream)
            if written_path is not None:
                written_paths[member.filename] = written_path
                written_members.append(member)
            if stream is not None:
                stream.close()

        apply_members_metadata(written_members, path)
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

    def _extract_regular_files(
        self,
        path: str,
        members: List[ArchiveMember],
        pwd: bytes | str | None,
    ) -> dict[str, str]:
        """Extract only regular file ``members`` into ``path``.

        Subclasses may override this to use optimized extraction provided by
        the underlying library.  The default implementation simply opens each
        member and writes it using :func:`_write_member`.
        """

        written: dict[str, str] = {}
        for member in members:
            with self.open(member, pwd=pwd) as stream:
                file_member = member if member.is_file else replace(member, type=MemberType.FILE)
                written_path = _write_member(path, file_member, True, stream)
                if written_path is not None:
                    written[member.filename] = written_path
        return written

    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: list[ArchiveMember | str] | Callable[[ArchiveMember], bool] | None = None,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
        preserve_links: bool = True,
    ) -> dict[str, str]:
        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        bool_filter = members if callable(members) else None
        member_list = None if callable(members) else members
        member_filter = create_member_filter(member_list, bool_filter)

        selected_members = [
            m for m in self.get_members() if member_filter is None or member_filter(m)
        ]

        processed_members: List[ArchiveMember] = []
        for m in selected_members:
            if filter is not None:
                new_m = filter(m)
                if new_m is None:
                    continue
                m = new_m
            processed_members.append(m)

        dirs: List[ArchiveMember] = []
        files: List[ArchiveMember] = []
        links: List[ArchiveMember] = []
        others: List[ArchiveMember] = []
        for m in processed_members:
            if m.is_dir:
                dirs.append(m)
            elif m.is_file:
                files.append(m)
            elif m.is_link:
                links.append(m)
            else:
                others.append(m)

        written_paths: dict[str, str] = {}
        written_members: List[ArchiveMember] = []

        # Ensure directories exist first
        for d in dirs:
            wp = _write_member(path, d, preserve_links, None)
            if wp is not None:
                written_paths[d.filename] = wp
                written_members.append(d)

        # Extract regular file members
        written = self._extract_regular_files(path, files, pwd)
        written_paths.update(written)
        for m in files:
            if m.filename in written:
                written_members.append(m)

        # Handle links
        if preserve_links:
            for l in links:
                with self.open(l, pwd=pwd) as stream:
                    wp = _write_member(path, l, preserve_links, stream)
                    if wp is not None:
                        written_paths[l.filename] = wp
                        written_members.append(l)
        else:
            # When not preserving links, copy the linked file contents
            for l in links:
                with self.open(l, pwd=pwd) as stream:
                    file_m = replace(l, type=MemberType.FILE)
                    wp = _write_member(path, file_m, True, stream)
                    if wp is not None:
                        written_paths[l.filename] = wp
                        written_members.append(l)

        # Fallback for other member types
        for o in others:
            with self.open(o, pwd=pwd) as stream:
                wp = _write_member(path, o, preserve_links, stream)
                if wp is not None:
                    written_paths[o.filename] = wp
                    written_members.append(o)

        apply_members_metadata(processed_members, path)
        return written_paths


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
        members: list[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
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
