import abc
import functools
import logging
import os
from typing import BinaryIO, Callable, Collection, Iterator, List, Union

from archivey.config import ArchiveyConfig, get_default_config
from archivey.exceptions import ArchiveError, ArchiveMemberNotFoundError
from archivey.extraction_helper import ExtractionHelper
from archivey.io_helpers import ErrorIOStream, LazyOpenIO
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

logger = logging.getLogger(__name__)


def _build_member_included_func(
    members: Collection[Union[ArchiveMember, str]]
    | Callable[[ArchiveMember], bool]
    | None,
) -> Callable[[ArchiveMember], bool]:
    if members is None:
        return lambda _: True
    elif isinstance(members, Callable):
        return members

    filenames: set[str] = set()
    internal_ids: set[int] = set()

    if members is not None and not isinstance(members, Callable):
        for member in members:
            if isinstance(member, ArchiveMember):
                internal_ids.add(member.internal_id)
            else:
                filenames.add(member)

    return lambda m: m.filename in filenames or m.internal_id in internal_ids


def _build_iterator_filter(
    members: Collection[Union[ArchiveMember, str]]
    | Callable[[ArchiveMember], bool]
    | None,
    filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None,
) -> Callable[[ArchiveMember], ArchiveMember | None]:
    """Build a filter function for the iterator.

    Args:
        members: A collection of members or a callable to filter members.
        filter: A filter function to apply to each member. If specified, only
            members for which the filter returns True will be yielded.
            The filter may be called for all members either before or during the
            iteration, so don't rely on any specific behavior.
    """
    member_included = _build_member_included_func(members)

    def _apply_filter(member: ArchiveMember) -> ArchiveMember | None:
        if not member_included(member):
            return None

        if filter is None:
            return member
        else:
            filtered = filter(member)
            # Check the filtered still refers to the same member
            if filtered is not None and filtered.internal_id != member.internal_id:
                raise ValueError(
                    f"Filter returned a member with a different internal ID: {member.filename} {member.internal_id} -> {filtered.filename} {filtered.internal_id}"
                )

            return filtered

    return _apply_filter


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
        self._member_id_to_member: dict[int, ArchiveMember] = {}
        self._filename_to_member: dict[str, ArchiveMember] = {}
        self._members_retrieved: bool = False

    def register_member(self, member: ArchiveMember) -> None:
        logger.info(f"Registering member {member.filename} ({member.internal_id})")
        self._filename_to_member[member.filename] = member
        self._member_id_to_member[member.internal_id] = member

        if member.type == MemberType.HARDLINK:
            # Store a reference to the target member. As the original member may be
            # overwritten later if there's another member with the same filename,
            # we need to keep a reference to the original member.
            link_target = member.link_target

            if link_target is not None:
                member.link_target_member = self._filename_to_member.get(link_target)

            if member.link_target_member is None:
                logger.warning(
                    f"Hardlink target {link_target} not found for {member.filename}"
                )
            elif member.link_target_member.link_target_member is not None:
                # The target member is a hardlink to yet another member.
                # We need to keep the reference to the original member.
                # As the previous member was already resolved in this same manner,
                # it's guaranteed to point to the non-link member, so we don't need
                # to follow the chain recursively.
                member.link_target_member = member.link_target_member.link_target_member

    def set_all_members_retrieved(self) -> None:
        self._members_retrieved = True

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
        members: Union[
            List[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None
        ] = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
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
    def has_random_access(self) -> bool:
        """Check if opening members is possible (i.e. not streaming-only access)."""
        pass

    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: Union[
            List[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None
        ] = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
    ) -> dict[str, str]:
        written_paths: dict[str, str] = {}

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        extraction_helper = ExtractionHelper(
            self.archive_path,
            path,
            self.config.overwrite_mode,
            can_process_pending_extractions=self.has_random_access(),
        )

        for member, stream in self.iter_members_with_io(
            members=members, pwd=pwd, filter=filter
        ):
            logger.debug(f"Writing member {member.filename}")
            extraction_helper.extract_member(member, stream)
            if stream:
                stream.close()

        if extraction_helper.get_pending_extractions():
            for member in extraction_helper.get_pending_extractions():
                stream = self.open(member, pwd=pwd) if member.is_file else None
                extraction_helper.extract_member(member, stream)
                if stream:
                    stream.close()

        extraction_helper.apply_metadata()

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

    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
        if isinstance(member_or_filename, ArchiveMember):
            # TODO: check that the member is from this archive
            return member_or_filename

        if not self._members_retrieved:
            self.get_members()

        if member_or_filename not in self._filename_to_member:
            raise ArchiveMemberNotFoundError(f"Member not found: {member_or_filename}")
        return self._filename_to_member[member_or_filename]

    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | None = None,
        pwd: bytes | str | None = None,
        preserve_links: bool = True,
    ) -> str | None:
        if path is None:
            path = os.getcwd()

        if self.has_random_access():
            member = self.get_member(member_or_filename)
            extraction_helper = ExtractionHelper(
                self.archive_path,
                path,
                self.config.overwrite_mode,
                can_process_pending_extractions=False,
            )

            stream = self.open(member, pwd=pwd) if member.is_file else None

            extraction_helper.extract_member(member, stream)
            if stream:
                stream.close()

            extraction_helper.apply_metadata()

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
        members: Union[
            Collection[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None
        ] = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        """Default implementation of iter_members for random access archives."""

        filter_func = _build_iterator_filter(members, filter)

        for member in self.get_members():
            filtered = filter_func(member)
            if filtered is None:
                continue

            stream: LazyOpenIO | None = None
            try:
                # TODO: some libraries support fast seeking for files (either all,
                # or only non-compressed ones), so we should set seekable=True
                # if possible.
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

    def _extract_pending_files(
        self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None
    ):
        """Extract pending files from the archive. Intended to be overridden by subclasses.

        For some libraries, extraction using extractall() or similar is faster than
        opening each member individually, so subclasses should override this method
        if it's beneficial.

        All directories needed are guaranteed to exist. The pending files are either
        regular files, or links if the archive does not store link targets in the header.
        Metadata attributes for the extracted files will be applied afterwards.
        """
        members_to_extract = extraction_helper.get_pending_extractions()
        for member in members_to_extract:
            stream = self.open(member, pwd=pwd) if member.is_file else None
            extraction_helper.extract_member(member, stream)
            if stream:
                stream.close()

    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: Union[
            List[Union[ArchiveMember, str]], Callable[[ArchiveMember], bool], None
        ] = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
    ) -> dict[str, str]:
        written_paths: dict[str, str] = {}

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        filter_func = _build_iterator_filter(members, filter)

        extraction_helper = ExtractionHelper(
            self.archive_path,
            path,
            self.config.overwrite_mode,
            can_process_pending_extractions=True,
        )

        for member in self.get_members():
            filtered_member = filter_func(member)
            if filtered_member is None:
                continue

            extraction_helper.extract_member(member, None)

        # Extract regular files
        self._extract_pending_files(path, extraction_helper, pwd=pwd)

        extraction_helper.apply_metadata()

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
        self, *args, **kwargs
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        return self.reader.iter_members_with_io(*args, **kwargs)

    def get_archive_info(self) -> ArchiveInfo:
        return self.reader.get_archive_info()

    def has_random_access(self) -> bool:
        return False

    def extractall(self, *args, **kwargs) -> dict[str, str]:
        return self.reader.extractall(*args, **kwargs)

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
