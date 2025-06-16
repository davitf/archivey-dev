import abc
import logging
import os
import posixpath
import threading
from collections import defaultdict
from typing import BinaryIO, Callable, Collection, Iterator, List, Union

from archivey.config import ArchiveyConfig, get_default_config
from archivey.exceptions import (
    ArchiveMemberCannotBeOpenedError,
    ArchiveMemberNotFoundError,
)
from archivey.extraction_helper import ExtractionHelper
from archivey.io_helpers import LazyOpenIO
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType
from archivey.unique_ids import UNIQUE_ID_GENERATOR

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
                internal_ids.add(member.member_id)
            else:
                filenames.add(member)

    return lambda m: m.filename in filenames or m.member_id in internal_ids


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
            if filtered is not None and filtered.member_id != member.member_id:
                raise ValueError(
                    f"Filter returned a member with a different internal ID: {member.filename} {member.member_id} -> {filtered.filename} {filtered.member_id}"
                )

            return filtered

    return _apply_filter


class ArchiveReader(abc.ABC):
    def __init__(self, archive_path: str | bytes | os.PathLike, format: ArchiveFormat):
        self.archive_path = (
            archive_path.decode("utf-8")
            if isinstance(archive_path, bytes)
            else str(archive_path)
        )
        self.format = format
        self.config: ArchiveyConfig = get_default_config()

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
        """Get a list of all members in the archive.

        Raises an error if the library or opening mode doesn't allow listing members.
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
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        pass

    @abc.abstractmethod
    def get_archive_info(self) -> ArchiveInfo:
        pass

    @abc.abstractmethod
    def has_random_access(self) -> bool:
        pass

    @abc.abstractmethod
    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
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
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
    ) -> dict[str, ArchiveMember]:
        pass

    # Context manager support
    def __enter__(self) -> "ArchiveReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class BaseArchiveReader(ArchiveReader):
    """Abstract base class for archive streams."""

    def __init__(
        self,
        format: ArchiveFormat,
        archive_path: str | bytes | os.PathLike,
        random_access_supported: bool,
        members_list_supported: bool,
    ):
        """Initialize the archive reader.

        Args:
            format: The format of the archive
            archive_path: The path to the archive file
        """
        super().__init__(archive_path, format)

        # self._member_id_to_member: dict[int, ArchiveMember] = {}
        self._members: list[ArchiveMember] = []
        self._filename_to_members: dict[str, list[ArchiveMember]] = defaultdict(list)
        self._normalized_path_to_last_member: dict[str, ArchiveMember] = {}
        self._all_members_registered: bool = False
        self._registration_lock: threading.Lock = threading.Lock()

        self._archive_id: int = UNIQUE_ID_GENERATOR.next_id()

        self._random_access_supported = random_access_supported
        self._early_members_list_supported = members_list_supported

        self._iterator_for_registration: Iterator[ArchiveMember] | None = None

    def _resolve_link_target(
        self, member: ArchiveMember, visited_members: set[int] = set()
    ) -> None:
        if member.link_target is None:
            return

        # Run the search even if we had previously resolved the link target, as it
        # may have been overwritten by a later member with the same filename.

        if member.type == MemberType.HARDLINK:
            # Look for the last member with the same filename and a lower member_id.
            link_target = member.link_target
            if link_target is None:
                logger.warning(f"Hardlink target is None for {member.filename}")
                return

            members = self._filename_to_members.get(link_target, [])
            target_member = max(
                (m for m in members if m.member_id < member.member_id),
                key=lambda m: m.member_id,
                default=None,
            )
            if target_member is None:
                logger.warning(
                    f"Hardlink target {link_target} not found for {member.filename}"
                )
                return

            # If the target is another hardlink, recursively resolve it.
            # As we always look for members with a lower member_id, this will not
            # loop forever.
            if target_member.type == MemberType.HARDLINK:
                self._resolve_link_target(target_member)
                # This is guaranteed to point to the final non-hardlink in the chain.
                target_member = target_member.link_target_member
                if target_member is None:
                    logger.warning(
                        f"Hardlink target {link_target} not found for {member.filename} (when following hardlink)"
                    )
                    return

            member.link_target_member = target_member
            member.link_target_type = target_member.type

        elif member.type == MemberType.SYMLINK:
            normalized_link_target = posixpath.normpath(
                posixpath.join(posixpath.dirname(member.filename), member.link_target)
            )
            target_member = self._normalized_path_to_last_member.get(
                normalized_link_target
            )
            if target_member is None:
                logger.warning(
                    f"Symlink target {normalized_link_target} not found for {member.filename}"
                )
                return

            if target_member.is_link:
                if target_member.member_id in visited_members:
                    logger.error(
                        f"Symlink loop detected: {member.filename} -> {target_member.filename}"
                    )
                    return
                self._resolve_link_target(
                    target_member, visited_members | {member.member_id}
                )
                if target_member.link_target_member is None:
                    logger.warning(
                        f"Link target {target_member.filename} {target_member.member_id} does not have a valid target (when resolving {member.filename} {member.member_id})"
                    )
                    return

                target_member = target_member.link_target_member

            member.link_target_member = target_member
            member.link_target_type = target_member.type

    def _register_member(self, member: ArchiveMember) -> None:
        assert self._registration_lock.locked(), "Not in registration lock"

        assert member._member_id is None, (
            f"Member {member.filename} already registered with member_id {member.member_id}"
        )

        member._archive_id = self._archive_id
        member._member_id = len(self._members)
        self._members.append(member)

        logger.info(f"Registering member {member.filename} ({member.member_id})")

        members_with_filename = self._filename_to_members[member.filename]
        if member not in members_with_filename:
            members_with_filename.append(member)
            members_with_filename.sort(key=lambda m: m.member_id)

        normalized_path = posixpath.normpath(member.filename)
        if (
            normalized_path not in self._normalized_path_to_last_member
            or self._normalized_path_to_last_member[normalized_path].member_id
            < member.member_id
        ):
            self._normalized_path_to_last_member[normalized_path] = member

        self._resolve_link_target(member)

    @abc.abstractmethod
    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        pass

    def _register_next_member(self) -> None:
        with self._registration_lock:
            if self._all_members_registered:
                return

            if self._iterator_for_registration is None:
                self._iterator_for_registration = self.iter_members_for_registration()

            next_member = next(self._iterator_for_registration, None)
            if next_member is None:
                self._all_members_registered = True
                return

            self._register_member(next_member)
            return

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """Get a list of all members in the archive, or None if not available. May not be available for stream archives."""
        if self._all_members_registered:
            return self._members

        if not self._early_members_list_supported:
            return None

        return self.get_members()

    def iter_members(self) -> Iterator[ArchiveMember]:
        i: int = 0
        # While the _iter_members_for_registration() iterator is still not exhausted,
        # yield all the members that have been registered so far, and register the next
        # member if possible. Keep in mind that multiple iterators may be active at the
        # same time, and they all need to return all members in the same order..
        while not self._all_members_registered:
            while i < len(self._members):
                yield self._members[i]
                i += 1

            # This iterator already provided all registered members, so try to advance
            # the _iter_members_for_registration() iterator to get the next member.
            self._register_next_member()

        # The flag that all members have been registered has been set, but possibly
        # from a different iterator. Yield any remaining members.
        while i < len(self._members):
            yield self._members[i]
            i += 1

    def open_for_iteration(self, member, pwd: bytes | str | None = None) -> BinaryIO:
        return self.open(member, pwd=pwd)

    def iter_members_with_io(
        self,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
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
        # This is a default implementation for random-access readers which support
        # open().
        # assert self._random_access_supported, (
        #     "Non-random access readers must override iter_members_with_io()"
        # )

        filter_func = _build_iterator_filter(members, filter)

        logger.info(f"iter_members_with_io: {self.iter_members()}")
        for member in self.iter_members():
            logger.info(f"iter_members_with_io member: {member}")
            filtered = filter_func(member)
            if filtered is None:
                logger.info(f"skipping {member.filename}")
                continue

            try:
                # TODO: some libraries support fast seeking for files (either all,
                # or only non-compressed ones), so we should set seekable=True
                # if possible.
                stream = (
                    LazyOpenIO(self.open_for_iteration, member, pwd=pwd, seekable=False)
                    if member.is_file
                    else None
                )
                yield member, stream

            finally:
                if stream is not None:
                    stream.close()

    def has_random_access(self) -> bool:
        """Check if opening members is possible (i.e. not streaming-only access)."""
        return self._random_access_supported

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

    def _extractall_with_random_access(
        self,
        path: str,
        filter_func: Callable[[ArchiveMember], Union[ArchiveMember, None]],
        pwd: bytes | str | None,
        extraction_helper: ExtractionHelper,
    ):
        # For readers that support random access, register all members first to get
        # a complete list of members that need to be extracted, so that the
        # subclass can extract all files at once (which may be faster).
        for member in self.get_members():
            filtered_member = filter_func(member)
            if filtered_member is None:
                continue

            extraction_helper.extract_member(member, None)

        # Extract regular files
        self._extract_pending_files(path, extraction_helper, pwd=pwd)

    def _extractall_with_streaming_mode(
        self,
        path: str,
        filter_func: Callable[[ArchiveMember], Union[ArchiveMember, None]],
        pwd: bytes | str | None,
        extraction_helper: ExtractionHelper,
    ):
        for member, stream in self.iter_members_with_io(filter=filter_func, pwd=pwd):
            logger.debug(f"Writing member {member.filename}")
            extraction_helper.extract_member(member, stream)
            if stream:
                stream.close()

    def extractall(
        self,
        path: str | os.PathLike | None = None,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
    ) -> dict[str, ArchiveMember]:
        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        filter_func = _build_iterator_filter(members, filter)

        extraction_helper = ExtractionHelper(
            self.archive_path,
            path,
            self.config.overwrite_mode,
            can_process_pending_extractions=self.has_random_access(),
        )

        if self._random_access_supported:
            self._extractall_with_random_access(
                path, filter_func, pwd, extraction_helper
            )
        else:
            self._extractall_with_streaming_mode(
                path, filter_func, pwd, extraction_helper
            )

        extraction_helper.apply_metadata()

        return extraction_helper.extracted_members_by_path

    # @abc.abstractmethod
    # def _read_members_list(self) -> bool:
    #     """Read the members list from the archive.

    #     This method is called by get_members() if the members list has not yet been
    #     read. If available for the format, it should read the members list from the
    #     archive without reading the whole archive, register the members and return True.
    #     If not available, return False.

    #     Returns:
    #         True if the members list was read successfully, False otherwise.
    #     """
    #     pass

    def get_members(self) -> List[ArchiveMember]:
        if not self._early_members_list_supported:
            raise ValueError("Archive reader does not support get_members().")

        # Default implementation for random-access readers.
        # assert self._random_access_supported, (
        #     "Non-random access readers must override get_members()"
        # )
        while not self._all_members_registered:
            self._register_next_member()

        return self._members

    def _resolve_member_to_open(
        self, member_or_filename: ArchiveMember | str
    ) -> tuple[ArchiveMember, str]:
        filename = (
            member_or_filename.filename
            if isinstance(member_or_filename, ArchiveMember)
            else member_or_filename
        )
        final_member = member = self.get_member(member_or_filename)

        if member.is_link:
            logger.info(
                f"Resolving link target for {member.filename} {member.type} {member.member_id}"
            )

            # If the user is opening a link, open the target member instead.
            self._resolve_link_target(member)
            logger.info(
                f"Resolved link target for {member.filename} {member.type} {member.member_id}: {member.link_target}"
            )
            if member.link_target_member is None:
                raise ArchiveMemberCannotBeOpenedError(
                    f"Link target not found: {member.filename} (when opening {filename})"
                )
            logger.info(
                f"  target_member={member.link_target_member.member_id} {member.link_target_member.filename} {member.link_target_member.type}"
            )
            final_member = member.link_target_member

        logger.info(
            f"Final member: orig {filename} {member.member_id} {final_member.filename} {final_member.type}"
        )
        if not final_member.is_file:
            if final_member is not member:
                raise ArchiveMemberCannotBeOpenedError(
                    f"Cannot open {final_member.type} {final_member.filename} (redirected from {filename})"
                )

            raise ArchiveMemberCannotBeOpenedError(
                f"Cannot open {final_member.type} {filename}"
            )

        return final_member, filename

    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
        if isinstance(member_or_filename, ArchiveMember):
            if member_or_filename.archive_id != self._archive_id:
                raise ValueError(
                    f"Member {member_or_filename.filename} is not from this archive"
                )
            return member_or_filename

        if not self._all_members_registered:
            self.get_members()

        if member_or_filename not in self._filename_to_members:
            raise ArchiveMemberNotFoundError(f"Member not found: {member_or_filename}")
        return self._filename_to_members[member_or_filename][-1]

    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | os.PathLike | None = None,
        pwd: bytes | str | None = None,
    ) -> str | None:
        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        if self._random_access_supported:
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
        )
        return list(d.keys())[0] if len(d) else None


class StreamingOnlyArchiveReaderWrapper(ArchiveReader):
    """Wrapper for archive readers that only support streaming access."""

    def __init__(self, reader: ArchiveReader):
        super().__init__(reader.archive_path, reader.format)
        self.reader = reader

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

    def extractall(self, *args, **kwargs) -> dict[str, ArchiveMember]:
        return self.reader.extractall(*args, **kwargs)

    def get_member(self, member_or_filename: ArchiveMember | str) -> ArchiveMember:
        return self.reader.get_member(member_or_filename)

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
