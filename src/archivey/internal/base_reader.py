"""Defines the abstract base classes and common functionality for archive readers."""

import abc
import logging
import os
import posixpath
import threading
from collections import defaultdict
from typing import BinaryIO, Callable, Collection, Iterator, List, Union, cast
from uuid import uuid4

from archivey.api.config import ArchiveyConfig, ExtractionFilter, get_default_config
from archivey.exceptions import (
    ArchiveMemberCannotBeOpenedError,
    ArchiveMemberNotFoundError,
)
from .extraction_helper import ExtractionHelper
from archivey.filters import DEFAULT_FILTERS # Will be reviewed later
from .io_helpers import LazyOpenIO
from archivey.api.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    ArchiveReader,
    ExtractFilterFunc,
    IteratorFilterFunc,
    MemberType,
)

logger = logging.getLogger(__name__)


def _build_member_included_func(
    members: Collection[Union[ArchiveMember, str]]
    | Callable[[ArchiveMember], bool]
    | None,
) -> Callable[[ArchiveMember], bool]:
    if members is None:
        return lambda _: True
    elif callable(members):
        return members

    filenames: set[str] = set()
    internal_ids: set[int] = set()

    for member in members:
        if isinstance(member, ArchiveMember):
            internal_ids.add(member.member_id)
        else:
            filenames.add(member)

    return lambda m: m.filename in filenames or m.member_id in internal_ids


def _build_filter(
    members: Collection[Union[ArchiveMember, str]]
    | Callable[[ArchiveMember], bool]
    | None,
    filter: ExtractFilterFunc | IteratorFilterFunc | ExtractionFilter,
    dest_path: str | None = None,
) -> IteratorFilterFunc:
    """Build a filter function for the iterator.

    Args:
        members: A collection of members or a callable to filter members.
        filter: A filter function to apply to each member. If specified, only
            members for which the filter returns True will be yielded.
            The filter may be called for all members either before or during the
            iteration, so don't rely on any specific behavior.
    """
    member_included = _build_member_included_func(members)
    filter_func = filter if callable(filter) else DEFAULT_FILTERS[filter]

    def _apply_filter(member: ArchiveMember) -> ArchiveMember | None:
        if not member_included(member):
            return None

        if dest_path is None:
            filtered = cast(IteratorFilterFunc, filter_func)(member)
        else:
            filtered = cast(ExtractFilterFunc, filter_func)(member, dest_path)

        # Check the filtered still refers to the same member
        if filtered is not None and filtered.member_id != member.member_id:
            raise ValueError(
                f"Filter returned a member with a different internal ID: {member.filename} {member.member_id} -> {filtered.filename} {filtered.member_id}"
            )

        return filtered

    return _apply_filter


class BaseArchiveReader(ArchiveReader):
    """
    A base implementation of ArchiveReader providing common logic.

    This class handles member registration, link resolution, and default
    implementations for some methods based on others. Developers creating
    new readers will typically inherit from this class and implement the
    abstract methods like `iter_members_for_registration`, `open`, and `close`.
    """

    def __init__(
        self,
        format: ArchiveFormat,
        archive_path: BinaryIO | str | bytes | os.PathLike,
        streaming_only: bool,
        members_list_supported: bool,
        pwd: bytes | str | None = None,
    ):
        """
        Initialize the BaseArchiveReader.

        Args:
            format: The ArchiveFormat enum value for this archive type.
            archive_path: Path to the archive file.
            streaming_only: bool indicating if the archive is opened in streaming
                mode. If True, `get_members()`, `open()` and `extract()` will raise
                ValueError, and the object will support a single iteration over the
                archive (with `iter_members_with_io` or `extractall`).
            members_list_supported: bool indicating if the archive can provide a
                full list before iterating through the archive (e.g., from a central
                directory). If True, `get_members_if_available()` will exhaust
                `iter_members_for_registration()` archive, and `get_members()` will raise ValueError. If False,
                `get_members()` might have to iterate through the archive.
            pwd: Optional default password for the archive.
        """
        super().__init__(archive_path, format)
        self.config: ArchiveyConfig = get_default_config()

        if pwd is not None and isinstance(pwd, str):
            self._archive_password: bytes | None = pwd.encode("utf-8")
        else:
            self._archive_password: bytes | None = pwd

        # self._member_id_to_member: dict[int, ArchiveMember] = {}
        self._members: list[ArchiveMember] = []
        self._filename_to_members: dict[str, list[ArchiveMember]] = defaultdict(list)
        self._normalized_path_to_last_member: dict[str, ArchiveMember] = {}
        self._all_members_registered: bool = False
        self._registration_lock: threading.Lock = threading.Lock()

        self._archive_id: str = uuid4().hex

        self._streaming_only = streaming_only
        self._early_members_list_supported = members_list_supported

        self._iterator_for_registration: Iterator[ArchiveMember] | None = None

        self._streaming_iteration_started: bool = False
        self._closed: bool = False

    def get_archive_password(self) -> bytes | None:
        """Return the default password for the archive, if one was provided."""
        return self._archive_password

    def resolve_link(self, member: ArchiveMember) -> ArchiveMember | None:
        if not member.is_link:
            return member  # Not a link or no target path specified

        # Ensure all members are registered so lookups are complete, for files that
        # support it. If the file doesn't support it, the lookup will be incomplete.
        self.get_members_if_available()

        return self._resolve_link_recursive(member, set())

    def _resolve_link_recursive(
        self, member: ArchiveMember, visited_ids: set[int]
    ) -> ArchiveMember | None:
        assert member.is_link, "member should be a link"

        # member_id should be set if it came from self.get_members() or iteration
        if member.member_id is None:
            logger.error(
                f"Attempted to resolve link for member {member.filename} with no member_id."
            )
            return None

        if member.member_id in visited_ids:
            logger.error(
                f"Link loop detected involving {member.filename} (ID: {member.member_id})."
            )
            return None
        visited_ids.add(member.member_id)

        target_member: ArchiveMember | None = None

        if member.type == MemberType.HARDLINK:
            link_target_str = member.link_target
            # This check is defensive, should be caught by the public resolve_link method
            if link_target_str is None:
                logger.warning(
                    f"Hardlink target string is None for {member.filename} (ID: {member.member_id})"
                )
                return None

            potential_targets = self._filename_to_members.get(link_target_str, [])
            # Find the most recent member with the same filename and a *lower* member_id
            # Ensure member_id is not None for potential targets as well
            valid_targets = [
                m
                for m in potential_targets
                if m.member_id is not None and m.member_id < member.member_id
            ]
            if not valid_targets:
                logger.warning(
                    f"Hardlink target {link_target_str} not found for {member.filename} (ID: {member.member_id}) or no earlier version exists."
                )
                return None

            target_member = max(valid_targets, key=lambda m: m.member_id)

        elif member.type == MemberType.SYMLINK:
            link_target_str = member.link_target
            if link_target_str is None:  # Defensive check
                logger.warning(
                    f"Symlink target string is None for {member.filename} (ID: {member.member_id})"
                )
                return None

            # Symlink targets are relative to the symlink's own directory
            normalized_link_target = posixpath.normpath(
                posixpath.join(posixpath.dirname(member.filename), link_target_str)
            )
            target_member = self._normalized_path_to_last_member.get(
                normalized_link_target
            )
            if target_member is None:
                logger.warning(
                    f"Symlink target '{normalized_link_target}' (from '{link_target_str}') not found for {member.filename} (ID: {member.member_id})."
                )
                return None
        else:
            # Not a link type that this method resolves, or already resolved.
            return member  # pragma: no cover

        # One of the above cases should have set target_member.
        assert target_member is not None

        # If the direct target is itself a link, resolve it further
        if target_member.is_link and target_member.link_target is not None:
            # Pass a copy of visited_members for the new recursion branch to handle complex cases correctly
            return self._resolve_link_recursive(target_member, visited_ids.copy())

        return target_member

    def _register_member(self, member: ArchiveMember) -> None:
        assert self._registration_lock.locked(), "Not in registration lock"

        assert member._member_id is None, (
            f"Member {member.filename} already registered with member_id {member.member_id}"
        )

        member._archive_id = self._archive_id
        member._member_id = len(self._members)
        self._members.append(member)

        logger.debug(f"Registering member {member.filename} ({member.member_id})")

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

        # Link resolution is now handled by the public resolve_link method when needed,
        # not automatically during registration.

    @abc.abstractmethod
    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        """
        Yield ArchiveMember objects one by one from the archive.

        This is a **crucial abstract method** that subclasses must implement.
        It's the primary way `BaseArchiveReader` discovers archive contents.
        The yielded `ArchiveMember` objects should have their metadata fields
        populated (filename, size, type, mtime, etc.). `BaseArchiveReader`
        will handle internal registration and link resolution.

        Yields:
            Iterator[ArchiveMember]: ArchiveMember instances from the archive.
        """
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

    def check_archive_open(self) -> None:
        if self._closed:
            raise ValueError("Archive is closed")

    def check_not_streaming_only(self, method_name: str) -> None:
        if self._streaming_only:
            raise ValueError(
                f"Archive opened for streaming only, {method_name} not supported"
            )

    def get_members(self) -> List[ArchiveMember]:
        self.check_archive_open()
        self.check_not_streaming_only("get_members()")

        while not self._all_members_registered:
            self._register_next_member()

        return list(self._members)

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """Get a list of all members in the archive, or None if not available. May not be available for stream archives."""
        self.check_archive_open()

        if self._all_members_registered:
            return list(self._members)

        if self._streaming_only and not self._early_members_list_supported:
            return None

        while not self._all_members_registered:
            self._register_next_member()

        return list(self._members)

    def iter_members(self) -> Iterator[ArchiveMember]:
        """Iterate over all members, registering them as they are discovered."""
        self.check_archive_open()

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

    def _prepare_member_for_open(
        self, member: ArchiveMember, *, pwd: bytes | str | None, for_iteration: bool
    ) -> ArchiveMember:
        """Hook for subclasses to adjust a member before opening."""
        return member

    @abc.abstractmethod
    def _open_member(
        self,
        member: ArchiveMember,
        *,
        pwd: bytes | str | None = None,
        for_iteration: bool = False,
    ) -> BinaryIO:
        """Open ``member`` and return a readable binary stream."""

    def _open_internal(
        self,
        member_or_filename: ArchiveMember | str,
        *,
        pwd: bytes | str | None,
        for_iteration: bool,
    ) -> BinaryIO:
        member = self.get_member(member_or_filename)
        member = self._prepare_member_for_open(
            member, pwd=pwd, for_iteration=for_iteration
        )
        final_member, _ = self._resolve_member_to_open(member)
        return self._open_member(final_member, pwd=pwd, for_iteration=for_iteration)

    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        """Open ``member_or_filename`` for random access reading."""
        self.check_archive_open()
        self.check_not_streaming_only("open()")

        return self._open_internal(member_or_filename, pwd=pwd, for_iteration=False)

    def _start_streaming_iteration(self) -> None:
        """Ensure only a single streaming iteration is performed for non-random-access readers."""
        if not self._streaming_only:
            return
        if self._streaming_iteration_started:
            raise ValueError("Streaming-only archive can only be iterated once")
        self._streaming_iteration_started = True

    def iter_members_with_io(
        self,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: IteratorFilterFunc | ExtractionFilter | None = None,
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
            A (ArchiveMember, BinaryIO) iterator over the members. Each stream
            should be consumed before advancing to the next member. Streams are
            closed automatically when iteration continues or the generator is
            closed. The stream may be None if the member is not a file.

        Notes:
            If :meth:`has_random_access` returns ``False`` (streaming-only
            access), this method can be called **only once**. Further attempts
            to iterate over the archive or to call :meth:`extractall` will raise
            ``ValueError``.
        """
        self.check_archive_open()
        self._start_streaming_iteration()

        filter_func = _build_filter(
            members, filter or self.config.extraction_filter, None
        )

        for member in self.iter_members():
            logger.debug(f"iter_members_with_io member: {member}")
            filtered_member = filter_func(member)
            if filtered_member is None:
                logger.debug(f"skipping {member.filename}")
                continue

            try:
                stream = (
                    LazyOpenIO(
                        self._open_internal,
                        member,
                        pwd=pwd,
                        for_iteration=True,
                        # Some backends are optimized for seeking inside files.
                        # Most others support seeking, but need to re-read the file from
                        # the beginning when seeking backwards.
                        # TODO: should we make these streams non-seekable?
                        seekable=not self._streaming_only,  # LazyOpenIO kwarg
                    )
                    if member.is_file
                    else None
                )
                yield filtered_member, stream

            finally:
                if stream is not None:
                    stream.close()

    def has_random_access(self) -> bool:
        """Check if opening members is possible (i.e. not streaming-only access)."""
        return not self._streaming_only

    def _extract_pending_files(
        self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None
    ):
        """
        Extract files that have been identified by the ExtractionHelper.

        This method is called by `extractall()` when `has_random_access()` is True.
        The default implementation iterates through `extraction_helper.get_pending_extractions()`
        and calls `self.open()` for each file member, then streams its content.

        Subclasses should override this if their underlying archive library offers a
        more efficient way to extract multiple files at once (e.g., a native
        `extractall`-like function in the third-party library).

        Args:
            path: The base extraction path (unused by default, but available).
            extraction_helper: The ExtractionHelper instance managing the process.
                               Use `extraction_helper.get_pending_extractions()` to
                               get the list of `ArchiveMember` objects to extract.
                               Use `extraction_helper.extract_member(member, stream)`
                               to perform the actual file writing.
            pwd: Optional password for decryption.
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
        filter_func: IteratorFilterFunc,
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
        filter_func: IteratorFilterFunc,
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
        filter: ExtractFilterFunc | ExtractionFilter | None = None,
    ) -> dict[str, ArchiveMember]:
        """Extract multiple members from the archive.

        Notes:
            For streaming-only archives (:meth:`has_random_access` returns ``False``)
            this method may only be called once, as it exhausts the underlying stream.
        """
        self.check_archive_open()

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        filter_func = _build_filter(
            members, filter or self.config.extraction_filter, path
        )

        extraction_helper = ExtractionHelper(
            self,
            path,
            self.config.overwrite_mode,
            can_process_pending_extractions=self.has_random_access(),
        )

        if self._streaming_only:
            self._extractall_with_streaming_mode(
                path, filter_func, pwd, extraction_helper
            )
        else:
            self._extractall_with_random_access(
                path, filter_func, pwd, extraction_helper
            )

        extraction_helper.apply_metadata()

        return extraction_helper.extracted_members_by_path

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
            logger.debug(
                f"Resolving link target for {member.filename} {member.type} {member.member_id}"
            )

            # If the user is opening a link, open the target member instead.
            resolved_target = self.resolve_link(member)
            if resolved_target is None:
                raise ArchiveMemberCannotBeOpenedError(
                    f"Link target not found or resolution failed for {member.filename} (when opening {filename})"
                )
            final_member = resolved_target
            logger.debug(
                f"Resolved link {member.filename} to {final_member.filename} (ID: {final_member.member_id})"
            )

        logger.debug(
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
        self.check_archive_open()
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
        self.check_archive_open()
        self.check_not_streaming_only("extract()")

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        member = self.get_member(member_or_filename)
        extraction_helper = ExtractionHelper(
            self,
            path,
            self.config.overwrite_mode,
            can_process_pending_extractions=False,
        )

        stream = self.open(member, pwd=pwd) if member.is_file else None

        extraction_helper.extract_member(member, stream)
        if stream:
            stream.close()

        extraction_helper.apply_metadata()

    @abc.abstractmethod
    def _close_archive(self) -> None:
        pass

    def close(self) -> None:
        if not self._closed:
            self._close_archive()
            self._closed = True
            self._members = None  # type: ignore
            self._filename_to_members = None  # type: ignore
            self._normalized_path_to_last_member = None  # type: ignore
            self._iterator_for_registration = None


class StreamingOnlyArchiveReaderWrapper(ArchiveReader):
    """
    A wrapper that restricts an ArchiveReader to streaming-only access.

    This class takes an existing ArchiveReader and makes it behave as if it
    does not support random access, by disabling methods like `open()`,
    `extract()`, and `get_members()` (if it implies random access).
    This is useful when `open_archive` is called with `streaming_only=True`.
    """

    def __init__(self, reader: ArchiveReader):
        super().__init__(reader.archive_path, reader.format)
        self.reader = reader
        self._streaming_iteration_started = False

    def close(self) -> None:
        self.reader.close()

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        return self.reader.get_members_if_available()

    def iter_members_with_io(
        self, *args, **kwargs
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        if self._streaming_iteration_started:
            raise ValueError("Streaming-only archive can only be iterated once")
        self._streaming_iteration_started = True
        return self.reader.iter_members_with_io(*args, **kwargs)

    def get_archive_info(self) -> ArchiveInfo:
        return self.reader.get_archive_info()

    def has_random_access(self) -> bool:
        return False

    def extractall(self, *args, **kwargs) -> dict[str, ArchiveMember]:
        if self._streaming_iteration_started:
            raise ValueError("Streaming-only archive can only be iterated once")
        self._streaming_iteration_started = True
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

    def resolve_link(self, member: ArchiveMember) -> ArchiveMember | None:
        return self.reader.resolve_link(member)
