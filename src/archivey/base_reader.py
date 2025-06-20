"""Defines the abstract base classes and common functionality for archive readers."""

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
    """
    Abstract base class defining the interface for an archive reader.

    This class provides a consistent way to interact with different archive
    formats. Subclasses must implement the abstract methods to provide
    format-specific functionality.
    """

    def __init__(
        self, archive_path: BinaryIO | str | bytes | os.PathLike, format: ArchiveFormat
    ):
        self._input = archive_path
        if isinstance(archive_path, (str, os.PathLike)):
            self.archive_path = str(archive_path)
        elif isinstance(archive_path, bytes):
            self.archive_path = archive_path.decode("utf-8")
        else:
            self.archive_path = "<stream>"
        self.format = format
        self.config: ArchiveyConfig = get_default_config()

    @abc.abstractmethod
    def close(self) -> None:
        """
        Close the archive and release any underlying resources.

        This method should be idempotent (callable multiple times without error).
        It is automatically called when the reader is used as a context manager.
        """
        pass

    @abc.abstractmethod
    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """
        Return a list of all ArchiveMember objects if readily available.

        For some archive formats (e.g., ZIP with a central directory), the full
        list of members can be obtained quickly without reading the entire archive.
        For others, especially stream-based formats, this might not be possible
        or efficient.

        Returns:
            A list of ArchiveMember objects, or None if the list is not readily
            available without significant processing. Implementations should prefer
            returning None over performing a costly operation here.
        """
        pass

    @abc.abstractmethod
    def get_members(self) -> List[ArchiveMember]:
        """
        Return a list of all ArchiveMember objects in the archive.

        This method guarantees returning the full list of members. However, for
        some archive types or streaming modes, this might involve processing a
        significant portion of the archive if the member list isn't available
        upfront (e.g., iterating through a tar stream).

        Returns:
            A list of all ArchiveMember objects.

        Raises:
            ArchiveError: If there's an issue reading member information.
            NotImplementedError: If the reader explicitly does not support
                                 obtaining a full member list in the current mode.
        """
        pass

    @abc.abstractmethod
    def iter_members_with_io(
        self,
        members: Collection[ArchiveMember | str]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: Optional[Union[bytes, str]] = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
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
            filter: Optional callable that takes an ArchiveMember and returns
                either the same ArchiveMember (or a modified one, though typically
                the same) if it should be included, or None if it should be skipped.
                This allows for more complex filtering logic.

        Yields:
            Iterator[tuple[ArchiveMember, Optional[BinaryIO]]]: An iterator where each
            item is a tuple containing the ArchiveMember object and a binary I/O
            stream for reading its content. The stream is None for non-file entries.
            Streams are closed automatically when iteration advances to the next
            member or when the generator is closed, so they should be consumed
            before requesting another member.

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
        self, member_or_filename: ArchiveMember | str, *, pwd: Optional[Union[bytes, str]] = None
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
        pwd: Optional[Union[bytes, str]] = None,
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
        pwd: Optional[Union[bytes, str]] = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
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
            filter: Optional callable that takes an ArchiveMember and returns
                either the same ArchiveMember (or a modified one) if it should be
                extracted, or None if it should be skipped. This is applied after
                the `members` selection.

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
        random_access_supported: bool,
        members_list_supported: bool,
        pwd: Optional[Union[bytes, str]] = None,
    ):
        """
        Initialize the BaseArchiveReader.

        Args:
            format: The ArchiveFormat enum value for this archive type.
            archive_path: Path to the archive file.
            random_access_supported: bool indicating if `open()` by member name is
                supported. If False, `iter_members_with_io` is likely the primary
                way to access content.
            members_list_supported: bool indicating if `get_members()` can provide
                a full list upfront (e.g., from a central directory). If False,
                `get_members()` might have to iterate through the archive.
            pwd: Optional default password for the archive.
        """
        super().__init__(archive_path, format)
        if pwd is not None and isinstance(pwd, str):
            self._archive_password: Optional[bytes] = pwd.encode("utf-8")
        elif isinstance(pwd, bytes):
            self._archive_password: Optional[bytes] = pwd
        else: # pwd is None
            self._archive_password: Optional[bytes] = None

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

        self._streaming_iteration_started: bool = False

    def get_archive_password(self) -> bytes | None:
        """Return the default password for the archive, if one was provided."""
        return self._archive_password

    def resolve_link(self, member: ArchiveMember) -> ArchiveMember | None:
        if not member.is_link or member.link_target is None:
            return member  # Not a link or no target path specified

        # Ensure all members are registered so lookups are complete
        # This is crucial for _normalized_path_to_last_member and _filename_to_members
        if not self._all_members_registered:
            # This call populates self._members and related lookup dicts
            # by exhausting self.iter_members_for_registration()
            self.get_members()

        return self._resolve_link_recursive(member, set())

    def _resolve_link_recursive(
        self, member: ArchiveMember, visited_ids: set[int]
    ) -> ArchiveMember | None:
        # Ensure _member_id is set. This should be guaranteed if the member
        # was obtained through normal archive operations.
        if member._member_id is None:
            logger.error(
                f"Attempted to resolve link for member {member.filename} with no internal member_id assigned."
            )
            return None

        # Now it's safe to use member.member_id property, which also checks _member_id
        if member.member_id in visited_ids:
            logger.error(
                f"Link loop detected involving {member.filename} (ID: {member.member_id})."
            )
            return None
        visited_ids.add(member.member_id)

        target_member: ArchiveMember | None = None

        if member.type == MemberType.HARDLINK:
            link_target_str = member.link_target
            # This check is defensive; link_target should be set for link types.
            if link_target_str is None:
                logger.warning(
                    f"Hardlink target string is None for {member.filename} (ID: {member.member_id})."
                )
                return None

            potential_targets = self._filename_to_members.get(link_target_str, [])
            # Find the most recent member with the same filename and a *lower* _member_id.
            # Accessing _member_id directly after confirming it's not None.
            valid_targets = [
                m
                for m in potential_targets
                if m._member_id is not None and m._member_id < member._member_id
            ]
            if not valid_targets:
                logger.warning(
                    f"Hardlink target '{link_target_str}' not found for {member.filename} (ID: {member.member_id}) or no earlier version exists."
                )
                return None
            # Sort by _member_id to get the highest one (most recent before current hardlink)
            target_member = max(valid_targets, key=lambda m: m._member_id) # type: ignore

        elif member.type == MemberType.SYMLINK:
            link_target_str = member.link_target
            if link_target_str is None:  # Defensive check
                logger.warning(
                    f"Symlink target string is None for {member.filename} (ID: {member.member_id})."
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
            return member

        if target_member is None:
            # This case should ideally be covered by the specific checks above,
            # but acts as a fallback.
            logger.warning(
                f"Could not find target for {member.type.value} link '{member.filename}' pointing to '{member.link_target}'."
            )
            return None

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

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        """Get a list of all members in the archive, or None if not available. May not be available for stream archives."""
        if self._all_members_registered:
            return list(self._members)

        if not self._early_members_list_supported:
            return None

        return self.get_members()

    def iter_members(self) -> Iterator[ArchiveMember]:
        """Iterate over all members, registering them as they are discovered."""
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

    def open_for_iteration(self, member, pwd: Optional[Union[bytes, str]] = None) -> BinaryIO:
        """
        Open a member for reading during iteration via `iter_members_with_io`.

        Defaults to calling `self.open(member, pwd=pwd)`.
        Subclasses can override this if opening a file during iteration requires
        different logic or optimizations than a direct `open()` call (e.g., if
        the underlying library provides a specific way to get a stream during
        its own iteration process).

        Args:
            member: The ArchiveMember to open.
            pwd: Optional password for decryption.

        Returns:
            A binary I/O stream for the member's content.
        """
        return self.open(member, pwd=pwd)

    def _start_streaming_iteration(self) -> None:
        """Ensure only a single streaming iteration is performed for non-random-access readers."""
        if self._random_access_supported:
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
        pwd: Optional[Union[bytes, str]] = None,
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
        # This is a default implementation for random-access readers which support
        # open().
        # assert self._random_access_supported, (
        #     "Non-random access readers must override iter_members_with_io()"
        # )

        self._start_streaming_iteration()

        filter_func = _build_iterator_filter(members, filter)

        for member in self.iter_members():
            logger.debug(f"iter_members_with_io member: {member}")
            filtered = filter_func(member)
            if filtered is None:
                logger.debug(f"skipping {member.filename}")
                continue

            try:
                # Some backends provide seekable streams for regular files.
                stream = (
                    LazyOpenIO(
                        self.open_for_iteration,
                        member,
                        pwd=pwd,
                        seekable=self._random_access_supported,
                    )
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
        self, path: str, extraction_helper: ExtractionHelper, pwd: Optional[Union[bytes, str]]
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
        filter_func: Callable[[ArchiveMember], Union[ArchiveMember, None]],
        pwd: Optional[Union[bytes, str]],
        pwd: Optional[Union[bytes, str]],
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
        pwd: Optional[Union[bytes, str]] = None,
        filter: Callable[[ArchiveMember], Union[ArchiveMember, None]] | None = None,
    ) -> dict[str, ArchiveMember]:
        """Extract multiple members from the archive.

        Notes:
            For streaming-only archives (:meth:`has_random_access` returns ``False``)
            this method may only be called once, as it exhausts the underlying stream.
        """

        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        filter_func = _build_iterator_filter(members, filter)

        extraction_helper = ExtractionHelper(
            self,
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

    def get_members(self) -> List[ArchiveMember]:
        if not self._early_members_list_supported:
            raise ValueError("Archive reader does not support get_members().")

        # Default implementation for random-access readers.
        # assert self._random_access_supported, (
        #     "Non-random access readers must override get_members()"
        # )
        while not self._all_members_registered:
            self._register_next_member()

        return list(self._members)

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
        pwd: Optional[Union[bytes, str]] = None,
    ) -> str | None:
        """Extract a single member to the specified path.

        This method is only available if `has_random_access()` returns `True`.
        For symlinks, this will attempt to extract the link itself, not its target.
        To extract the target of a symlink, resolve it first using `resolve_link()`
        and then extract the resulting `ArchiveMember`.

        Args:
            member_or_filename: The ArchiveMember object or the filename (str) of
                the member to extract.
            path: Target directory for extraction. Defaults to the current working
                directory if None. The directory will be created if it doesn't exist.
            pwd: Optional password (str or bytes) for decrypting the member if it's
                encrypted.

        Returns:
            The absolute path to the extracted file or directory, or None if nothing
            was extracted (e.g., if the member was a directory and it already existed,
            though specifics may vary).

        Raises:
            NotImplementedError: If the archive is opened in a streaming-only mode
                                 (i.e., `has_random_access()` is `False`).
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveMemberCannotBeOpenedError: If the member is a type that cannot be
                                            opened for extraction (e.g., a directory,
                                            though behavior might vary by implementation).
            ArchiveEncryptedError: If the member is encrypted and `pwd` is incorrect
                                   or not provided.
            ArchiveCorruptedError: If the member data is found to be corrupt.
            ArchiveExtractionError: For other issues during extraction.
        """
        if path is None:
            path = os.getcwd()
        else:
            path = str(path)

        if self._random_access_supported:
            member = self.get_member(member_or_filename)
            # For extract(), we are extracting one specific item.
            # We don't want to resolve links automatically here, because the user might
            # specifically want to extract the symlink itself.
            # If they want to extract the target, they should resolve it first.

            extraction_helper = ExtractionHelper(
                self,
                path,
                self.config.overwrite_mode,
                # can_process_pending_extractions is False because we are only extracting one item.
                # This ensures that _extract_pending_files is not called, and extract_member
                # will directly write the file if it's a regular file.
                can_process_pending_extractions=False,
            )

            # For non-file types like directories or symlinks, stream will be None.
            # extract_member handles creation of these types without a stream.
            stream = self.open(member, pwd=pwd) if member.is_file else None
            try:
                # extract_member will handle directory creation, symlink creation, etc.
                # and file writing if stream is provided.
                extraction_helper.extract_member(member, stream)
            finally:
                if stream:
                    stream.close()

            extraction_helper.apply_metadata() # Applies permissions, mtime for the extracted item.

            # Return the full path of the extracted item.
            # The key in extracted_members_by_path is the absolute path.
            extracted_paths = list(extraction_helper.extracted_members_by_path.keys())
            return extracted_paths[0] if extracted_paths else None
        else: # Not self._random_access_supported
            raise NotImplementedError(
                "extract() is not supported for this streaming-only archive. "
                "Use extractall() or iterate with iter_members_with_io() instead."
            )


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
        self, member: ArchiveMember, *, pwd: Optional[Union[bytes, str]] = None
    ) -> BinaryIO:
        raise ValueError("Streaming-only archive reader does not support open().")

    def extract(
        self,
        member_or_filename: ArchiveMember | str,
        path: str | None = None,
        pwd: Optional[Union[bytes, str]] = None,
    ) -> str | None:
        raise ValueError("Streaming-only archive reader does not support extract().")

    def resolve_link(self, member: ArchiveMember) -> ArchiveMember | None:
        return self.reader.resolve_link(member)
