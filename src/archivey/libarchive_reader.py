from __future__ import annotations

import datetime
import io
import logging
import os
import stat
from typing import BinaryIO, Iterator, cast, Any

import libarchive # type: ignore
from libarchive import ffi # type: ignore
from libarchive.entry import ArchiveEntry # type: ignore
from libarchive.exception import ArchiveError # type: ignore

from archivey.base_reader import BaseArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveIOError,
    ArchiveMemberCannotBeOpenedError,
    IsADirectoryError,
    FileNotFoundError,
)
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType
from archivey.extraction_helper import ExtractionHelper


logger = logging.getLogger(__name__)


class LibarchiveReader(BaseArchiveReader):
    """Archive reader using libarchive-c bindings."""

    def __init__(
        self,
        archive_path: BinaryIO | str | bytes | os.PathLike,
        format_hint: ArchiveFormat, # Renamed from 'format' to avoid conflict with self.format
        pwd: bytes | str | None = None,
        streaming_only: bool = False, # This flag influences BaseArchiveReader behavior
    ):
        if streaming_only:
            random_access_supported = False
        else:
            if hasattr(archive_path, "read"): # Stream-like
                random_access_supported = callable(getattr(archive_path, "seek", None))
            else: # Path-like or bytes (filename)
                random_access_supported = True

        super().__init__(
            format=ArchiveFormat.UNKNOWN, # libarchive will detect format
            archive_path=archive_path,    # Store original path/stream for re-opening
            random_access_supported=random_access_supported,
            members_list_supported=True,  # libarchive can list members
            pwd=pwd,
        )
        self._raw_archive_path_obj = archive_path
        self._member_infos_cache: list[ArchiveMember] | None = None

        # Determine the actual format from libarchive if possible by a quick open-check
        # This is primarily to set self.format early.
        temp_archive_cm = None
        try:
            temp_archive_cm = self._create_archive_cm()
            with temp_archive_cm as temp_archive:
                self._apply_password_to_handle(temp_archive) # Needed if format detection requires reading encrypted headers
                la_format_name = temp_archive.format_name
                self.format = ArchiveFormat.from_libarchive_name(la_format_name) # type: ignore[attr-defined]
        except ArchiveEncryptedError: # If password is required for headers, format might not be detectable yet
            self.format = format_hint if format_hint != ArchiveFormat.UNKNOWN else ArchiveFormat.UNKNOWN
            logger.warning(f"Could not determine format from libarchive due to encryption for {archive_path}, using hint: {self.format}")
        except Exception as e: # Other errors during initial format check
            self.format = format_hint if format_hint != ArchiveFormat.UNKNOWN else ArchiveFormat.UNKNOWN
            logger.warning(f"Could not determine format from libarchive for {archive_path} (error: {e}), using hint: {self.format}")
        finally:
            if temp_archive_cm and hasattr(temp_archive_cm, '__exit__'):
                temp_archive_cm.__exit__(None, None, None)


    def _create_archive_cm(self):
        """Creates and returns a new libarchive context manager."""
        path_obj = self._raw_archive_path_obj

        if hasattr(path_obj, "read"):
            if isinstance(path_obj, io.BytesIO):
                path_obj.seek(0)
                archive_data = path_obj.read()
                path_obj.seek(0)
                return libarchive.memory_reader(archive_data)
            elif callable(getattr(path_obj, "seek", None)):
                try:
                    current_pos = cast(BinaryIO, path_obj).tell()
                    cast(BinaryIO, path_obj).seek(0)
                    archive_data = cast(BinaryIO, path_obj).read()
                    cast(BinaryIO, path_obj).seek(current_pos)
                    return libarchive.memory_reader(archive_data)
                except Exception as e:
                    raise ArchiveIOError(
                        f"Stream {path_obj} is seekable but failed during read for libarchive: {e}"
                    ) from e
            else:
                # This case should be handled by open_archive ensuring streams are BytesIO if not seekable
                raise ArchiveIOError(f"Non-seekable stream {path_obj} must be pre-read into BytesIO for libarchive.")
        elif isinstance(path_obj, bytes):
            archive_path_str = os.fsdecode(path_obj)
            return libarchive.file_reader(archive_path_str)
        elif isinstance(path_obj, (str, os.PathLike)):
            archive_path_str = os.fspath(path_obj)
            return libarchive.file_reader(archive_path_str)
        else:
            raise TypeError(f"Unsupported archive_path type for _create_archive_cm: {type(path_obj)}")

    def _apply_password_to_handle(self, archive_handle):
        """Applies password to an open libarchive handle."""
        if self._archive_password:
            try:
                if hasattr(archive_handle, 'add_passphrase'):
                    # Passphrase must be str for libarchive-c
                    decoded_pwd = self._archive_password.decode('utf-8', 'surrogateescape') if isinstance(self._archive_password, bytes) else self._archive_password
                    archive_handle.add_passphrase(decoded_pwd)
                    logger.debug("Password applied to libarchive handle.")
                else: # pragma: no cover
                    logger.warning("add_passphrase method not found on libarchive handle. Password may not be applied.")
            except Exception as e:
                raise ArchiveEncryptedError(f"Error setting passphrase for libarchive: {e}") from e

    def _convert_entry_to_member(self, entry: ArchiveEntry) -> ArchiveMember:
        member_type = MemberType.UNKNOWN
        mode = entry.mode
        if stat.S_ISREG(mode): member_type = MemberType.FILE
        elif stat.S_ISDIR(mode): member_type = MemberType.FOLDER
        elif stat.S_ISLNK(mode): member_type = MemberType.SYMLINK
        # Other types like FIFO, CHAR_DEVICE, BLOCK_DEVICE could be added if needed.

        mtime_val = entry.mtime if entry.mtime is not None else 0
        dt_mtime = datetime.datetime.fromtimestamp(mtime_val, tz=datetime.timezone.utc)
        size = entry.size if entry.size is not None else 0

        pathname = entry.pathname
        if member_type == MemberType.FOLDER and not pathname.endswith('/'):
            pathname += '/'

        link_target = None
        if member_type == MemberType.SYMLINK:
            link_target = entry.linkname # linkname is preferred for symlinks by libarchive
        elif bool(entry.hardlink) and not entry.islnk: # True hardlink
             link_target = entry.hardlink

        return ArchiveMember(
            filename=pathname, type=member_type, size=size, mtime=dt_mtime,
            mode=entry.mode, uid=entry.uid, gid=entry.gid,
            link_target=link_target,
            comment=None, # libarchive entries don't easily provide this
            is_encrypted=entry.encrypted, # Uses entry.encrypted property
            crc=None, extra=None, # Not typically provided by libarchive generic entry
        )

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        if self._member_infos_cache is not None:
            # This method is for initial registration, so cache shouldn't be hit here
            # but if it is, it implies a logic error or re-entry.
            # For safety, let's just return from cache if populated.
            # However, BaseArchiveReader calls this only once.
            for member_info_compat in self._member_infos_cache: # Convert to ArchiveMember
                 yield ArchiveMember.from_archive_member_info(member_info_compat) # type: ignore[arg-type]
            return

        infos = []
        archive_cm = self._create_archive_cm()
        try:
            with archive_cm as archive:
                self._apply_password_to_handle(archive)
                for entry in archive:
                    member = self._convert_entry_to_member(entry)
                    infos.append(member) # Store ArchiveMember
                    yield member
        except ArchiveError as e:
            if "password" in str(e).lower() or "encrypted" in str(e).lower():
                 raise ArchiveEncryptedError(f"Libarchive error (encryption-related) during member iteration: {e}") from e
            if "corrupt" in str(e).lower() or "truncated" in str(e).lower():
                raise ArchiveCorruptedError(f"Libarchive error (corrupted/truncated) during member iteration: {e}") from e
            raise ArchiveIOError(f"Libarchive error during member iteration: {e}") from e
        except Exception as e:
            raise ArchiveIOError(f"Unexpected error during member iteration with libarchive: {e}") from e

        # Cache ArchiveMemberInfo compatible objects for BaseArchiveReader's internal use
        self._member_infos_cache = [info.to_archive_member_info() for info in infos]


    def open(self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None) -> BinaryIO:
        member_name: str
        if isinstance(member_or_filename, str):
            member_name = member_or_filename
        else: # Is ArchiveMember
            member_name = member_or_filename.filename
            if member_or_filename.type == MemberType.FOLDER:
                 raise IsADirectoryError(f"Member '{member_name}' is a directory.")
            if member_or_filename.type == MemberType.SYMLINK and not member_or_filename.link_target:
                # Symlink with no target, treat as zero-byte content (or error depending on strictness)
                # libarchive's get_blocks() on such symlinks might return empty or target path.
                # For now, let it proceed, could return empty bytes.
                pass


        # Use provided password for this operation if given, else the instance's default
        # Note: libarchive applies passwords per-archive, not per-file easily.
        # This local pwd might not have the desired effect if the archive was already opened
        # with a different password for listing. The _apply_password_to_handle uses self._archive_password.
        if pwd:
            logger.warning("Per-member password for open() with libarchive is complex; "
                           "relying on password set during initial archive interaction.")


        archive_cm = self._create_archive_cm()
        try:
            with archive_cm as archive:
                self._apply_password_to_handle(archive)
                for entry in archive:
                    current_entry_pathname = entry.pathname
                    if entry.isdir and not current_entry_pathname.endswith('/'):
                        current_entry_pathname += '/'

                    if current_entry_pathname == member_name:
                        if entry.isdir: # Should have been caught by ArchiveMember check too
                            raise IsADirectoryError(f"Member '{member_name}' is a directory.")

                        try:
                            # For symlinks, get_blocks() typically yields the link target as bytes.
                            # For regular files/hardlinks, yields content.
                            content_stream = io.BytesIO()
                            for block in entry.get_blocks():
                                content_stream.write(block)
                            content_stream.seek(0)
                            return content_stream
                        except ArchiveError as e: # Errors during block reading
                            raise ArchiveIOError(f"Error reading member '{member_name}' blocks with libarchive: {e}") from e

                raise FileNotFoundError(f"Member '{member_name}' not found in archive for opening.") # Consistent with os.FileNotFoundError
        except ArchiveError as e:
            if "password" in str(e).lower() or "encrypted" in str(e).lower():
                 raise ArchiveEncryptedError(f"Libarchive error (encryption-related) opening member '{member_name}': {e}") from e
            if "corrupt" in str(e).lower() or "truncated" in str(e).lower():
                raise ArchiveCorruptedError(f"Libarchive error (corrupted/truncated) opening member '{member_name}': {e}") from e
            raise ArchiveIOError(f"Libarchive error opening member '{member_name}': {e}") from e
        except Exception as e:
            raise ArchiveIOError(f"Unexpected error opening member '{member_name}' with libarchive: {e}") from e


    def _close_archive(self) -> None:
        # This reader manages handles per-operation, so no persistent self._archive to close here.
        self._member_infos_cache = None
        logger.debug("LibarchiveReader state cleared (cache reset).")
        # Base class close will handle self._archive_file_obj if it was responsible for opening a file.

    def get_archive_info(self) -> ArchiveInfo:
        # Attempt a quick open to get format info if not already determined
        if self.format == ArchiveFormat.UNKNOWN:
            temp_archive_cm = None
            try:
                temp_archive_cm = self._create_archive_cm()
                with temp_archive_cm as temp_archive:
                    self._apply_password_to_handle(temp_archive)
                    la_format_name = temp_archive.format_name
                    self.format = ArchiveFormat.from_libarchive_name(la_format_name) # type: ignore[attr-defined]
                    filter_names = []
                    if hasattr(temp_archive, '_archive') and temp_archive._archive:
                        archive_ptr = temp_archive._archive
                        count = ffi.archive_filter_count(archive_ptr)
                        for i in range(count):
                            name_ptr = ffi.archive_filter_name(archive_ptr, i)
                            if name_ptr: # pragma: no branch
                                filter_names.append(ffi.string(name_ptr).decode('utf-8', errors='replace'))

                    return ArchiveInfo(
                        format=self.format, format_name=la_format_name, filter_names=filter_names,
                        comment=None, is_solid=False, # Defaults, hard to get from libarchive
                        is_encrypted=self._archive_password is not None
                    )
            except Exception as e:
                logger.warning(f"Could not get archive info via libarchive: {e}")
                return ArchiveInfo(format=ArchiveFormat.UNKNOWN, format_name="unknown", is_encrypted=self._archive_password is not None)
            finally:
                 if temp_archive_cm and hasattr(temp_archive_cm, '__exit__'):
                    temp_archive_cm.__exit__(None, None, None)

        # If format was determined in __init__ or above
        return ArchiveInfo(
            format=self.format, format_name=self.format.value, # Or a more specific name if available
            is_encrypted=self._archive_password is not None
            # Other fields default
        )

# Ensure ArchiveFormat has the helper method
if not hasattr(ArchiveFormat, 'from_libarchive_name'):
    _LIBARCHIVE_FORMAT_MAP = {
        "zip": ArchiveFormat.ZIP, "rar": ArchiveFormat.RAR, "rar5": ArchiveFormat.RAR,
        "7zip": ArchiveFormat.SEVENZIP, "7-zip": ArchiveFormat.SEVENZIP,
        "tar": ArchiveFormat.TAR, "pax": ArchiveFormat.TAR, "gnu tar": ArchiveFormat.TAR,
        "iso9660": ArchiveFormat.ISO, "cd9660": ArchiveFormat.ISO,
        "gzip": ArchiveFormat.GZIP, "bzip2": ArchiveFormat.BZIP2,
        "xz": ArchiveFormat.XZ, "lzip": ArchiveFormat.LZIP,
        "lzma": ArchiveFormat.LZMA, "lz4": ArchiveFormat.LZ4, "zstd": ArchiveFormat.ZSTD,
        # Note: libarchive usually reports container format (e.g. "tar")
        # and compression via filter names (e.g. "gzip").
        # This map is for direct format names libarchive might report.
    }
    def from_libarchive_name(cls, name: str) -> ArchiveFormat:
        name_lower = name.lower().strip()
        if name_lower in _LIBARCHIVE_FORMAT_MAP:
            return _LIBARCHIVE_FORMAT_MAP[name_lower]
        # Fallbacks for common patterns
        if 'tar' in name_lower: return ArchiveFormat.TAR # Broad fallback
        return ArchiveFormat.UNKNOWN
    ArchiveFormat.from_libarchive_name = classmethod(from_libarchive_name) # type: ignore[assignment]
