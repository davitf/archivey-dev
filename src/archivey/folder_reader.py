import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterator, List, Optional

from archivey.base_reader import ArchiveReader, PathType
from archivey.exceptions import (
    ArchiveyError,
    CorruptedArchiveError,
    MemberNotFoundError,
)
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType


class FolderReader(ArchiveReader):
    """
    Reads a folder on the filesystem as an archive.
    """

    format = ArchiveFormat.FOLDER
    magic = None  # Not applicable for folders
    magic_offset = -1

    def __init__(
        self,
        archive: PathType,  # Should be a path to a directory
        password: Optional[str | bytes] = None,  # Not used for folders
        encoding: Optional[str] = None,  # Filesystem encoding is handled by OS
    ):
        super().__init__(archive, password=password, encoding=encoding)  # type: ignore

        if isinstance(archive, (str, bytes, os.PathLike)):
            self.archive_path = Path(archive).resolve()  # Store absolute path
        else:
            # FolderReader fundamentally needs a path, not a stream.
            raise TypeError("FolderReader requires a file system path, not a stream.")

        if not self.archive_path.is_dir():
            raise CorruptedArchiveError(f"Path is not a directory: {self.archive_path}")

        # Password and encoding are generally not applicable here.
        # self.encoding might be used for filename encoding if needed, but Python 3 handles unicode paths.

    def _get_member_type(self, path: Path, lstat_result: os.stat_result) -> MemberType:
        """Determines the MemberType from a path and its lstat result."""
        if stat.S_ISDIR(lstat_result.st_mode):
            return MemberType.DIR
        elif stat.S_ISLNK(lstat_result.st_mode):
            return MemberType.LINK
        elif stat.S_ISREG(lstat_result.st_mode):
            return MemberType.FILE
        return MemberType.OTHER

    def _convert_entry_to_member(
        self, entry_path: Path, root_path: Path
    ) -> ArchiveMember:
        """Converts a filesystem path to an ArchiveMember."""
        try:
            # Use lstat to get info about the link itself, not the target
            lstat_result = entry_path.lstat()
            # For actual file size and potentially other details if not a link, stat() is useful
            stat_result = (
                entry_path.stat() if not entry_path.is_symlink() else lstat_result
            )

        except OSError as e:
            # Could be a broken symlink or permission error
            # Create a placeholder member
            return ArchiveMember(
                filename=str(entry_path.relative_to(root_path)).replace(os.sep, "/"),
                file_size=0,
                compress_size=0,
                mtime=None,
                type=MemberType.OTHER,  # Or be more specific if possible from error
                comment=f"Error reading entry: {e}",
                raw_info=e,
            )

        member_type = self._get_member_type(entry_path, lstat_result)

        # Relative path from the root of the "archive" (the folder)
        # Ensure consistent '/' separator for archive paths
        relative_path_str = str(entry_path.relative_to(root_path)).replace(os.sep, "/")

        file_size = stat_result.st_size if member_type == MemberType.FILE else 0
        # For symlinks, file_size is often the length of the target path string.
        # Here, we'll keep it consistent with how other archive formats might report symlink size (often 0 or target path length)
        if member_type == MemberType.LINK:
            file_size = lstat_result.st_size

        link_target: Optional[str] = None
        if member_type == MemberType.LINK:
            try:
                link_target = os.readlink(entry_path)
            except OSError:
                link_target = "Error reading link target"

        return ArchiveMember(
            filename=relative_path_str,
            file_size=file_size,
            compress_size=file_size,  # No compression for folders
            mtime=datetime.fromtimestamp(lstat_result.st_mtime, tz=timezone.utc),
            type=member_type,
            mode=lstat_result.st_mode,
            link_target=link_target,
            raw_info=lstat_result,  # Store stat result for potential further use
            # CRC32 and compression_method are not applicable
        )

    def iter_members(self) -> Iterator[ArchiveMember]:
        if not self.archive_path.is_dir():
            raise ArchiveyError(f"Archive path is not a directory: {self.archive_path}")

        # Yield the root directory itself first
        # This is a common convention for archive listings
        try:
            root_stat = (
                self.archive_path.lstat()
            )  # lstat for consistency, though for root it's likely not a symlink itself
            yield ArchiveMember(
                filename="",  # Root of the archive
                file_size=0,  # Directories usually have 0 size or size of their entries list
                compress_size=0,
                mtime=datetime.fromtimestamp(root_stat.st_mtime, tz=timezone.utc),
                type=MemberType.DIR,
                mode=root_stat.st_mode,
                raw_info=root_stat,
            )
        except OSError as e:
            raise CorruptedArchiveError(
                f"Cannot stat root directory {self.archive_path}: {e}"
            ) from e

        for root, dirs, files in os.walk(
            self.archive_path, topdown=True, followlinks=False
        ):
            current_root_path = Path(root)

            # Process directories
            for dir_name in sorted(dirs):  # Sort for consistent order
                dir_path = current_root_path / dir_name
                yield self._convert_entry_to_member(dir_path, self.archive_path)

            # Process files
            for file_name in sorted(files):  # Sort for consistent order
                file_path = current_root_path / file_name
                yield self._convert_entry_to_member(file_path, self.archive_path)

    def get_members(self) -> List[ArchiveMember]:
        return list(self.iter_members())

    def open(
        self, member: ArchiveMember | str, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        # pwd is ignored for FolderReader

        member_name: str
        if isinstance(member, ArchiveMember):
            member_name = member.filename
        elif isinstance(member, str):
            member_name = member
        else:
            raise TypeError("member must be an ArchiveMember or a string path")

        # Convert archive path (with '/') to OS-specific path
        os_specific_member_path = member_name.replace("/", os.sep)
        full_path = self.archive_path / os_specific_member_path

        if not full_path.exists():
            raise MemberNotFoundError(
                f"Member not found: {member_name} (resolved to {full_path})"
            )

        if full_path.is_dir():
            raise IsADirectoryError(
                f"Cannot open directory '{member_name}' as a file stream."
            )

        # It's good practice to ensure the resolved path is still within the archive root
        # to prevent potential directory traversal issues if member_name contains '..'
        try:
            resolved_full_path = full_path.resolve()
            if (
                self.archive_path not in resolved_full_path.parents
                and resolved_full_path != self.archive_path
            ):
                # This check needs to be careful. If archive_path is /foo/bar and resolved_full_path is /foo/bar/file.txt
                # then archive_path is in resolved_full_path.parents.
                # If archive_path is /foo/bar and resolved_full_path is /foo/baz/file.txt (due to symlink or ..) this is bad.
                # A more robust check:
                if not str(resolved_full_path).startswith(str(self.archive_path)):
                    raise MemberNotFoundError(
                        f"Access to member '{member_name}' outside archive root is denied."
                    )

        except OSError as e:  # e.g. broken symlink during resolve()
            raise MemberNotFoundError(
                f"Error resolving path for member '{member_name}': {e}"
            ) from e

        try:
            return open(full_path, "rb")
        except OSError as e:
            raise CorruptedArchiveError(
                f"Cannot open member '{member_name}': {e}"
            ) from e

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(
            format=self.format.value,
            comment=str(self.archive_path),  # Use folder path as comment
            # is_solid, version, extra are not applicable for folders
        )

    def close(self) -> None:
        # No-op for FolderReader, as there's no main file handle to close.
        # Individual files are opened and closed in the open() method.
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def check_format_by_signature(cls, path_or_file: PathType) -> bool:
        # Folders don't have signatures in the traditional sense.
        # This method might always return False, or True if path_or_file is a directory.
        # For consistency with how `detect_archive_format` works, it relies on `os.path.isdir`.
        # This check is primarily for file-based signatures.
        if isinstance(path_or_file, (str, bytes, os.PathLike)):
            return Path(path_or_file).is_dir()
        return False  # Cannot determine if a stream is a "folder"

    @classmethod
    def check_format_by_path(cls, path: PathType) -> bool:
        """
        Checks if the given path is a directory.
        """
        if isinstance(path, (str, bytes, os.PathLike)):
            p = Path(path)
            return p.is_dir()
        return False

    @classmethod
    def get_extra_extensions(cls) -> list[str]:
        # Folders don't have extensions.
        return []

    # The init of ArchiveReader expects these, even if None.
    # We override them here to ensure they are set for the class.
    _supported_compressions: Optional[List[str]] = None
    _supported_encryption_methods: Optional[List[str]] = None
    _supported_encryption_strengths: Optional[List[int]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Ensure the format is correctly set from the class attribute
        # The super().__init__ in ArchiveReader does not take format as arg
        # but sets self._format from type(self).format
        # So this is mostly for clarity or if we directly manipulated self._format
        super().__init_subclass__(**kwargs)  # type: ignore
        if not hasattr(cls, "format") or cls.format != ArchiveFormat.FOLDER:
            raise TypeError(
                "FolderReader subclasses must have format set to ArchiveFormat.FOLDER"
            )


ArchiveReader.register(FolderReader)  # type: ignore
