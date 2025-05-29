import abc
import io
import os
from typing import IO, Any, Union, Optional

from archivey.types import ArchiveMember


class ArchiveWriter(abc.ABC):
    """Abstract base class for archive writers."""

    def __init__(self, archive: Union[str, IO[bytes]], mode: str = "w", *, encoding: Optional[str] = None, **kwargs: Any):
        """Initialize the archive writer.

        Args:
            archive: The path to the archive file or a file-like object.
            mode: The mode to open the archive in (e.g., "w", "x", "a").
            encoding: The encoding to use for filenames and comments.
            **kwargs: Additional keyword arguments for specific archive formats.
        """
        self._archive = archive
        self._mode = mode
        self._encoding = encoding
        self._kwargs = kwargs
        self._closed = False

    @abc.abstractmethod
    def open(
        self,
        member_info: Union[str, ArchiveMember],
        mode: str = "w",
    ) -> IO[bytes]:
        """Open a member for writing.

        Args:
            member_info: The name of the member or an ArchiveMember object.
                         If an ArchiveMember object is provided, its attributes
                         (like mtime, permissions) may be used.
            mode: The mode to open the member in (currently only "w" is supported).

        Returns:
            A file-like object for writing the member's contents.

        Raises:
            ValueError: If the archive is closed or mode is not 'w'.
            ArchiveError: For other archive-related errors.
        """
        if self._closed:
            raise ValueError("Archive is closed")
        if mode != "w":
            # TODO: Potentially support "r" for read-write archives if underlying format allows
            raise ValueError("Only 'w' mode is currently supported for opening members")

    def writestr(
        self,
        member_info: Union[str, ArchiveMember],
        data: Union[bytes, str],
        *,
        encoding: Optional[str] = None,
    ) -> None:
        """Write a string or bytes to a member in the archive.

        Args:
            member_info: The name of the member or an ArchiveMember object.
            data: The data to write (either bytes or a string).
            encoding: The encoding to use if data is a string. Defaults to the
                      archive's encoding or UTF-8.
        """
        if self._closed:
            raise ValueError("Archive is closed")

        _encoding = encoding or self._encoding or "utf-8"
        _data_bytes: bytes
        if isinstance(data, str):
            _data_bytes = data.encode(_encoding)
        else:
            _data_bytes = data

        with self.open(member_info, mode="w") as member_io:
            member_io.write(_data_bytes)

    def write(
        self,
        filename: str,
        arcname: Optional[str] = None,
        *,
        recursive: bool = True,
        filter: Optional[callable[[str], bool]] = None,
    ) -> None:
        """Write a file or directory to the archive.

        Args:
            filename: Path to the file or directory to add.
            arcname: Name for the file or directory in the archive.
                     If None, it's derived from `filename`.
            recursive: If True (default), recursively add files in directories.
            filter: A callable that takes a filename and returns True if it
                    should be added, False otherwise.
        """
        if self._closed:
            raise ValueError("Archive is closed")

        _arcname = arcname or os.path.basename(filename)

        if os.path.isdir(filename):
            if recursive:
                for root, dirs, files in os.walk(filename):
                    # Filter directories
                    if filter:
                        dirs[:] = [d for d in dirs if filter(os.path.join(root, d))]

                    for name in files:
                        current_file_path = os.path.join(root, name)
                        if filter and not filter(current_file_path):
                            continue
                        # Create arcname relative to the original directory
                        relative_path = os.path.relpath(current_file_path, filename)
                        member_arcname = os.path.join(_arcname, relative_path)
                        self._write_file_to_archive(current_file_path, member_arcname)
                    # Optionally add directory entries (some archive formats might not need this)
                    # For now, we only add files.
            else:
                # Add an empty directory entry if not recursive (behavior might vary by format)
                # For simplicity, we can choose to only add files or require explicit directory member creation.
                # Current implementation focuses on files.
                pass # Or raise an error, or add a directory entry if format supports
        else: # It's a file
            if filter and not filter(filename):
                return
            self._write_file_to_archive(filename, _arcname)

    def _write_file_to_archive(self, filepath: str, arcname: str) -> None:
        """Helper to write a single file to the archive."""
        # Create an ArchiveMember to pass mtime and potentially other attributes
        # This requires a more fleshed out ArchiveMember or a way to pass these
        # to the open() method of subclasses.
        # For now, just pass arcname as string.
        # stat_result = os.stat(filepath)
        # member = ArchiveMember(
        #     filename=arcname,
        #     size=stat_result.st_size,
        #     mtime=datetime.fromtimestamp(stat_result.st_mtime),
        #     # permissions=stat.S_IMODE(stat_result.st_mode) # Requires stat module
        # )
        with self.open(arcname, mode="w") as member_io:
            with open(filepath, "rb") as f_in:
                io.copyfileobj(f_in, member_io)

    @abc.abstractmethod
    def close(self) -> None:
        """Close the archive and finalize its contents.

        This method must be called to ensure all data is written and resources
        are released.
        """
        if not self._closed:
            self._closed = True

    def __enter__(self) -> "ArchiveWriter":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # Potential additional methods:
    # - add_member(member_info: ArchiveMember, data_stream: IO[bytes])
    # - comment (property for archive comment)
