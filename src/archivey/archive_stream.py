# A zipfile-like interface for reading all the files in an archive.

import logging
import os
import shutil
from typing import IO, Any, Iterator, List, Union

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError,
)
from archivey.formats import detect_archive_format
from archivey.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
)

logger = logging.getLogger(__name__)


def create_archive_reader(
    archive_path: str,
    use_libarchive: bool = False,
    use_rar_stream: bool = False,
    **kwargs: dict[str, Any],
) -> ArchiveReader:
    """Create an appropriate archive reader for the given file.

    Args:
        archive_path: Path to the archive file
        use_libarchive: Whether to use libarchive for reading
        use_rar_stream: Whether to use the RAR stream reader for RAR files
        **kwargs: Additional options passed to the reader

    Returns:
        An ArchiveReader instance

    Raises:
        ArchiveNotSupportedError: If the archive format is not supported
        ArchiveError: For other archive-related errors
    """
    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"Archive file not found: {archive_path}")

    format = detect_archive_format(archive_path)

    # ext = os.path.splitext(archive_path)[1].lower()
    pwd = kwargs.get("pwd")
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    if use_libarchive:
        raise NotImplementedError("LibArchiveReader is not implemented")
        # from archivey.libarchive_reader import LibArchiveReader

        # return LibArchiveReader(archive_path, **kwargs)

    if format == ArchiveFormat.RAR:
        if use_rar_stream:
            from archivey.rar_reader import RarStreamReader

            return RarStreamReader(archive_path, pwd=pwd)
        else:
            from archivey.rar_reader import RarReader

            return RarReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.ZIP:
        from archivey.zip_reader import ZipReader

        return ZipReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.SEVENZIP:
        from archivey.sevenzip_reader import SevenZipReader

        return SevenZipReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.TAR or format in TAR_COMPRESSED_FORMATS:
        from archivey.tar_reader import TarReader

        return TarReader(archive_path, pwd=pwd, format=format)

    if format in SINGLE_FILE_COMPRESSED_FORMATS:
        from archivey.single_file_reader import SingleFileReader

        return SingleFileReader(archive_path, pwd=pwd, format=format)

    raise ArchiveNotSupportedError(f"Unsupported archive format: {format}")


class ArchiveStream:
    """A zipfile-like interface for reading all the files in an archive."""

    def __init__(
        self,
        filename: str,
        use_libarchive: bool = False,
        use_rar_stream: bool = False,
        pwd: str | None = None,
        use_single_file_stored_metadata: bool = False,
        **kwargs: dict[str, Any],
    ):
        """Initialize the archive stream.

        Args:
            filename: Path to the archive file
            use_libarchive: Whether to use libarchive for reading
            use_rar_stream: Whether to use the RAR stream reader for RAR files
            pwd: Password to use for decryption
            **kwargs: Additional options passed to the reader
        """
        self._reader: ArchiveReader

        if not os.path.exists(filename):
            raise FileNotFoundError(f"Archive file not found: {filename}")

        format = detect_archive_format(filename)
        logger.debug(f"Archive format for {filename}: {format}")

        if use_libarchive:
            raise NotImplementedError("LibArchiveReader is not implemented")
            # from archivey.libarchive_reader import LibArchiveReader

            # self._reader = LibArchiveReader(filename, **kwargs)
        elif format == ArchiveFormat.TAR or format in TAR_COMPRESSED_FORMATS:
            from archivey.tar_reader import TarReader

            self._reader = TarReader(filename, pwd=pwd, format=format)

        elif format == ArchiveFormat.RAR:
            if use_rar_stream:
                from archivey.rar_reader import RarStreamReader

                self._reader = RarStreamReader(filename, pwd=pwd)
            else:
                from archivey.rar_reader import RarReader

                self._reader = RarReader(filename, pwd=pwd)
        elif format == ArchiveFormat.ZIP:
            from archivey.zip_reader import ZipReader

            self._reader = ZipReader(filename, pwd=pwd)
        elif format == ArchiveFormat.SEVENZIP:
            from archivey.sevenzip_reader import SevenZipReader

            self._reader = SevenZipReader(filename, pwd=pwd)
        elif format in SINGLE_FILE_COMPRESSED_FORMATS:
            from archivey.single_file_reader import SingleFileReader

            self._reader = SingleFileReader(
                filename,
                pwd=pwd,
                format=format,
                use_stored_metadata=use_single_file_stored_metadata,
            )
        else:
            raise ArchiveNotSupportedError(
                f"Unsupported archive format: {filename} {format}"
            )

    def __enter__(self) -> "ArchiveStream":
        """Return self for context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the archive."""
        self._reader.close()

    def namelist(self) -> List[str]:
        """Get a list of all member names in the archive."""
        return [member.filename for member in self._reader.get_members()]

    def infolist(self) -> List[ArchiveMember]:
        """Get a list of ArchiveMember objects for all members in the archive."""
        return self._reader.get_members()

    def info_iter(self) -> Iterator[ArchiveMember]:
        """Get an iterator over ArchiveMember objects for all members in the archive."""
        return self._reader.iter_members()

    def get_format(self) -> ArchiveFormat:
        """Get the format of the archive."""
        return self._reader.get_format()

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive format."""
        return self._reader.get_archive_info()

    def getinfo(self, name: str) -> ArchiveMember:
        """Get an ArchiveMember object for a specific member.

        Args:
            name: Name of the member to get info for

        Returns:
            ArchiveMember object for the specified member

        Raises:
            ArchiveMemberNotFoundError: If the member doesn't exist
        """
        for member in self._reader.get_members():
            if member.filename == name:
                return member
        raise ArchiveMemberNotFoundError(f"Member not found: {name}")

    def open(self, name: ArchiveMember, *, pwd: bytes | str | None = None) -> IO[bytes]:
        """Open a member for reading.

        Args:
            name: Member to open
            pwd: Password to use for decryption

        Returns:
            A file-like object for reading the member's contents

        Raises:
            ArchiveMemberNotFoundError: If the member doesn't exist
            ArchiveError: For other archive-related errors
        """
        return self._reader.open(name, pwd=pwd)

    def extract(
        self,
        member: Union[str, ArchiveMember],
        path: str | None = None,
        preserve_ownership: bool = False,
        preserve_links: bool = True,
    ) -> str:
        """Extract a member to the filesystem.

        Args:
            member: Either the member name or an ArchiveMember object
            path: Directory to extract to (defaults to current directory)
            preserve_ownership: Whether to preserve file ownership
            preserve_links: Whether to preserve symbolic links

        Returns:
            Path to the extracted file

        Raises:
            ArchiveMemberNotFoundError: If the member doesn't exist
            ArchiveError: For other archive-related errors
        """
        if path is None:
            path = os.getcwd()

        if isinstance(member, str):
            member = self.getinfo(member)

        target_path = os.path.join(path, member.filename)

        # Create parent directories if they don't exist
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        if member.is_dir:
            os.makedirs(target_path, exist_ok=True)
            return target_path

        if member.is_link and preserve_links:
            # Handle symbolic links
            with self.open(member) as f:
                link_target = f.read().decode("utf-8")
            os.symlink(link_target, target_path)
            return target_path

        # Regular file
        with self.open(member) as src, open(target_path, "wb") as dst:
            shutil.copyfileobj(src, dst)

        # Preserve modification time
        if member.mtime:
            os.utime(target_path, (member.mtime.timestamp(), member.mtime.timestamp()))

        return target_path

    def extractall(
        self,
        path: str | None = None,
        preserve_ownership: bool = False,
        preserve_links: bool = True,
    ) -> None:
        """Extract all members to the filesystem.

        Args:
            path: Directory to extract to (defaults to current directory)
            preserve_ownership: Whether to preserve file ownership
            preserve_links: Whether to preserve symbolic links

        Raises:
            ArchiveError: For archive-related errors
        """
        if path is None:
            path = os.getcwd()

        for member in self._reader.iter_members():
            self.extract(member, path, preserve_ownership, preserve_links)

    @property
    def comment(self) -> str | None:
        """Get the comment for the archive.

        Added for compatibility with zipfile.ZipFile.
        """
        return self._reader.get_archive_info().comment
