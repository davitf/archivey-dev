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
    """Creates an appropriate archive reader for the given file.

    This function is primarily for internal use by `ArchiveStream` but
    can be used directly if specific reader control is needed.

    Args:
        archive_path: Path to the archive file.
        use_libarchive: Whether to attempt using the libarchive backend.
                        (Currently not implemented).
        use_rar_stream: Whether to use the streaming reader for RAR files.
                        If False, the standard `rarfile` backend is used.
        **kwargs: Additional keyword arguments passed to the underlying
                  reader's constructor (e.g., `pwd` for password).

    Returns:
        An `ArchiveReader` instance suitable for the detected archive type.

    Raises:
        FileNotFoundError: If `archive_path` does not exist.
        TypeError: If `pwd` is provided but is not a string or bytes.
        NotImplementedError: If `use_libarchive` is True, as it's not implemented.
        ArchiveNotSupportedError: If the archive format is not supported or
                                  cannot be determined.
        ArchiveError: For other archive-related errors during reader instantiation.
    """
    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"Archive file not found: {archive_path}")

    ext = os.path.splitext(archive_path)[1].lower()
    pwd = kwargs.get("pwd")
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    if use_libarchive:
        raise NotImplementedError("LibArchiveReader is not implemented")
        # from archivey.libarchive_reader import LibArchiveReader

        # return LibArchiveReader(archive_path, **kwargs)

    if ext == ".rar":
        if use_rar_stream:
            from archivey.rar_reader import RarStreamReader

            return RarStreamReader(archive_path, pwd=pwd)
        else:
            from archivey.rar_reader import RarReader

            return RarReader(archive_path, pwd=pwd)

    if ext == ".zip":
        from archivey.zip_reader import ZipReader

        return ZipReader(archive_path, pwd=pwd)

    if ext == ".7z":
        from archivey.sevenzip_reader import SevenZipReader

        return SevenZipReader(archive_path, pwd=pwd)

    if ext == ".tar":
        from archivey.tar_reader import TarReader

        return TarReader(archive_path, pwd=pwd)

    if ext in [".gz", ".bz2", ".xz", ".tgz", ".tbz", ".txz"]:
        # Check if it's a tar archive
        member_name = os.path.splitext(os.path.basename(archive_path))[0]
        if ext in [".tgz", ".tbz", ".txz"] or member_name.lower().endswith(".tar"):
            from archivey.tar_reader import TarReader

            return TarReader(archive_path, pwd=pwd)
        else:
            from archivey.single_file_reader import SingleFileReader

            return SingleFileReader(archive_path, pwd=pwd)

    raise ArchiveNotSupportedError(f"Unsupported archive format: {ext}")


class ArchiveStream:
    """A zipfile-like interface for reading all the files in an archive."""

    def __init__(
        self,
        filename: str,
        use_libarchive: bool = False,
        use_rar_stream: bool = False,
        pwd: str | bytes | None = None,
        use_single_file_stored_metadata: bool = False,
        **kwargs: dict[str, Any],
    ):
        """Initializes an ArchiveStream for reading an archive file.

        This class provides a high-level, `zipfile`-like interface for
        interacting with various archive formats. It automatically detects the
        archive type and selects an appropriate backend reader.

        Args:
            filename: Path to the archive file.
            use_libarchive: If True, attempts to use the libarchive backend
                            (currently not implemented).
            use_rar_stream: For RAR archives, if True, uses a streaming reader
                            which might be more memory-efficient for certain
                            operations but could be slower. If False, uses the
                            standard `rarfile` backend.
            pwd: Password to use for decrypting encrypted archives. Can be
                 `str` or `bytes`.
            use_single_file_stored_metadata: For single-file compressed
                archives (e.g., .gz, .bz2), if True, attempts to use metadata
                (like original filename) stored within the archive if available.
            **kwargs: Additional keyword arguments passed to the underlying
                      reader's constructor.

        Raises:
            FileNotFoundError: If `filename` does not exist.
            NotImplementedError: If `use_libarchive` is True.
            ArchiveNotSupportedError: If the archive format is not supported or
                                      cannot be determined.
            ArchiveError: For other archive-related errors during initialization.
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
        """Enters the runtime context related to this object.

        The `with` statement will bind this method’s return value to the
        target(s) specified in the `as` clause of the statement.

        Returns:
            The `ArchiveStream` object itself.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exits the runtime context and closes the archive.

        Performs cleanup actions, primarily closing the underlying archive
        reader.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_val: The exception instance that caused the context to be exited.
                     None if the context was exited without an exception.
            exc_tb: A traceback object encapsulating the call stack at the
                    point where the exception was raised. None if the context
                    was exited without an exception.
        """
        self._reader.close()

    def namelist(self) -> List[str]:
        """Returns a list of all member names in the archive.

        This method is similar to `zipfile.ZipFile.namelist()`.

        Returns:
            A list of strings, where each string is a member's filename.
        """
        return [member.filename for member in self._reader.get_members()]

    def infolist(self) -> List[ArchiveMember]:
        """Returns a list of `ArchiveMember` objects for all members.

        This provides more detailed information about each member than
        `namelist()`.

        Returns:
            A list of `ArchiveMember` objects.
        """
        return self._reader.get_members()

    def info_iter(self) -> Iterator[ArchiveMember]:
        """Returns an iterator over `ArchiveMember` objects in the archive.

        This method is useful for large archives as it avoids loading all
        member information into memory at once.

        Returns:
            An iterator yielding `ArchiveMember` objects.
        """
        return self._reader.iter_members()

    def get_format(self) -> ArchiveFormat:
        """Returns the detected format of the archive.

        Returns:
            An `ArchiveFormat` enum member representing the archive's format.
        """
        return self._reader.get_format()

    def get_archive_info(self) -> ArchiveInfo:
        """Returns detailed information about the archive itself.

        This may include things like archive-level comments or format-specific
        details, if supported by the backend reader.

        Returns:
            An `ArchiveInfo` object.
        """
        return self._reader.get_archive_info()

    def getinfo(self, name: str) -> ArchiveMember:
        """Returns an `ArchiveMember` object for a specific member name.

        Args:
            name: The name of the member to retrieve information for. This
                  should match the `filename` attribute of an `ArchiveMember`.

        Returns:
            An `ArchiveMember` object for the specified member.

        Raises:
            ArchiveMemberNotFoundError: If a member with the given name is not
                                      found in the archive.
        """
        for member in self._reader.get_members():
            if member.filename == name:
                return member
        raise ArchiveMemberNotFoundError(f"Member not found: {name}")

    def open(
        self, member: Union[str, ArchiveMember], *, pwd: bytes | str | None = None
    ) -> IO[bytes]:
        """Opens a member within the archive for reading.

        This returns a file-like object from which the member's contents
        can be read.

        Args:
            member: Either the name of the member (str) or an `ArchiveMember`
                    object. If a string is provided, `getinfo()` will be called
                    to retrieve the `ArchiveMember` object.
            pwd: Password to use for decryption if the member is encrypted.
                 This overrides the password provided during `ArchiveStream`
                 initialization for this specific open operation.
                 Can be `str` or `bytes`.

        Returns:
            A buffered binary I/O stream (file-like object) for reading the
            member's content.

        Raises:
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveEncryptedError: If the member is encrypted and no password
                                   is provided or the provided password is incorrect.
            ArchiveCorruptedError: If the member data is corrupted.
            ArchiveError: For other archive-related errors during opening.
        """
        if isinstance(member, str):
            member_info = self.getinfo(member)
        elif isinstance(member, ArchiveMember):
            member_info = member
        else:
            raise TypeError(
                "open() requires a member name (str) or ArchiveMember object"
            )
        return self._reader.open(member_info, pwd=pwd)

    def extract(
        self,
        member: Union[str, ArchiveMember],
        path: str | None = None,
        preserve_ownership: bool = False,
        preserve_links: bool = True,
    ) -> str:
        """Extracts a single member from the archive to the filesystem.

        Args:
            member: Either the name of the member (str) or an `ArchiveMember`
                    object to extract.
            path: The directory to extract the member to. If None, defaults to
                  the current working directory. The member's full path from
                  the archive will be appended to this base path.
                  Non-existent parent directories will be created.
            preserve_ownership: If True, attempts to preserve the original
                                ownership information of the extracted file/directory.
                                This operation may require special privileges.
                                (Currently not fully implemented across all readers).
            preserve_links: If True, symbolic links and hard links within the
                            archive will be recreated as links on the filesystem.
                            If False, the target of the link might be extracted instead,
                            or the link ignored, depending on the backend reader.

        Returns:
            The absolute path to the extracted file or directory.

        Raises:
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveError: For other archive-related errors during extraction,
                          such as permission issues or disk space errors.
            TypeError: If `member` is not a string or `ArchiveMember` object.
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

        if not isinstance(member, (str, ArchiveMember)):
            raise TypeError(
                "extract() requires a member name (str) or ArchiveMember object"
            )

        member_info = self.getinfo(member) if isinstance(member, str) else member

        # Ensure target_path is absolute and normalized
        # Path traversal vulnerability mitigation: os.path.join might be problematic
        # if member.filename contains ".." or starts with "/".
        # Normalizing and ensuring it's within the intended 'path' directory.
        target_filename = os.path.normpath(member_info.filename)
        if os.path.isabs(target_filename) or target_filename.startswith(".."):
            # Potentially unsafe filename, log or raise error
            # For now, we join and let OS handle it, but this could be hardened
            logger.warning(
                f"Potentially unsafe member filename: {member_info.filename}"
            )
            # A more robust solution would be to sanitize member_info.filename
            # or ensure it's relative and clean.

        target_path = os.path.join(path, target_filename)
        target_path = os.path.abspath(target_path) # Ensure it's absolute

        # Security check: Ensure the target_path is within the extraction 'path'
        if not target_path.startswith(os.path.abspath(path)):
            raise ArchiveError(
                f"Attempted to extract file outside of target directory: {target_path}"
            )


        # Create parent directories if they don't exist
        # os.path.dirname might be empty if target_path is just a filename
        parent_dir = os.path.dirname(target_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        if member_info.is_dir:
            os.makedirs(target_path, exist_ok=True)
            # TODO: Set permissions and ownership if preserve_ownership is True
            return target_path

        if member_info.is_link and preserve_links:
            # Handle symbolic links
            # Ensure link target is read correctly
            link_target_bytes = b""
            try:
                with self.open(member_info) as f:
                    link_target_bytes = f.read() # Link target is stored as content
            except Exception as e:
                logger.error(f"Failed to read link target for {member_info.filename}: {e}")
                # Decide how to handle: skip, raise, or create empty file?
                # For now, we'll skip creating the link if target read fails.
                return target_path # Or raise an error

            try:
                link_target = link_target_bytes.decode("utf-8") # Assuming UTF-8
            except UnicodeDecodeError:
                # If not UTF-8, could be platform specific or binary.
                # For now, we'll try a best-effort platform-dependent decoding.
                link_target = os.fsdecode(link_target_bytes)

            if os.path.exists(target_path) or os.path.lexists(target_path):
                os.remove(target_path) # Remove if it exists (e.g., from previous extract)

            try:
                os.symlink(link_target, target_path)
                logger.debug(f"Created symlink: {target_path} -> {link_target}")
            except Exception as e:
                logger.error(f"Failed to create symlink {target_path}: {e}")
                # Fallback or error handling if symlink creation fails
            return target_path

        # Regular file
        try:
            with self.open(member_info) as src, open(target_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        except Exception as e:
            # Clean up partially written file if extraction fails
            if os.path.exists(target_path):
                os.remove(target_path)
            raise ArchiveError(f"Error extracting file {member_info.filename}: {e}") from e


        # Preserve modification time
        if member_info.mtime:
            try:
                os.utime(target_path, (member_info.mtime.timestamp(), member_info.mtime.timestamp()))
            except Exception as e:
                logger.warning(f"Could not set modification time for {target_path}: {e}")

        # TODO: Preserve permissions and ownership if preserve_ownership is True

        return target_path

    def extractall(
        self,
        path: str | None = None,
        members: List[Union[str, ArchiveMember]] | None = None,
        preserve_ownership: bool = False,
        preserve_links: bool = True,
    ) -> None:
        """Extracts all (or specified) members from the archive to the filesystem.

        Args:
            path: The directory to extract members to. If None, defaults to the
                  current working directory.
            members: A list of members to extract. Can be a list of member names
                     (str) or `ArchiveMember` objects. If None, all members in
                     the archive are extracted.
            preserve_ownership: If True, attempts to preserve original ownership.
                                (See `extract` method notes).
            preserve_links: If True, preserves symbolic links. (See `extract`
                            method notes).

        Raises:
            ArchiveError: For archive-related errors during extraction.
            ArchiveMemberNotFoundError: If a specified member in the `members`
                                        list is not found.
        """
        if path is None:
            path = os.getcwd()

        if members is None:
            member_iterator = self.info_iter()
        else:
            # Convert all string members to ArchiveMember objects
            member_infos = []
            for m in members:
                if isinstance(m, str):
                    member_infos.append(self.getinfo(m))
                elif isinstance(m, ArchiveMember):
                    member_infos.append(m)
                else:
                    raise TypeError(
                        "Invalid type in 'members' list. "
                        "Expected str or ArchiveMember."
                    )
            member_iterator = iter(member_infos)

        for member_info in member_iterator:
            try:
                self.extract(
                    member_info,
                    path,
                    preserve_ownership=preserve_ownership,
                    preserve_links=preserve_links,
                )
            except ArchiveError as e:
                logger.error(f"Error extracting {member_info.filename}: {e}")
                # Decide on behavior: continue, or re-raise?
                # For now, log and continue to attempt extracting other files.
                # If a critical error, the underlying extract call would have raised.

    @property
    def comment(self) -> bytes | str | None:
        """The archive comment.

        This attempts to provide the archive-level comment, if one exists and
        is supported by the underlying archive format reader. The type of the
        comment (bytes or str) may vary depending on the archive format and
        its encoding.

        Returns:
            The archive comment as `bytes` or `str`, or `None` if no comment
            is present or supported.
        """
        archive_info = self._reader.get_archive_info()
        if archive_info:
            return archive_info.comment
        return None
