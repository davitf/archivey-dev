import io
import logging
import lzma
from typing import Iterator, List, cast

import py7zr
import py7zr.compressor
import py7zr.exceptions
import py7zr.helpers
from py7zr.py7zr import ArchiveFile

from archivey.base_reader import ArchiveReader
from archivey.exceptions import ArchiveCorruptedError, ArchiveEncryptedError
from archivey.formats import ArchiveFormat
from archivey.types import (
    ArchiveInfo,
    ArchiveMember,
    MemberType,
)
from archivey.utils import bytes_to_str

logger = logging.getLogger(__name__)


class SevenZipReader(ArchiveReader):
    """Reader for 7-Zip archives (`.7z`) using the `py7zr` library.

    This class provides functionality to list members, open individual members
    for reading, and retrieve metadata about the 7-Zip archive.

    Args:
        archive_path: Path to the 7-Zip archive file.
        pwd: Password for encrypted archives. Can be str or bytes.

    Attributes:
        archive_path (str): Path to the archive file.
        _archive (Optional[py7zr.SevenZipFile]): The underlying `py7zr.SevenZipFile` object.
        _members (Optional[List[ArchiveMember]]): Cached list of archive members.
        _format_info (Optional[ArchiveInfo]): Cached archive information.

    Raises:
        ArchiveCorruptedError: If the archive is invalid, corrupted, or not a 7-Zip file.
        ArchiveEncryptedError: If a password is required but not provided, or if
                               the provided password is incorrect (potentially leading
                               to corruption errors during header parsing).
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        """Initializes SevenZipReader.

        Args:
            archive_path: Path to the 7-Zip archive.
            pwd: Password for the archive, if encrypted.
        """
        super().__init__(archive_path, ArchiveFormat.SEVENZIP, pwd=pwd)
        # self.archive_path is set by super()
        self._members: list[ArchiveMember] | None = None
        self._format_info: ArchiveInfo | None = None
        self._pwd = pwd # Store for potential re-use or logging

        try:
            self._archive = py7zr.SevenZipFile(
                self.archive_path, "r", password=bytes_to_str(self._pwd)
            )
        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(f"Invalid 7-Zip archive {self.archive_path}") from e
        except py7zr.PasswordRequired as e: # Indicates header encryption or file encryption detected early
            raise ArchiveEncryptedError(
                f"7-Zip archive {self.archive_path} is encrypted and requires a password for headers."
            ) from e
        except TypeError as e: # py7zr can raise TypeError for certain password/corruption issues
            if "Unknown field" in str(e) or "is not a valid Folder" in str(e):
                # Often indicates wrong password leading to misinterpretation of encrypted headers
                err_msg = f"Corrupted header data or wrong password for {self.archive_path}"
                if self._pwd:
                    err_msg += " (if password protected)."
                raise ArchiveCorruptedError(err_msg) from e
            else: # pragma: no cover
                raise ArchiveError(f"Unexpected TypeError initializing 7ZipFile for {self.archive_path}: {e}") from e
        except EOFError as e: # Can occur with truncated archives
            raise ArchiveCorruptedError(f"Invalid or truncated 7-Zip archive {self.archive_path}") from e
        except lzma.LZMAError as e: # LZMA errors can also indicate corruption or wrong password
            # "Corrupt input data" is a common symptom of wrong password with LZMA-compressed headers
            if "Corrupt input data" in str(e) and self._pwd is not None:
                raise ArchiveEncryptedError(
                    f"Corrupted header data or wrong password for {self.archive_path}"
                ) from e
            else:
                raise ArchiveCorruptedError(
                    f"LZMA decompression error in 7-Zip archive {self.archive_path}: {e}"
                ) from e
        except Exception as e: # Catch other py7zr init errors
            raise ArchiveError(f"Error initializing 7ZipFile for {self.archive_path}: {e}") from e


    def close(self) -> None:
        """Closes the 7-Zip archive and releases any resources.

        Safe to call multiple times.
        """
        if hasattr(self, "_archive") and self._archive:
            try:
                self._archive.close()
            except Exception as e: # pragma: no cover
                logger.warning(f"Error closing SevenZipFile for {self.archive_path}: {e}")
            self._archive = None
        self._members = None
        self._format_info = None

    def _is_member_encrypted(self, file_info: ArchiveFile) -> bool:
        """Checks if a given archive member (file) is encrypted.

        This relies on internal `py7zr` logic to determine if the folder
        associated with the file uses an encryption method.

        Args:
            file_info: The `py7zr.ArchiveFile` object for the member.

        Returns:
            True if the member is likely encrypted, False otherwise.
        """
        if file_info.folder is None: # Should not happen for actual files
            return False
        # `py7zr.compressor.SupportedMethods.needs_password` checks if any coder
        # in the folder's processing chain requires a password.
        return py7zr.compressor.SupportedMethods.needs_password(file_info.folder.coders)

    def get_members(self) -> List[ArchiveMember]:
        """Retrieves a list of all members in the 7-Zip archive.

        Member information is cached after the first call. Symbolic link
        targets are resolved by reading their content if they are encountered.

        Returns:
            A list of `ArchiveMember` objects.

        Raises:
            ValueError: If the archive is closed.
            ArchiveCorruptedError: If reading link targets fails due to archive issues.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            links_to_resolve: dict[str, ArchiveMember] = {}

            # `self._archive.files` provides a list of `ArchiveFile` objects
            for file_info in self._archive.files:
                member_type = MemberType.FILE # Default
                if file_info.is_directory:
                    member_type = MemberType.DIR
                elif file_info.is_symlink:
                    member_type = MemberType.LINK
                elif file_info.is_junction or file_info.is_socket: # py7zr specific types
                    member_type = MemberType.OTHER

                # py7zr's `file.uncompressed` is typed as `List[int]` but is an `int`.
                size = file_info.uncompressed
                if isinstance(size, list): # pragma: no cover
                    logger.warning(f"Unexpected size type (list) for {file_info.filename}, using first element if available.")
                    size = size[0] if size else 0 # type: ignore

                member = ArchiveMember(
                    filename=file_info.filename,
                    size=int(size), # type: ignore
                    mtime=py7zr.helpers.filetime_to_dt(file_info.lastwritetime).replace(tzinfo=None)
                          if file_info.lastwritetime else None,
                    type=member_type,
                    permissions=file_info.posix_mode, # `None` if not available
                    crc32=file_info.crc32, # `None` if not available
                    compression_method=None,  # Not directly exposed by py7zr per file in a simple string form
                    encrypted=self._is_member_encrypted(file_info),
                    raw_info=file_info, # Store the original py7zr.ArchiveFile object
                    link_target=None # To be resolved later if it's a link
                )

                if member.is_link:
                    links_to_resolve[member.filename] = member
                self._members.append(member)

            # Resolve symbolic link targets if any were found
            if links_to_resolve:
                try:
                    self._archive.reset() # Required before certain operations in py7zr
                    # `read()` returns a dict mapping filenames to file-like objects (BytesIO)
                    # The type hint for `read()` in py7zr might be inaccurate.
                    extracted_files_dict = self._archive.read(list(links_to_resolve.keys()))
                    for filename, file_like_obj in extracted_files_dict.items(): # type: ignore
                        if filename in links_to_resolve:
                            link_content_bytes = file_like_obj.read()
                            try:
                                links_to_resolve[filename].link_target = link_content_bytes.decode('utf-8')
                            except UnicodeDecodeError: # pragma: no cover
                                logger.warning(f"Could not decode symlink target for {filename} as UTF-8. Storing raw.")
                                links_to_resolve[filename].link_target = repr(link_content_bytes)
                except Exception as e: # pragma: no cover
                    raise ArchiveCorruptedError(f"Error reading symlink targets from {self.archive_path}: {e}") from e
        return self._members

    def open(self, member: ArchiveMember, *, pwd: str | bytes | None = None) -> io.IOBase:
        """Opens a member within the 7-Zip archive for reading.

        Note: `py7zr` reads the entire member into memory upon calling `read()`.
        The returned object is typically an `io.BytesIO` instance.

        Args:
            member: The `ArchiveMember` object representing the member to open.
            pwd: Password for decryption if the member is encrypted. This is
                 applied by temporarily modifying the internal state of `py7zr`
                 for this operation if the member's folder requires a password.
                 Can be str or bytes.

        Returns:
            A file-like object (typically `io.BytesIO`) containing the member's content.

        Raises:
            ValueError: If the archive is closed.
            ArchiveCorruptedError: If there's an error decompressing or reading the member.
            ArchiveEncryptedError: If the member is encrypted and the password is
                                   incorrect or not provided.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        self._archive.reset()  # py7zr requires reset before some read operations

        file_info = cast(ArchiveFile, member.raw_info)
        previous_folder_password: Optional[str] = None # Store original password of the folder
        folder_modified = False

        try:
            # HACK: py7zr doesn't allow per-file password on open. If a password is
            # provided to this open() method, and the member's folder in py7zr
            # expects a password, we temporarily set it on the folder object.
            # This is an internal manipulation and might be fragile.
            if pwd is not None and file_info.folder is not None:
                # Only override if the folder actually has coders (i.e., is compressed/encrypted)
                # and potentially needs a password.
                if file_info.folder.coders:
                    previous_folder_password = file_info.folder.password
                    file_info.folder.password = bytes_to_str(pwd)
                    folder_modified = True

            # `self._archive.read()` returns a dict: {filename: BytesIO_object}
            # The type hint for `read()` in py7zr might be inaccurate.
            content_dict = self._archive.read([member.filename])
            if member.filename not in content_dict: # pragma: no cover
                 raise ArchiveCorruptedError(f"Member '{member.filename}' not found by py7zr.read despite being in infolist.")
            return content_dict[member.filename]  # type: ignore

        except py7zr.exceptions.ArchiveError as e: # Includes various py7zr issues
            # Check if it's a password issue if a password was involved
            if folder_modified or (self._pwd and member.encrypted):
                 # py7zr might not always raise PasswordRequired if pwd leads to corruption
                if "CRC error" in str(e) or "Data error" in str(e):
                     raise ArchiveEncryptedError(
                        f"Potential wrong password for member '{member.filename}'. Upstream error: {e}"
                    ) from e
            raise ArchiveCorruptedError(f"Error reading member '{member.filename}': {e}") from e
        except py7zr.PasswordRequired as e: # Should ideally be caught if headers were fine
            raise ArchiveEncryptedError(
                f"Password required to read member '{member.filename}'"
            ) from e
        except lzma.LZMAError as e: # Decompression errors
            if folder_modified or (self._pwd and member.encrypted):
                raise ArchiveEncryptedError(
                    f"LZMA error, potential wrong password for member '{member.filename}'. Upstream error: {e}"
                ) from e
            raise ArchiveCorruptedError(
                f"LZMA decompression error for member '{member.filename}': {e}"
            ) from e
        finally:
            # Restore the original password on the folder object if we changed it.
            if folder_modified and file_info.folder is not None:
                file_info.folder.password = previous_folder_password

    def iter_members(self) -> Iterator[ArchiveMember]:
        """Returns an iterator over `ArchiveMember` objects in the archive.

        Yields:
            `ArchiveMember` objects.
        """
        return iter(self.get_members())

    def get_archive_info(self) -> ArchiveInfo:
        """Retrieves detailed information about the 7-Zip archive.

        Information is cached after the first call.

        Returns:
            An `ArchiveInfo` object.

        Raises:
            ValueError: If the archive is closed.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._format_info is None:
            # `archiveinfo()` provides metadata like solid status.
            # `password_protected` attribute indicates if a password was used
            # to open the archive (for header decryption or if any file needs it).
            sevenzip_file_info = self._archive.archiveinfo()
            is_archive_encrypted = self._archive.password_protected() # Actual method call

            self._format_info = ArchiveInfo(
                format=self.get_format(), # Should be ArchiveFormat.SEVENZIP
                version=None, # py7zr doesn't easily expose 7z format version
                is_solid=sevenzip_file_info.solid if sevenzip_file_info else None,
                comment=None, # 7zip standard doesn't have archive global comment in the same way zip/rar do
                extra={
                    "header_encrypted": is_archive_encrypted, # If archive opened with password, headers might have been encrypted
                    "files_encrypted": any(m.encrypted for m in self.get_members()) # Check if any specific file is marked encrypted
                },
            )
        return self._format_info
