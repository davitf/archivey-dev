import io
import logging
import stat
import subprocess
import threading
import zlib
from datetime import datetime
from typing import IO, Any, Iterable, Iterator, List, Optional

import rarfile

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveError,
)
from archivey.formats import ArchiveFormat
from archivey.io_wrappers import ExceptionTranslatingIO
from archivey.types import ArchiveInfo, ArchiveMember, MemberType
from archivey.utils import bytes_to_str

logger = logging.getLogger(__name__)


_RAR_COMPRESSION_METHODS = {
    0x30: "store",
    0x31: "fastest",
    0x32: "fast",
    0x33: "normal",
    0x34: "good",
    0x35: "best",
}


class BaseRarReader(ArchiveReader):
    """Base class for RAR archive readers.

    Handles common RAR file operations using the `rarfile` library.
    This class is not intended to be used directly but subclassed by
    `RarReader` and `RarStreamReader`.

    Args:
        archive_path: Path to the RAR archive file.
        pwd: Password for encrypted archives. Can be str or bytes.

    Attributes:
        archive_path (str): Path to the archive file.
        _archive (Optional[rarfile.RarFile]): The underlying `rarfile.RarFile` object.
        _members (Optional[List[ArchiveMember]]): Cached list of archive members.
        _format_info (Optional[ArchiveInfo]): Cached archive information.

    Raises:
        ArchiveCorruptedError: If the archive is invalid or not a RAR file.
        ArchiveError: For other RAR-specific errors like needing the first volume.
        ArchiveEncryptedError: If a password is required but not provided, or incorrect.
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        super().__init__(archive_path, ArchiveFormat.RAR, pwd=pwd)
        # self.archive_path is already set by super().__init__
        self._members: Optional[list[ArchiveMember]] = None
        self._format_info: Optional[ArchiveInfo] = None
        self._pwd = pwd # Store for potential re-use or logging

        try:
            self._archive = rarfile.RarFile(self.archive_path, "r")
            if self._pwd:
                self._archive.setpassword(self._pwd)
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(f"Invalid RAR archive {self.archive_path}: {e}") from e
        except rarfile.NotRarFile as e:
            raise ArchiveCorruptedError(f"Not a RAR archive {self.archive_path}: {e}") from e
        except rarfile.NeedFirstVolume as e:
            raise ArchiveError(
                f"Need first volume of multi-volume RAR archive {self.archive_path}: {e}"
            ) from e
        except rarfile.RarWrongPassword as e:
            raise ArchiveEncryptedError(
                f"Wrong password specified for {self.archive_path}"
            ) from e
        except rarfile.PasswordRequired as e: # rarfile specific
             raise ArchiveEncryptedError(
                f"Password required for {self.archive_path}"
            ) from e
        except Exception as e: # Catch other rarfile init errors
            raise ArchiveError(f"Error initializing RarFile for {self.archive_path}: {e}") from e


    def close(self) -> None:
        """Closes the RAR archive and releases resources.

        Safe to call multiple times.
        """
        if hasattr(self, "_archive") and self._archive:
            try:
                self._archive.close()
            except Exception as e: # pragma: no cover
                logger.warning(f"Error closing rarfile for {self.archive_path}: {e}")
            self._archive = None
        self._members = None # Clear cached members on close
        self._format_info = None # Clear cached format info

    def _get_link_target(self, info: rarfile.RarInfo) -> Optional[str]:
        """Attempts to read the target of a symbolic link.

        Args:
            info: The `rarfile.RarInfo` object for the link.

        Returns:
            The link target as a string, or None if it cannot be determined
            (e.g., encrypted link content without password).
        """
        if not info.is_symlink(): # pragma: no cover
            return None

        # For RAR5, file_redir contains link information
        if info.file_redir: # (type, flags, target_path_str)
            return info.file_redir[2] # target_path_str

        # For older RAR or if file_redir is not populated, try reading link content
        if not info.needs_password():
            if self._archive is None: # pragma: no cover
                raise ArchiveError("Archive is closed or not initialized")
            try:
                # Link target is stored as the content of the symlink "file"
                link_content_bytes = self._archive.read(info.filename)
                # Attempt to decode using common encodings, UTF-8 first.
                try:
                    return link_content_bytes.decode("utf-8")
                except UnicodeDecodeError: # pragma: no cover
                    # Fallback to system's default encoding if UTF-8 fails
                    return link_content_bytes.decode(io.DEFAULT_BUFFER_SIZE) # type: ignore
            except Exception as e: # pragma: no cover
                logger.warning(f"Could not read symlink target for {info.filename}: {e}")
                return None # Cannot determine target

        # If the link target itself is encrypted and no password provided, we can't read it.
        logger.warning(f"Symlink target for {info.filename} is encrypted and password not available/incorrect.")
        return None

    def get_members(self) -> List[ArchiveMember]:
        """Retrieves a list of all members in the RAR archive.

        Member information is cached after the first call.

        Returns:
            A list of `ArchiveMember` objects.

        Raises:
            ArchiveError: If the archive is closed or not initialized.
        """
        if self._archive is None: # pragma: no cover
            raise ArchiveError("Archive is closed or not initialized")

        # According to https://documentation.help/WinRAR/HELPArcEncryption.htm :
        # If "Encrypt file names" [i.e. header encryption] option is off,
        # file checksums for encrypted RAR 5.0 files are modified using a
        # special password dependent algorithm. [...] So do not expect checksums
        # for encrypted RAR 5.0 files to match actual CRC32 or BLAKE2 values.
        # If "Encrypt file names" option is on, checksums are stored without modification,
        # because they can be accessed only after providing a valid password.

        archive_info = self.get_archive_info()
        may_have_encrypted_crc = (
            not (archive_info.extra or {}).get("header_encrypted", False)
            and archive_info.version == "5"
        )

        if self._members is None:
            self._members = []
            rarinfos: list[rarfile.RarInfo] = self._archive.infolist()
            for info in rarinfos:
                compression_method = (
                    _RAR_COMPRESSION_METHODS.get(info.compress_type, "unknown")
                    if info.compress_type is not None
                    else None
                )

                encrypted = info.needs_password()
                has_encrypted_crc = encrypted and may_have_encrypted_crc

                member = ArchiveMember(
                    filename=info.filename,
                    size=info.file_size,
                    mtime=datetime(*info.date_time) if info.date_time else None,
                    type=(
                        MemberType.DIR
                        if info.is_dir()
                        else MemberType.FILE
                        if info.is_file()
                        else MemberType.LINK
                        if info.is_symlink()
                        else MemberType.OTHER
                    ),
                    permissions=stat.S_IMODE(info.mode)
                    if hasattr(info, "mode") and isinstance(info.mode, int)
                    else None,
                    crc32=info.CRC if not has_encrypted_crc else None,
                    compression_method=compression_method,
                    comment=info.comment,
                    encrypted=info.needs_password(),
                    extra=None,
                    raw_info=info,
                    link_target=self._get_link_target(info),
                )
                self._members.append(member)

        return self._members

    def iter_members(self) -> Iterator[ArchiveMember]:
        """Iterates over `ArchiveMember` objects in the RAR archive.

        Yields:
            `ArchiveMember` objects.
        """
        return iter(self.get_members())

    def get_archive_info(self) -> ArchiveInfo:
        """Retrieves detailed information about the RAR archive.

        Information is cached after the first call.

        Returns:
            An `ArchiveInfo` object.

        Raises:
            ArchiveError: If the archive is closed or not initialized.
        """
        if self._archive is None: # pragma: no cover
            raise ArchiveError("Archive is closed or not initialized")

        if self._format_info is None:
            rar_version = "Unknown"
            # Attempt to determine RAR version (RAR4 vs RAR5) from magic bytes
            # rarfile library itself doesn't directly expose this easily post-init
            try:
                with open(self.archive_path, "rb") as f:
                    magic = f.read(8)
                    if magic.startswith(b"\x52\x61\x72\x21\x1a\x07\x01\x00"):
                        rar_version = "5" # RAR5
                    elif magic.startswith(b"\x52\x61\x72\x21\x1a\x07\x00"):
                        rar_version = "4" # RAR4
            except IOError as e: # pragma: no cover
                logger.warning(f"Could not read archive file to determine RAR version: {e}")


            has_header_encryption = False
            if hasattr(self._archive, '_file_parser') and self._archive._file_parser is not None:
                # This is an internal API of rarfile, might break with updates
                if hasattr(self._archive._file_parser, 'has_header_encryption'):
                    has_header_encryption = self._archive._file_parser.has_header_encryption() # type: ignore
                elif rar_version == "5" and self._archive.needs_password():
                    # For RAR5, if any password is needed, header encryption is possible
                    # This is an assumption as direct check isn't simple via public API for all cases
                    # `rarfile` handles it internally when listing files if password is set
                    logger.debug("RAR5 archive needs password, header encryption status relies on rarfile internals.")


            archive_comment_bytes = self._archive.comment
            archive_comment = None
            if archive_comment_bytes:
                try:
                    archive_comment = archive_comment_bytes.decode('utf-8')
                except UnicodeDecodeError: # pragma: no cover
                    try:
                        # Try a common fallback encoding if UTF-8 fails
                        archive_comment = archive_comment_bytes.decode('cp437') # Common for ZIP, might apply to RAR comments
                    except UnicodeDecodeError:
                        archive_comment = repr(archive_comment_bytes) # Raw representation

            self._format_info = ArchiveInfo(
                format=self.get_format(), # Should be ArchiveFormat.RAR
                version=rar_version,
                is_solid=self._archive.is_solid(),
                comment=archive_comment,
                extra={
                    "needs_password": self._archive.needs_password(),
                    "header_encrypted": has_header_encryption,
                    # rarfile does not directly expose is_multivolume in a simple boolean way.
                    # It's usually handled by trying to open and catching NeedFirstVolume etc.
                },
            )
        return self._format_info


class RarReader(BaseRarReader):
    """Reader for RAR archives using the `rarfile` library.

    This reader is suitable for most common RAR archive operations, including
    accessing individual files.

    Args:
        archive_path: Path to the RAR archive file.
        pwd: Password for encrypted archives (str or bytes).
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        """Initializes RarReader.

        Args:
            archive_path: Path to the RAR archive.
            pwd: Password for the archive, if encrypted.
        """
        super().__init__(archive_path, pwd=pwd)

    def _exception_translator(self, e: Exception) -> Optional[Exception]:
        """Translates `rarfile` exceptions to `archivey` exceptions for I/O operations."""
        if isinstance(e, rarfile.BadRarFile): # pragma: no cover
            return ArchiveCorruptedError(f"Corrupted RAR file or member in {self.archive_path}: {e}")
        elif isinstance(e, rarfile.RarWrongPassword): # pragma: no cover
            return ArchiveEncryptedError(f"Wrong password for RAR file {self.archive_path}: {e}")
        elif isinstance(e, rarfile.PasswordRequired): # pragma: no cover
            return ArchiveEncryptedError(f"Password required for RAR file {self.archive_path}: {e}")
        return None # pragma: no cover

    def open(
        self, member: ArchiveMember, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        """Opens a member within the RAR archive for reading.

        Args:
            member: The `ArchiveMember` object representing the member to open.
            pwd: Password for decryption. Overrides archive-level password if provided.
                 Can be str or bytes.

        Returns:
            A file-like object (binary I/O stream) for reading the member's content.

        Raises:
            ValueError: If the archive is closed.
            ArchiveCorruptedError: If the member data is corrupted.
            ArchiveEncryptedError: If the member is encrypted and the password is
                                   incorrect or not provided.
            ArchiveError: For other RAR-related errors.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        effective_pwd = pwd if pwd is not None else self._pwd

        try:
            # rarfile.open expects filename (str) and optionally password (str or bytes)
            # The member.filename should be correct as obtained from rarfile.infolist()
            inner_file_obj = self._archive.open(member.filename, pwd=bytes_to_str(effective_pwd))
            return ExceptionTranslatingIO(inner_file_obj, self._exception_translator)  # type: ignore[arg-type]
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(
                f"Error reading member '{member.filename}' from {self.archive_path}: {e}"
            ) from e
        except rarfile.RarWrongPassword as e:
            raise ArchiveEncryptedError(
                f"Wrong password for member '{member.filename}' in {self.archive_path}"
            ) from e
        except rarfile.PasswordRequired as e: # Specific to rarfile
            raise ArchiveEncryptedError(
                f"Password required for member '{member.filename}' in {self.archive_path}"
            ) from e
        except rarfile.Error as e: # Catch-all for other rarfile errors
            raise ArchiveError(
                f"Unknown rarfile error for member '{member.filename}' in {self.archive_path}: {e}"
            ) from e
        except Exception as e: # General exceptions
            raise ArchiveError(
                f"Unexpected error opening member '{member.filename}' in {self.archive_path}: {e}"
            ) from e


class CRCMismatchError(ArchiveCorruptedError):
    """Custom exception for CRC mismatch errors during streaming extraction."""
    def __init__(self, filename: str, expected: int, actual: int):
        """Initializes CRCMismatchError.

        Args:
            filename: Name of the file where CRC mismatch occurred.
            expected: The expected CRC32 checksum.
            actual: The actual calculated CRC32 checksum.
        """
        super().__init__(
            f"CRC mismatch in {filename}: expected {expected:08x}, got {actual:08x}"
        )


class RarStreamMemberFile(io.RawIOBase, IO[bytes]):
    """A file-like object for reading a member from a streaming RAR extraction.

    This class wraps the output stream from an external `unrar` process,
    manages reading the correct number of bytes for a specific member,
    and performs CRC validation if expected CRC is known.

    Args:
        member: The `ArchiveMember` this file object represents.
        shared_stream: The shared I/O stream from the `unrar` process.
        lock: A `threading.Lock` to synchronize access to the shared stream.
    """
    def __init__(
        self, member: ArchiveMember, shared_stream: IO[bytes], lock: threading.Lock
    ):
        super().__init__()
        self._stream = shared_stream
        self._member_filename = member.filename # For logging/error messages
        self._remaining = member.size if member.size is not None else -1 # -1 if size unknown (should not happen for RAR)
        if self._remaining == -1 and not member.is_dir: # Directories might have size 0 or None
             logger.warning(f"Member {self._member_filename} has unknown size, streaming might be unreliable.")

        self._expected_crc = (
            member.crc32 & 0xFFFFFFFF if member.crc32 is not None else None
        )
        self._actual_crc = 0
        self._lock = lock
        self._closed = False
        self._fully_read = (self._remaining == 0)


    def read(self, n: int = -1) -> bytes:
        """Reads up to n bytes from the member stream.

        If n is -1, reads until EOF for this member.

        Returns:
            Bytes read from the stream. An empty bytes object indicates EOF for this member.

        Raises:
            ValueError: If the file is closed or an operation is attempted on a closed file.
            EOFError: If unexpected EOF is encountered from the underlying stream.
            CRCMismatchError: If CRC validation fails after reading the entire member.
        """
        if self._closed: # pragma: no cover
            raise ValueError(f"Cannot read from closed/expired file: {self._member_filename}")
        if self._fully_read : # Already read all expected bytes for this member
            return b""

        with self._lock:
            if self._remaining == 0: # Should be caught by self._fully_read, but as a safeguard
                self._fully_read = True
                self._check_crc() # Final check if somehow missed
                return b""

            # Determine how much to read
            if self._remaining == -1: # Size unknown case
                to_read = n if n != -1 else io.DEFAULT_BUFFER_SIZE
            else:
                to_read = self._remaining if n == -1 else min(self._remaining, n)

            if to_read == 0 and n != 0 : # If remaining is 0, but not fully_read (e.g. initial state for 0-byte file)
                 self._fully_read = True
                 self._check_crc()
                 return b""


            data = self._stream.read(to_read)
            if not data and to_read > 0 : # Underlying stream EOF before member fully read
                # This is an error if we expected more data for *this* member
                if self._remaining > 0 or self._remaining == -1:
                    raise EOFError(f"Unexpected EOF from unrar stream while reading {self._member_filename}. "
                                   f"Expected {self._remaining if self._remaining != -1 else 'more'} bytes, got none.")
            
            if self._remaining != -1:
                self._remaining -= len(data)

            if self._expected_crc is not None:
                self._actual_crc = zlib.crc32(data, self._actual_crc)

            # logger.debug( # Reduced log level for performance
            #     f"Read {len(data)} bytes from {self._member_filename}, {self._remaining if self._remaining != -1 else 'unknown'} remaining"
            # )

            if self._remaining == 0 and self._remaining != -1: # Check if done with this member
                self._fully_read = True
                self._check_crc()
            
            # If size was unknown and read returned less than requested (or empty), assume EOF for this member
            if self._remaining == -1 and (len(data) < to_read or not data):
                self._fully_read = True
                # Cannot reliably CRC check if size was unknown and no explicit EOF marker from unrar per file.
                # This mode is inherently less safe.

            return data

    def _check_crc(self) -> None:
        """Validates CRC if expected CRC is known and member is fully read."""
        if self._expected_crc is None or not self._fully_read: # pragma: no cover
            return
        if (self._actual_crc & 0xFFFFFFFF) != self._expected_crc:
            raise CRCMismatchError(self._member_filename, self._expected_crc, self._actual_crc & 0xFFFFFFFF)

    def readable(self) -> bool:
        """Returns True if the stream is readable."""
        return not self._closed # pragma: no cover

    def writable(self) -> bool:
        """Returns False as stream is read-only."""
        return False # pragma: no cover

    def seekable(self) -> bool:
        """Returns False as stream is not seekable."""
        return False # pragma: no cover

    def write(self, b: Any) -> int: # pragma: no cover
        """Raises io.UnsupportedOperation."""
        raise io.UnsupportedOperation("write")

    def writelines(self, lines: Iterable[Any]) -> None: # pragma: no cover
        """Raises io.UnsupportedOperation."""
        raise io.UnsupportedOperation("writelines")

    def close(self) -> None:
        """Closes the member stream.

        Reads and discards any remaining data for this member to ensure the
        shared stream is positioned correctly for the next member and to
        perform final CRC check.
        """
        if self._closed: # pragma: no cover
            return

        with self._lock:
            try:
                if not self._fully_read and self._remaining != 0:
                    logger.debug(f"Closing {self._member_filename}, draining {self._remaining if self._remaining != -1 else 'remaining'} bytes.")
                    # Drain remaining data for this member
                    while self._remaining > 0 or self._remaining == -1:
                        chunk_size = min(65536, self._remaining) if self._remaining != -1 else 65536
                        if chunk_size == 0 and self._remaining > 0 : # Should not happen if _remaining > 0
                             break
                        chunk = self._stream.read(chunk_size)
                        if not chunk: # EOF from underlying stream
                            if self._remaining > 0 and self._remaining != -1: # pragma: no cover
                                raise EOFError(f"Unexpected EOF from unrar stream while skipping/closing {self._member_filename}")
                            break # Normal EOF if _remaining was -1 or became 0

                        if self._expected_crc is not None:
                            self._actual_crc = zlib.crc32(chunk, self._actual_crc)
                        if self._remaining != -1:
                            self._remaining -= len(chunk)
                        if self._remaining == 0: break
                
                self._fully_read = True # Mark as fully processed
                self._check_crc() # Perform final CRC check
            finally:
                self._closed = True
                # Do not close the shared_stream here, only this member's view of it.
        super().close()


class RarStreamReader(BaseRarReader):
    """Reader for RAR archives using a streaming approach via `unrar` command.

    This reader pipes the output of the `unrar p -inul ...` command, which
    extracts all file contents sequentially to stdout. It is suitable for
    reading members in order, especially for solid archives.

    Warning:
        - Seeking and out-of-order access are not supported.
        - If the archive is not solid and contains files with different passwords
          or some unencrypted files mixed with encrypted ones, this reader might
          misalign data if an incorrect password is initially provided for the
          whole stream. It's best used when a single password applies to all
          relevant parts of the archive or for unencrypted solid archives.
        - Requires the `unrar` command-line utility to be installed and in PATH.

    Args:
        archive_path: Path to the RAR archive file.
        pwd: Password for the archive (str or bytes). This password is used for
             the `unrar` command.
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        """Initializes RarStreamReader.

        Args:
            archive_path: Path to the RAR archive.
            pwd: Password for the archive, if encrypted. Used for `unrar` command.
        """
        super().__init__(archive_path, pwd=pwd) # This will call rarfile.RarFile to get member list
        self._proc: Optional[subprocess.Popen] = None
        self._stream: Optional[IO[bytes]] = None
        self._lock = threading.Lock() # Protects access to shared _stream
        self._active_member_file: Optional[RarStreamMemberFile] = None
        self._current_member_idx = -1 # Index of the member currently being streamed or last streamed
        # self.archive_path is set by super()
        # self._pwd (from BaseRarReader) is used for _open_stream

    def close(self) -> None:
        """Closes the streaming RAR reader and waits for the `unrar` process.

        Ensures that the currently active member file is closed (which drains it)
        and that the underlying stream from `unrar` is closed.
        """
        with self._lock: # Ensure close operations are synchronized
            if self._active_member_file:
                try:
                    self._active_member_file.close()
                except Exception as e: # pragma: no cover
                    logger.warning(f"Error closing active member file for {self.archive_path}: {e}")
                self._active_member_file = None
            
            if self._stream:
                try:
                    self._stream.close()
                except Exception as e: # pragma: no cover
                     logger.warning(f"Error closing unrar stream for {self.archive_path}: {e}")
                self._stream = None

            if self._proc:
                try:
                    self._proc.terminate() # Try to terminate first
                    try:
                        self._proc.wait(timeout=1.0) # Wait with a timeout
                    except subprocess.TimeoutExpired: # pragma: no cover
                        logger.warning(f"unrar process for {self.archive_path} did not terminate gracefully, killing.")
                        self._proc.kill() # Force kill if terminate fails
                        self._proc.wait() # Wait for kill
                except Exception as e: # pragma: no cover
                    logger.error(f"Error managing unrar process for {self.archive_path} on close: {e}")
                self._proc = None
        super().close() # Call BaseRarReader close for rarfile object

    def _get_member_file_for_streaming(self, member: ArchiveMember) -> RarStreamMemberFile:
        """Creates a RarStreamMemberFile for the given member. (Internal)"""
        if self._stream is None: # pragma: no cover
            # This should not happen if open() logic is correct
            raise ArchiveError("Unrar stream not initialized before creating member file.")
        return RarStreamMemberFile(member, self._stream, self._lock)

    def _ensure_stream_opened(self, pwd_override: Optional[str] = None) -> None:
        """Initializes the `unrar` process and stream if not already done."""
        if self._stream is not None:
            return

        # Determine password: override, then instance default from BaseRarReader
        current_pwd = pwd_override if pwd_override is not None else self._pwd
        current_pwd_str = bytes_to_str(current_pwd) if current_pwd is not None else None
        
        # Unrar command: 'unrar p' (print file to stdout)
        # '-inul': disable messages to stderr (important for clean pipe)
        # '-p<pass>': set password. '-p-' to specify empty password.
        cmd = ["unrar", "p", "-inul"]
        if current_pwd_str:
            cmd.append(f"-p{current_pwd_str}")
        else:
            cmd.append("-p-") # Explicitly use empty password if none provided
        cmd.append(self.archive_path)

        logger.info(
            f"Opening RAR archive '{self.archive_path}' for streaming with command: {' '.join(cmd)}"
        )
        try:
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, # Capture stderr to check for errors
                bufsize=io.DEFAULT_BUFFER_SIZE, # Default buffering
            )
        except FileNotFoundError: # pragma: no cover
            raise ArchiveError(
                "unrar command not found. Please ensure it is installed and in your PATH."
            )
        except Exception as e: # pragma: no cover
            raise ArchiveError(f"Failed to start unrar process for {self.archive_path}: {e}") from e

        if self._proc.stdout is None : # pragma: no cover
             # Should be caught by Popen, but as a safeguard
            stderr_output = ""
            if self._proc.stderr:
                stderr_output = self._proc.stderr.read().decode(errors='replace')
            self._proc.wait()
            raise ArchiveError(f"Could not get unrar output stream for {self.archive_path}. Stderr: {stderr_output}")

        self._stream = self._proc.stdout
        
        # Optional: Check stderr for early errors from unrar (e.g. archive not found by unrar)
        # This requires non-blocking read or threading for stderr, which adds complexity.
        # For now, errors are assumed to manifest in the stream or on process exit.


    def open(
        self, member: ArchiveMember, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        """Opens a member for streaming read.

        Members must be opened in the order they appear in the archive.
        Attempting to open a member out of order or re-open a member will
        result in an error.

        Args:
            member: The `ArchiveMember` to open. Must be from `self.infolist()`.
            pwd: Password for decryption. If provided, it must match the
                 password used to initialize the stream (if already initialized).
                 The stream is initialized on the first call to `open`.

        Returns:
            A file-like object (`RarStreamMemberFile`) for reading the member's content.

        Raises:
            ValueError: If archive is closed, member is not found, or access is
                        out of order.
            ArchiveError: If there's an issue with the `unrar` process or stream.
            TypeError: If `pwd` is provided but its type is inconsistent with previous.
        """
        if self._archive is None or self._members is None: # pragma: no cover
             # self._archive is from BaseRarReader, used for get_members initially
            raise ValueError("Archive (metadata part) is closed or not initialized.")

        # Handle password consistency for the stream
        # The stream, once opened with a password, uses that for all subsequent files.
        effective_pwd_str: Optional[str] = None
        if pwd is not None:
            effective_pwd_str = bytes_to_str(pwd)
            if self._pwd is not None and effective_pwd_str != bytes_to_str(self._pwd): # type: ignore
                raise ValueError(
                    "RarStreamReader was initialized with a different password. "
                    "Cannot change password for an active stream."
                )
        elif self._pwd is not None:
             effective_pwd_str = bytes_to_str(self._pwd) # type: ignore


        # Ensure the unrar stream is started (uses effective_pwd_str or None)
        # This lock ensures that _ensure_stream_opened and subsequent critical sections are atomic
        with self._lock:
            self._ensure_stream_opened(effective_pwd_str)

            try:
                target_idx = self._members.index(member)
            except ValueError: # pragma: no cover
                # Should not happen if 'member' is from self.infolist()
                raise ValueError(f"Member '{member.filename}' not found in this archive's member list.")

            if target_idx < self._current_member_idx: # pragma: no cover
                raise ValueError(
                    f"Cannot re-open already processed or skipped member: '{member.filename}'. "
                    "Streaming is forward-only."
                )
            
            if self._active_member_file and target_idx == self._current_member_idx: # pragma: no cover
                # Trying to open the same member that is already active
                # This could be allowed if we return the existing file, but complicates state.
                # For now, treat as error or return existing if it's not closed.
                if not self._active_member_file.closed:
                    logger.warning(f"Re-opening already active member '{member.filename}'. Returning existing stream.")
                    return self._active_member_file
                else: # Should not happen - if closed, _current_member_idx should have advanced or this is an error
                    raise ValueError(f"Cannot re-open closed member '{member.filename}' at same index.")


            # If there's a currently active member file, it must be closed first.
            # Closing it will drain its remaining content from the shared stream.
            if self._active_member_file:
                if not self._active_member_file.closed:
                    logger.debug(f"Closing previous active member '{self._members[self._current_member_idx].filename}' before opening '{member.filename}'.")
                    self._active_member_file.close()
                self._active_member_file = None # Mark as no longer active


            # Drain any intermediate members if we are skipping ahead
            # (e.g., user called open for member 0, then member 3)
            # Start draining from the member *after* the last one processed.
            for i in range(self._current_member_idx + 1, target_idx):
                intermediate_member = self._members[i]
                logger.debug(f"Skipping intermediate member '{intermediate_member.filename}' to reach '{member.filename}'.")
                temp_member_file = self._get_member_file_for_streaming(intermediate_member)
                temp_member_file.close() # Drains the member's data

            # Now, create and return the file object for the requested member
            new_member_file = self._get_member_file_for_streaming(member)
            self._active_member_file = new_member_file
            self._current_member_idx = target_idx
            return new_member_file
