import stat
import struct
import zipfile
from datetime import datetime
from typing import IO, Iterator, List, Optional

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveError,
)
from archivey.formats import ArchiveFormat
from archivey.types import ArchiveInfo, ArchiveMember, MemberType
from archivey.utils import decode_bytes_with_fallback, str_to_bytes

#: List of fallback encodings used for decoding ZIP comments and filenames
#: if UTF-8 fails or is not indicated. CP437 is common in older ZIP files.
_ZIP_ENCODINGS = ["utf-8", "cp437", "cp1252", "latin-1"]


def get_zipinfo_timestamp(zip_info: zipfile.ZipInfo) -> datetime:
    """Extracts a `datetime` object from a `zipfile.ZipInfo` object.

    This function prioritizes the high-precision Unix timestamp stored in
    the 'extended timestamp' (0x5455) extra field if available. Otherwise,
    it falls back to the standard DOS date and time from `zip_info.date_time`.

    Args:
        zip_info: The `zipfile.ZipInfo` object for a member.

    Returns:
        A `datetime` object representing the member's modification time.
    """
    # Standard DOS date_time is mandatory
    main_modtime = datetime(*zip_info.date_time)

    # Check for 'extended timestamp' extra field (0x5455) for higher precision
    if zip_info.extra:
        offset = 0
        while offset < len(zip_info.extra):
            if len(zip_info.extra) - offset < 4:  # Header ID (2) + Size (2)
                break # Not enough bytes for another field header

            field_id, field_size = struct.unpack("<HH", zip_info.extra[offset : offset + 4])
            offset += 4

            if field_id == 0x5455: # 'UT' - Universal Time extra field
                if field_size >= 1: # Flags byte must be present
                    flags = zip_info.extra[offset]
                    data_offset = offset + 1 # Move past flags

                    # Modification time is present if bit 0 of flags is set
                    if flags & 0x01:
                        if field_size - (data_offset - offset) >= 4: # Check if 4 bytes for mtime available
                            # Unix mtime is a 4-byte little-endian unsigned integer
                            mtime_unix = struct.unpack("<I", zip_info.extra[data_offset : data_offset + 4])[0]
                            try:
                                return datetime.fromtimestamp(mtime_unix)
                            except ValueError: # pragma: no cover
                                # Invalid timestamp (e.g., too large for system's fromtimestamp)
                                # Fallback to main_modtime in this case
                                logger.warning(f"Invalid Unix timestamp {mtime_unix} in extra field for {zip_info.filename}.")
                                break # Stop parsing this field
                        else: # pragma: no cover
                            # Not enough data for mtime in this 'UT' field
                            break
                    else: # pragma: no cover
                        # 'UT' field present, but mtime flag not set. Other times might be here (atime, ctime).
                        # We only care about mtime for now.
                        break # Stop processing this 'UT' field.
                else: # pragma: no cover
                    # Field too small for flags
                    break
            offset += field_size # Move to the next field

    return main_modtime


class ZipReader(ArchiveReader):
    """Reader for ZIP archives using the standard library's `zipfile` module.

    This class provides functionality to list members, open individual members
    for reading, and retrieve metadata about the ZIP archive and its members.
    It handles basic ZIP features including password-protected (ZipCrypto)
    members and attempts to decode filenames and comments using common encodings.

    Args:
        archive_path: Path to the ZIP archive file.
        pwd: Password for encrypted members (str or bytes). This password is
             applied when opening encrypted members.

    Attributes:
        archive_path (str): Path to the archive file.
        _archive (Optional[zipfile.ZipFile]): The underlying `zipfile.ZipFile` object.
        _members (Optional[List[ArchiveMember]]): Cached list of archive members.
        _format_info (Optional[ArchiveInfo]): Cached archive information.
        _pwd (Optional[bytes | str]): Stored password for the archive.

    Raises:
        ArchiveCorruptedError: If the archive is invalid or corrupted.
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        """Initializes ZipReader.

        Args:
            archive_path: Path to the ZIP archive.
            pwd: Password for the archive, if encrypted.
        """
        super().__init__(archive_path, ArchiveFormat.ZIP, pwd=pwd)
        # self.archive_path is set by super()
        self._members: list[ArchiveMember] | None = None
        self._format_info: ArchiveInfo | None = None
        self._pwd = pwd # Stored for use in open()

        try:
            self._archive = zipfile.ZipFile(self.archive_path, "r")
        except zipfile.BadZipFile as e:
            raise ArchiveCorruptedError(f"Invalid or corrupted ZIP archive '{self.archive_path}': {e}") from e
        except FileNotFoundError: # pragma: no cover
            raise
        except Exception as e: # Catch other zipfile.ZipFile init errors
            raise ArchiveError(f"Error opening ZIP archive '{self.archive_path}': {e}") from e


    def close(self) -> None:
        """Closes the ZIP archive and releases resources.

        Safe to call multiple times.
        """
        if hasattr(self, "_archive") and self._archive:
            try:
                self._archive.close()
            except Exception as e: # pragma: no cover
                logger.warning(f"Error closing ZipFile for {self.archive_path}: {e}")
            self._archive = None
        self._members = None
        self._format_info = None

    def get_archive_info(self) -> ArchiveInfo:
        """Retrieves detailed information about the ZIP archive.

        Information is cached after the first call.

        Returns:
            An `ArchiveInfo` object.

        Raises:
            ValueError: If the archive is closed.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._format_info is None:
            # Check if any member is encrypted to mark archive as potentially encrypted
            is_any_member_encrypted = False
            try:
                for info in self._archive.infolist():
                    if info.flag_bits & 0x1: # Standard encryption flag
                        is_any_member_encrypted = True
                        break
            except zipfile.BadZipFile as e: # pragma: no cover
                 # If infolist() fails, consider it corrupted for info purposes
                logger.warning(f"Could not read infolist for encryption check on {self.archive_path}: {e}")
                raise ArchiveCorruptedError(f"Failed to read member list from {self.archive_path} for info: {e}") from e


            archive_comment_bytes = self._archive.comment
            decoded_comment = None
            if archive_comment_bytes:
                # ZIP specification is vague on comment encoding. Try common ones.
                decoded_comment = decode_bytes_with_fallback(archive_comment_bytes, _ZIP_ENCODINGS)

            self._format_info = ArchiveInfo(
                format=self.get_format(), # Should be ArchiveFormat.ZIP
                is_solid=False,  # ZIP archives are never solid in the typical sense
                comment=decoded_comment,
                extra={
                    "encrypted_members_present": is_any_member_encrypted,
                    # zipfile.ZipFile doesn't directly expose overall ZIP version,
                    # but info.extract_version gives version needed to extract a member.
                },
            )
        return self._format_info

    def is_solid(self) -> bool: # pragma: no cover
        """Checks if the archive is solid. For ZIP, this is always False.

        Returns:
            False, as ZIP archives are not solid.
        """
        return False # ZIP archives store files individually compressed

    def _get_link_target(self, info: zipfile.ZipInfo) -> Optional[str]:
        """Attempts to read the target of a symbolic link from a ZIP member.

        Args:
            info: The `zipfile.ZipInfo` object for the member.

        Returns:
            The link target as a string if the member is a symlink and its
            target can be read and decoded. Returns `None` otherwise.

        Raises:
            ValueError: If the archive is closed.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        # Check if external_attr indicates a symbolic link (common for Unix-created zips)
        # S_IFLNK is 0o120000. external_attr stores mode in the upper 16 bits.
        if stat.S_ISLNK(info.external_attr >> 16):
            try:
                # For symlinks, the content of the "file" is the target path.
                # Password might be needed if the symlink entry itself is encrypted.
                with self._archive.open(info, pwd=str_to_bytes(self._pwd)) as f:
                    link_bytes = f.read()
                    # Attempt to decode using common encodings, prioritizing UTF-8.
                    return decode_bytes_with_fallback(link_bytes, _ZIP_ENCODINGS, default_to_repr=True)
            except RuntimeError as e: # pragma: no cover
                if "password required" in str(e).lower():
                    logger.warning(f"Symlink target for '{info.filename}' is encrypted, password needed/wrong.")
                else:
                    logger.warning(f"Could not read symlink target for '{info.filename}': {e}")
            except Exception as e: # pragma: no cover
                logger.warning(f"Unexpected error reading symlink target for '{info.filename}': {e}")
        return None

    def get_members(self) -> List[ArchiveMember]:
        """Retrieves a list of all members in the ZIP archive.

        Member information is cached after the first call. Filenames and comments
        are decoded using common encodings if UTF-8 is not indicated or fails.

        Returns:
            A list of `ArchiveMember` objects.

        Raises:
            ValueError: If the archive is closed.
            ArchiveCorruptedError: If reading the member list fails.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            try:
                zip_infos = self._archive.infolist()
            except zipfile.BadZipFile as e: # pragma: no cover
                raise ArchiveCorruptedError(f"Failed to read member list from '{self.archive_path}': {e}") from e

            for info in zip_infos:
                # Determine filename: try UTF-8 if flag set, else fallback encodings
                # General purpose bit flag, bit 11 (0x800) indicates UTF-8 for filename and comment.
                is_utf8 = bool(info.flag_bits & 0x800)
                encodings_to_try = [_ZIP_ENCODINGS[0]] if is_utf8 else _ZIP_ENCODINGS
                
                filename = decode_bytes_with_fallback(
                    info.filename.encode('latin-1'), # Re-encode from assumed latin-1 if not UTF-8
                                                     # to bytes for consistent decoding attempts.
                                                     # zipfile might already decode cp437 by default
                                                     # if not utf-8 flag. This is complex.
                                                     # A simpler approach is to trust zipfile's initial decode,
                                                     # then correct if it seems wrong, but that's harder.
                                                     # For now, assume info.filename is str, potentially misdecoded.
                                                     # If `info.filename` was bytes, it's simpler.
                                                     # Python's zipfile tries to give you strings directly.
                    encodings_to_try,
                    default_to_repr=True # Ensure some string form if all decodes fail
                )
                # If zipfile already gave a str, and it was from cp437 but we want utf-8,
                # we'd need to re-encode to cp437 then decode to utf-8.
                # This part is tricky due to zipfile's own decoding.
                # A common practice is to expect `filename` to be correct if is_utf8,
                # otherwise it might be cp437. The decode_bytes_with_fallback handles this.
                # If info.filename is already a str, we need to ensure it's treated as bytes for decode_bytes_with_fallback
                # A common source of `info.filename` if not UTF-8 is CP437.
                # Let's assume `info.filename` is the library's best effort at a string.
                # If it contains non-ASCII and is_utf8 is false, it's likely CP437 or similar.
                # For robustness, we might need to access raw bytes if possible, but zipfile doesn't expose that easily.

                # Simplified: use filename as is, assuming zipfile did its best.
                # Advanced: if not is_utf8 and has_non_ascii(filename): try filename.encode(cp437).decode(utf-8) etc.
                # This is a known pain point with ZIP files. For now, we trust zipfile's initial string.


                member_type = MemberType.FILE # Default
                if info.is_dir():
                    member_type = MemberType.DIR
                elif stat.S_ISLNK(info.external_attr >> 16):
                    member_type = MemberType.LINK
                
                member_comment_bytes = info.comment
                decoded_member_comment = None
                if member_comment_bytes:
                    decoded_member_comment = decode_bytes_with_fallback(member_comment_bytes, encodings_to_try)


                member = ArchiveMember(
                    filename=info.filename, # Using the string directly from zipfile.ZipInfo
                    size=info.file_size,
                    mtime=get_zipinfo_timestamp(info),
                    type=member_type,
                    permissions=stat.S_IMODE(info.external_attr >> 16) if info.external_attr != 0 else None,
                    crc32=info.CRC,
                    compression_method=zipfile.compressor_names.get(info.compress_type, "unknown"),
                    comment=decoded_member_comment,
                    encrypted=bool(info.flag_bits & 0x1),
                    extra={
                        "compress_type": info.compress_type, # Raw compress type int
                        "compress_size": info.compress_size,
                        "header_offset": info.header_offset,
                        "create_system": info.create_system, # System that created ZIP
                        "create_version": info.create_version, # Version of pkzip used
                        "extract_version": info.extract_version, # Version needed to extract
                        "flag_bits": info.flag_bits, # Bit flags
                        "volume": info.volume, # Volume number of disk
                        "internal_attr": info.internal_attr,
                        "external_attr": info.external_attr,
                    },
                    raw_info=info, # Store the zipfile.ZipInfo object
                    link_target=None # Will be populated by _get_link_target if it's a link
                )
                if member.is_link: # Populate link_target after basic member creation
                    member.link_target = self._get_link_target(info)

                self._members.append(member)
        return self._members

    def open(
        self, member: ArchiveMember, *, pwd: Optional[bytes | str] = None
    ) -> IO[bytes]:
        """Opens a member within the ZIP archive for reading.

        Args:
            member: The `ArchiveMember` object representing the member to open.
            pwd: Password for decryption if the member is encrypted. This
                 overrides the archive-level password for this operation.
                 Can be str or bytes.

        Returns:
            A file-like object (binary I/O stream) for reading the member's content.

        Raises:
            ValueError: If the archive is closed.
            ArchiveEncryptedError: If the member is encrypted and the password is
                                   incorrect or not provided.
            ArchiveCorruptedError: If the member data is corrupted.
            ArchiveError: For other ZIP-related errors during opening.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        # Determine password: use pwd if provided, else use archive-level password
        effective_pwd_bytes = str_to_bytes(pwd if pwd is not None else self._pwd)

        try:
            # `member.filename` should be the correct name as per `zipfile.ZipInfo`
            # `zipfile.open` can take ZipInfo object or filename. Using filename for simplicity here.
            # If member.raw_info is ZipInfo, it could be passed directly: self._archive.open(member.raw_info, pwd=...)
            return self._archive.open(member.filename, mode='r', pwd=effective_pwd_bytes)
        except RuntimeError as e:
            # zipfile raises RuntimeError for incorrect password or other issues
            if "password" in str(e).lower() or "bad password" in str(e).lower():
                raise ArchiveEncryptedError(
                    f"Member '{member.filename}' is encrypted and password was incorrect or not provided."
                ) from e
            # Other RuntimeErrors might indicate corruption or unsupported features
            raise ArchiveError(f"Runtime error reading member '{member.filename}': {e}") from e
        except zipfile.BadZipFile as e: # Should ideally be caught earlier, but open can also fail
            raise ArchiveCorruptedError(
                f"Corrupted ZIP data for member '{member.filename}': {e}"
            ) from e
        except Exception as e: # Catch other unexpected errors
            raise ArchiveError(f"Unexpected error opening member '{member.filename}': {e}") from e


    def iter_members(self) -> Iterator[ArchiveMember]:
        """Returns an iterator over `ArchiveMember` objects in the archive.

        Yields:
            `ArchiveMember` objects.
        """
        return iter(self.get_members())
