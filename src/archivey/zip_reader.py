import logging
import os
import stat
import struct
import zipfile
from datetime import datetime, timezone
from typing import BinaryIO, Iterator, Optional, cast

from archivey.base_reader import (
    BaseArchiveReader,
)
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveError,
)
from archivey.formats import ArchiveFormat
from archivey.io_helpers import ExceptionTranslatingIO
from archivey.types import ArchiveInfo, ArchiveMember, CreateSystem, MemberType
from archivey.utils import decode_bytes_with_fallback, str_to_bytes

# TODO: check if this is correct
_ZIP_ENCODINGS = ["utf-8", "cp437", "cp1252", "latin-1"]

logger = logging.getLogger(__name__)


def get_zipinfo_timestamps(
    zip_info: zipfile.ZipInfo,
) -> tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """
    Get mtime, atime, and ctime from a ZipInfo object, handling extended timestamp fields.
    Returns a tuple (mtime, atime, ctime).
    """
    mtime: Optional[datetime] = datetime(*zip_info.date_time)
    atime: Optional[datetime] = None
    ctime: Optional[datetime] = None

    if not zip_info.extra:
        return mtime, atime, ctime

    # Parse extended timestamp extra field (0x5455)
    # Structure: Header ID (2 bytes), Data Size (2 bytes), Flags (1 byte), [ModTime (4 bytes)], [AccessTime (4 bytes)], [CreateTime (4 bytes)]
    pos = 0
    while pos < len(zip_info.extra):
        if len(zip_info.extra) - pos < 4:  # Need at least 2 bytes for ID and 2 for size
            break
        field_id, field_size = struct.unpack("<HH", zip_info.extra[pos : pos + 4])
        pos += 4

        if field_id == 0x5455:  # Extended Timestamp (UT)
            if field_size < 1: # Must have at least Flags byte
                pos += field_size
                continue

            flags = zip_info.extra[pos]
            current_ts_pos = pos + 1 # Current position within this field's data for timestamps

            # Bit 0: Modification Time
            if flags & 0x01:
                if field_size >= current_ts_pos - pos + 4:
                    ts_val = int.from_bytes(
                        zip_info.extra[current_ts_pos : current_ts_pos + 4], "little"
                    )
                    if ts_val > 0:
                        mtime = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                    current_ts_pos += 4
                else: # Not enough bytes for mtime, skip further parsing of this field
                    logger.warning("Extended timestamp field 0x5455: Not enough data for mtime.")
                    pos += field_size
                    continue


            # Bit 1: Access Time
            if flags & 0x02:
                if field_size >= current_ts_pos - pos + 4:
                    ts_val = int.from_bytes(
                        zip_info.extra[current_ts_pos : current_ts_pos + 4], "little"
                    )
                    if ts_val > 0:
                        atime = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                    current_ts_pos += 4
                else: # Not enough bytes for atime
                    logger.warning("Extended timestamp field 0x5455: Not enough data for atime.")
                    pos += field_size
                    continue


            # Bit 2: Creation Time
            if flags & 0x04: # Bit 2 for CreateTime
                if field_size >= current_ts_pos - pos + 4:
                    ts_val = int.from_bytes(
                        zip_info.extra[current_ts_pos : current_ts_pos + 4], "little"
                    )
                    if ts_val > 0:
                        ctime = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                    # current_ts_pos += 4 # No need to advance, it's the last one we parse
                else: # Not enough bytes for ctime
                    logger.warning("Extended timestamp field 0x5455: Not enough data for ctime.")
                    pos += field_size
                    continue

            # Successfully parsed this field, or parts of it.
            # The outer loop's pos update will handle moving to the next field.

        pos += field_size # Move to the next extra field header

    return mtime, atime, ctime


class ZipReader(BaseArchiveReader):
    """Reader for ZIP archives."""

    def __init__(
        self,
        archive_path: BinaryIO | str | bytes | os.PathLike,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ):
        super().__init__(
            ArchiveFormat.ZIP,
            archive_path,
            members_list_supported=True,
            random_access_supported=not streaming_only,
            pwd=pwd,
        )
        self._format_info: ArchiveInfo | None = None
        try:
            # The typeshed definition of ZipFile is incorrect, it should allow byte streams.
            self._archive = zipfile.ZipFile(archive_path, "r")  # type: ignore
        except zipfile.BadZipFile as e:
            raise ArchiveCorruptedError(f"Invalid ZIP archive {archive_path}") from e

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._format_info is None:
            self._format_info = ArchiveInfo(
                format=self.format,
                is_solid=False,  # ZIP archives are never solid
                comment=decode_bytes_with_fallback(
                    self._archive.comment, _ZIP_ENCODINGS
                )
                if self._archive.comment
                else None,
                extra={
                    "is_encrypted": any(
                        info.flag_bits & 0x1 for info in self._archive.infolist()
                    ),
                    # "zip_version": self._archive.version,
                },
            )
        return self._format_info

    def _read_link_target(self, info: zipfile.ZipInfo) -> str | None:
        if self._archive is None:
            raise ValueError("Archive is closed")

        # Zip archives store the link target as the contents of the file.
        # TODO: do we need to handle the UTF8 flag or fallback encodings?
        if stat.S_ISLNK(info.external_attr >> 16):
            with self._archive.open(info.filename) as f:
                return f.read().decode("utf-8")
        return None

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        assert self._archive is not None

        for info in self._archive.infolist():
            is_dir = info.is_dir()
            compression_method = (
                {
                    zipfile.ZIP_STORED: "store",
                    zipfile.ZIP_DEFLATED: "deflate",
                    zipfile.ZIP_BZIP2: "bzip2",
                    zipfile.ZIP_LZMA: "lzma",
                }.get(info.compress_type, "unknown")
                if hasattr(info, "compress_type")
                else None
            )

            mode = info.external_attr >> 16
            is_link = stat.S_ISLNK(mode)

            mtime, atime, ctime = get_zipinfo_timestamps(info)

            member = ArchiveMember(
                filename=info.filename,
                file_size=info.file_size,
                compress_size=info.compress_size,
                mtime_with_tz=mtime,
                atime_with_tz=atime,
                ctime_with_tz=ctime,
                type=MemberType.DIR
                if is_dir
                else MemberType.SYMLINK
                if is_link
                else MemberType.FILE,
                mode=stat.S_IMODE(info.external_attr >> 16)
                if hasattr(info, "external_attr") # external_attr = 0 is valid (mode 0)
                else None,
                crc32=info.CRC if hasattr(info, "CRC") else None,
                compression_method=compression_method,
                comment=decode_bytes_with_fallback(info.comment, _ZIP_ENCODINGS)
                if info.comment
                else None,
                encrypted=bool(info.flag_bits & 0x1),
                create_system=CreateSystem(info.create_system)
                if hasattr(info, "create_system") and info.create_system in CreateSystem._value2member_map_
                else CreateSystem.UNKNOWN,
                extra={
                    "compress_type": info.compress_type if hasattr(info, "compress_type") else None,
                    "compress_size": info.compress_size, # Already used
                    "original_create_system": info.create_system if hasattr(info, "create_system") else None,
                    "create_version": info.create_version if hasattr(info, "create_version") else None,
                    "extract_version": info.extract_version if hasattr(info, "extract_version") else None,
                    "flag_bits": info.flag_bits if hasattr(info, "flag_bits") else None,
                    "volume": info.volume if hasattr(info, "volume") else None,
                    # Store raw extra field for debugging or further parsing if needed
                    "raw_extra": info.extra if info.extra else None,
                },
                raw_info=info,
                link_target=self._read_link_target(info),
            )

            # Parse UID/GID from Info-ZIP Unix extra fields
            if info.extra:
                extra_pos = 0
                uid_parsed = False
                gid_parsed = False
                while extra_pos < len(info.extra):
                    if len(info.extra) - extra_pos < 4: # Need header ID and size
                        break
                    field_id, field_size = struct.unpack("<HH", info.extra[extra_pos : extra_pos + 4])
                    extra_pos += 4
                    field_data = info.extra[extra_pos : extra_pos + field_size]
                    extra_pos += field_size

                    if field_id == 0x7875:  # Info-ZIP UNIX new
                        # Ver (1 byte), UIDSize (1 byte), UID (UIDSize bytes), GIDSize (1 byte), GID (GIDSize bytes)
                        if len(field_data) >= 2: # Min Ver + UIDSize
                            ver = field_data[0]
                            if ver == 1:
                                uid_size = field_data[1]
                                if len(field_data) >= 2 + uid_size + 1: # Ver, UIDSize, UID, GIDSize
                                    uid_bytes = field_data[2 : 2 + uid_size]
                                    gid_size_pos = 2 + uid_size
                                    gid_size = field_data[gid_size_pos]
                                    if len(field_data) >= gid_size_pos + 1 + gid_size: # Ver, UIDSize, UID, GIDSize, GID
                                        gid_bytes = field_data[gid_size_pos + 1 : gid_size_pos + 1 + gid_size]
                                        try:
                                            member.uid = int.from_bytes(uid_bytes, "little")
                                            member.gid = int.from_bytes(gid_bytes, "little")
                                            uid_parsed = True
                                            gid_parsed = True
                                            if member.create_system == CreateSystem.UNKNOWN: # Don't override NTFS etc.
                                                member.create_system = CreateSystem.UNIX
                                        except ValueError:
                                            logger.warning(f"Could not parse UID/GID from 0x7875 field: {field_data}")
                                        break # Found and processed 0x7875, assume it's authoritative for UID/GID
                    elif field_id == 0x5855 and not (uid_parsed and gid_parsed): # Info-ZIP UNIX (old) - lower priority
                        # This field can be ambiguous. Standard defines it for Atime/Mtime/UID/GID.
                        # Common usage for UID/GID: 16-bit UID, 16-bit GID (4 bytes total) or 32-bit UID, 32-bit GID (8 bytes total)
                        # We are only interested if it's for UID/GID and they are not already parsed.
                        # If it contains timestamps, get_zipinfo_timestamps should handle it via 0x5455 'UT' field.
                        # This 0x5855 'UX' field is different.
                        # For simplicity, we'll only attempt to parse if it looks like UID/GID based on size.
                        # This is a heuristic and might be incorrect if 0x5855 is used for other purposes.
                        if field_size == 4 and not uid_parsed: # Potentially 16-bit UID and GID
                            try:
                                member.uid = int.from_bytes(field_data[0:2], "little")
                                member.gid = int.from_bytes(field_data[2:4], "little")
                                if member.create_system == CreateSystem.UNKNOWN:
                                    member.create_system = CreateSystem.UNIX
                            except ValueError:
                                logger.warning(f"Could not parse 16-bit UID/GID from 0x5855 field: {field_data}")
                        elif field_size == 8 and not uid_parsed: # Potentially 32-bit UID and GID
                            try:
                                member.uid = int.from_bytes(field_data[0:4], "little")
                                member.gid = int.from_bytes(field_data[4:8], "little")
                                if member.create_system == CreateSystem.UNKNOWN:
                                    member.create_system = CreateSystem.UNIX
                            except ValueError:
                                logger.warning(f"Could not parse 32-bit UID/GID from 0x5855 field: {field_data}")
                        # If we parsed something from 0x5855, we can break or continue if other fields might exist.
                        # For now, let's assume if we find a plausible UID/GID here, we're done for these fields.
                        if member.uid is not None and member.gid is not None:
                             break


            yield member

    def open(
        self,
        member_or_filename: ArchiveMember | str,
        *,
        pwd: Optional[bytes | str] = None,
    ) -> BinaryIO:
        if self._archive is None:
            raise ValueError("Archive is closed")

        member, filename = self._resolve_member_to_open(member_or_filename)

        try:
            stream = self._archive.open(
                cast(zipfile.ZipInfo, member.raw_info),
                pwd=str_to_bytes(
                    pwd if pwd is not None else self.get_archive_password()
                ),
            )

            return ExceptionTranslatingIO(
                cast(BinaryIO, stream),
                lambda e: ArchiveCorruptedError(f"Error reading member {filename}: {e}")
                if isinstance(e, zipfile.BadZipFile)
                else None,
            )
        except RuntimeError as e:
            if "password required" in str(e):
                raise ArchiveEncryptedError(f"Member {filename} is encrypted") from e
            raise ArchiveError(f"Error reading member {filename}: {e}") from e
        except zipfile.BadZipFile as e:
            raise ArchiveCorruptedError(f"Error reading member {filename}: {e}") from e

    @classmethod
    def is_zip_file(cls, file: BinaryIO | str | os.PathLike) -> bool:
        return zipfile.is_zipfile(file)

    def test_member(self, member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[bytes|str] = None) -> bool:
        """
        Test the integrity of a ZIP archive member.

        For non-encrypted files, this method relies on the CRC check performed
        by `zipfile` when the member's stream is opened via `self.open()` and fully
        read/closed. For encrypted files, successful decryption and reading of
        the entire stream is considered a successful test.

        Args:
            member_or_filename: The ArchiveMember object or the filename (str)
                of the member to test.
            pwd: Optional password for decrypting the member if it's encrypted.

        Returns:
            True if the member is valid, False if a CRC error (BadZipFile) or
            other ArchiveCorruptedError occurs.

        Raises:
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveEncryptedError: If the member is encrypted and `pwd` is incorrect
                                   or not provided.
            ArchiveError: For other archive-related errors during testing.
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        member_obj, filename = self._resolve_member_to_open(member_or_filename)
        # zip_info = cast(zipfile.ZipInfo, member_obj.raw_info) # Not strictly needed for this logic

        # For directories or symlinks, ZipFile.testzip() doesn't apply. Assume valid.
        if not member_obj.is_file:
            return True

        try:
            # ZipFile.testzip() checks CRC for the given member name.
            # It returns None if OK, or the filename if CRC error.
            # We need to handle the password if the file is encrypted.
            # The `open` method within `_archive.open` (used by `extractfile`)
            # handles password decryption before data is passed to CRC check.
            # Unfortunately, testzip() itself doesn't take a pwd.
            # A common workaround is to read the file if encrypted or if testzip is problematic.

            # The most reliable way without re-implementing CRC logic,
            # and to handle encryption correctly, is to read the stream.
            # zipfile.ZipFile.open() performs CRC check on the stream's close() method
            # if the file is not encrypted with a known-bad password.
            # If encrypted, successful decryption and read is the test.
            with self.open(member_obj, pwd=pwd) as stream:
                while stream.read(65536): # Read in chunks
                    pass
            return True # If open/read/close (with CRC check) succeeds.
        except zipfile.BadZipFile: # This is raised by zipfile on CRC error during open/read/close
            return False
        except RuntimeError as e: # For password errors during open
            if "password required" in str(e).lower() or "bad password" in str(e).lower():
                # Raising ArchiveEncryptedError as per method contract
                raise ArchiveEncryptedError(f"Password error for member {filename}: {e}") from e
            # Other RuntimeErrors should be wrapped
            raise ArchiveError(f"Error testing member {filename}: {e}") from e
        except ArchiveCorruptedError: # From our own open() if it wraps a BadZipFile or other issues
            return False
        # ArchiveEncryptedError from self.open() should propagate as per contract
