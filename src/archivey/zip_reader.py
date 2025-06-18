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


def get_zipinfo_timestamp(zip_info: zipfile.ZipInfo) -> Optional[datetime]:
    """Get the timestamp from a ZipInfo object, handling extended timestamp fields.

    Returns:
        A datetime object (UTC-aware if from extended field, naive if from main field)
        or None if no valid timestamp.
    """
    main_modtime: Optional[datetime] = None
    if zip_info.date_time and len(zip_info.date_time) == 6:
        try:
            main_modtime = datetime(*zip_info.date_time)
        except ValueError: # Handles cases like date_time=(0,0,0,0,0,0) which is invalid
            logger.warning(f"Invalid main date_time for {zip_info.filename}: {zip_info.date_time}")
            main_modtime = None

    if not zip_info.extra:
        return main_modtime

    # Parse extended timestamp extra field (0x5455)
    pos = 0
    while pos < len(zip_info.extra):
        # Need at least 4 bytes for tag and length
        if len(zip_info.extra) - pos < 4:
            break

        tp, ln = struct.unpack("<HH", zip_info.extra[pos : pos + 4])
        pos += 4

        if tp == 0x5455:  # Extended Timestamp (UT)
            if pos + ln > len(zip_info.extra):  # Check if data is complete
                break

            field_data = zip_info.extra[pos : pos + ln]
            field_pos = 0

            if field_pos + 1 > len(field_data):  # Need at least 1 byte for flags
                break
            flags = field_data[field_pos]
            field_pos += 1

            # Check if modification time is present (bit 0 of flags)
            if flags & 0x01:
                if field_pos + 4 > len(field_data):  # Need 4 bytes for mtime
                    break
                # Read modification time (4 bytes, Unix timestamp)
                mod_time_unix = int.from_bytes(
                    field_data[field_pos : field_pos + 4], "little"
                )
                field_pos += 4

                # Convert to datetime
                if mod_time_unix > 0:
                    try:
                        # This will be a UTC-aware datetime object
                        extra_modtime_utc = datetime.fromtimestamp(
                            mod_time_unix, tz=timezone.utc
                        )
                        logger.debug(
                            f"Modtime from UT extra field for {zip_info.filename}: "
                            f"{extra_modtime_utc} (raw_unix={mod_time_unix})"
                        )
                        return extra_modtime_utc  # Return UTC-aware datetime
                    except (OSError, ValueError) as e:
                        logger.warning(
                            f"Invalid Unix timestamp {mod_time_unix} in UT extra field for {zip_info.filename}: {e}"
                        )
            # No need to parse other times (access, create) for now
            # Fall through to return main_modtime if mtime not in UT field or invalid
            break  # Only parse the first UT field if multiple exist (should not happen)

        # Skip this field: data_size
        pos += ln

    logger.debug(
        f"Modtime from main date_time for {zip_info.filename}: {main_modtime}"
    )
    return main_modtime  # Return naive main_modtime


class ZipReader(BaseArchiveReader):
    """Reader for ZIP archives."""

    def __init__(
        self,
        archive_path: str | bytes | os.PathLike,
        *,
        pwd: bytes | str | None = None,
    ):
        super().__init__(
            ArchiveFormat.ZIP,
            archive_path,
            random_access_supported=True,
            members_list_supported=True,
            pwd=pwd,
        )
        self._format_info: ArchiveInfo | None = None
        try:
            self._archive = zipfile.ZipFile(self.archive_path, "r")
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

            timestamp = get_zipinfo_timestamp(info)
            member = ArchiveMember(
                filename=info.filename,
                file_size=info.file_size,
                compress_size=info.compress_size,
                mtime=timestamp,
                type=MemberType.DIR
                if is_dir
                else MemberType.SYMLINK
                if is_link
                else MemberType.FILE,
                mode=stat.S_IMODE(info.external_attr >> 16)
                if hasattr(info, "external_attr") and info.external_attr != 0
                else None,
                crc32=info.CRC if hasattr(info, "CRC") else None,
                compression_method=compression_method,
                comment=decode_bytes_with_fallback(info.comment, _ZIP_ENCODINGS)
                if info.comment
                else None,
                encrypted=bool(info.flag_bits & 0x1),
                create_system=CreateSystem(info.create_system)
                if info.create_system in CreateSystem._value2member_map_
                else CreateSystem.UNKNOWN,
                extra={
                    "compress_type": info.compress_type,
                    "compress_size": info.compress_size,
                    "create_system": info.create_system,
                    "create_version": info.create_version,
                    "extract_version": info.extract_version,
                    "flag_bits": info.flag_bits,
                    "volume": info.volume,
                },
                raw_info=info,
                link_target=self._read_link_target(info),
            )
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
