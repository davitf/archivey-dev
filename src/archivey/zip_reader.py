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


def get_zipinfo_timestamp(zip_info: zipfile.ZipInfo) -> datetime:
    """Get the timestamp from a ZipInfo object, handling extended timestamp fields."""
    main_modtime = datetime(*zip_info.date_time)
    if not zip_info.extra:
        return main_modtime

    # Parse extended timestamp extra field (0x5455)
    pos = 0
    while pos < len(zip_info.extra):
        if len(zip_info.extra) - pos < 4:  # Need at least 4 bytes for header
            break

        tp, ln = struct.unpack("<HH", zip_info.extra[pos : pos + 4])

        if tp == 0x5455:  # Extended Timestamp
            flags = zip_info.extra[pos + 4]

            # Check if modification time is present (bit 0)
            if flags & 0x01:
                # Read modification time (4 bytes, Unix timestamp)
                mod_time = int.from_bytes(zip_info.extra[pos + 5 : pos + 9], "little")

                # Convert to datetime
                if mod_time > 0:
                    extra_modtime = datetime.fromtimestamp(mod_time, tz=timezone.utc)
                    logger.debug(
                        f"Modtime: main={main_modtime}, extra={extra_modtime} timestamp={mod_time}"
                    )
                    return extra_modtime

        # Skip this field: 4 bytes header + data_size
        pos += 4 + ln

    logger.info(f"Modtime: main={main_modtime}")
    return main_modtime


class ZipReader(BaseArchiveReader):
    """Reader for ZIP archives."""

    def __init__(
        self,
        archive_path: BinaryIO | str | bytes | os.PathLike,
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

            member = ArchiveMember(
                filename=info.filename,
                file_size=info.file_size,
                compress_size=info.compress_size,
                mtime_with_tz=get_zipinfo_timestamp(info),
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
