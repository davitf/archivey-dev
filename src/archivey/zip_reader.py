import stat

import struct
import zipfile
from datetime import datetime
from typing import List, Iterator, Optional, IO
from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveError,
)
from archivey.formats import ArchiveFormat
from archivey.types import ArchiveInfo, ArchiveMember, MemberType
from archivey.utils import decode_bytes_with_fallback, str_to_bytes

# TODO: check if this is correct
_ZIP_ENCODINGS = ["utf-8", "cp437", "cp1252", "latin-1"]


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
                    return datetime.fromtimestamp(mod_time)

        # Skip this field: 4 bytes header + data_size
        pos += 4 + ln

    return main_modtime


class ZipReader(ArchiveReader):
    """Reader for ZIP archives."""

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        self.archive_path = archive_path
        self._members: list[ArchiveMember] | None = None
        self._format_info: ArchiveInfo | None = None
        self._pwd = pwd
        try:
            self._archive = zipfile.ZipFile(archive_path, "r")
        except zipfile.BadZipFile as e:
            raise ArchiveCorruptedError(f"Invalid ZIP archive {archive_path}") from e

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None
            self._members = None

    def get_format(self) -> ArchiveFormat:
        """Get the compression format of the archive.

        Returns:
            ArchiveFormat: Always returns ArchiveFormat.ZIP
        """
        return ArchiveFormat.ZIP

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._format_info is None:
            self._format_info = ArchiveInfo(
                format=ArchiveFormat.ZIP,
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

    def is_solid(self) -> bool:
        """Check if the archive is solid (all files compressed together).

        Returns:
            bool: Always returns False for ZIP archives
        """
        return False

    def _get_link_target(self, info: zipfile.ZipInfo) -> Optional[str]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        # TODO: do we need to handle the UTF8 flag or fallback encodings?
        if stat.S_ISLNK(info.external_attr >> 16):
            with self._archive.open(info.filename) as f:
                return f.read().decode("utf-8")
        return None

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
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
                    size=info.file_size,
                    mtime=get_zipinfo_timestamp(info),
                    type=MemberType.DIR
                    if is_dir
                    else MemberType.LINK
                    if is_link
                    else MemberType.FILE,
                    permissions=stat.S_IMODE(info.external_attr >> 16) if hasattr(info, 'external_attr') and info.external_attr != 0 else None,
                    crc32=info.CRC if hasattr(info, "CRC") else None,
                    compression_method=compression_method,
                    comment=decode_bytes_with_fallback(info.comment, _ZIP_ENCODINGS)
                    if info.comment
                    else None,
                    encrypted=bool(info.flag_bits & 0x1),
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
                    link_target=self._get_link_target(info),
                )
                self._members.append(member)

        return self._members

    def open(
        self, member: ArchiveMember, pwd: Optional[bytes | str] = None
    ) -> IO[bytes]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        try:
            return self._archive.open(
                member.filename, pwd=str_to_bytes(pwd or self._pwd)
            )
        except RuntimeError as e:
            if "password required" in str(e):
                raise ArchiveEncryptedError(
                    f"Member {member.filename} is encrypted"
                ) from e
            raise ArchiveError(f"Error reading member {member.filename}: {e}") from e
        except zipfile.BadZipFile as e:
            raise ArchiveCorruptedError(
                f"Error reading member {member.filename}: {e}"
            ) from e

    def iter_members(self) -> Iterator[ArchiveMember]:
        return iter(self.get_members())
