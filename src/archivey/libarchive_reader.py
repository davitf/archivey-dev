import io
import logging
import os
from datetime import datetime, timezone
from typing import BinaryIO, Iterator

import libarchive

from .base_reader import BaseArchiveReader
from .exceptions import (
    ArchiveMemberCannotBeOpenedError,
    ArchiveMemberNotFoundError,
)
from .types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

logger = logging.getLogger(__name__)


def _convert_entry(entry: libarchive.entry.ArchiveEntry, format: ArchiveFormat) -> ArchiveMember:
    filename = entry.pathname
    if entry.isdir and not filename.endswith("/"):
        filename += "/"
    if entry.mtime is not None:
        mtime_dt = datetime.fromtimestamp(entry.mtime, tz=timezone.utc)
        if format == ArchiveFormat.ZIP:
            mtime = mtime_dt.replace(tzinfo=None)
        else:
            mtime = mtime_dt
    else:
        mtime = None
    if entry.isfile:
        member_type = MemberType.FILE
    elif entry.issym:
        member_type = MemberType.SYMLINK
    elif entry.islnk:
        member_type = MemberType.HARDLINK
    elif entry.isdir:
        member_type = MemberType.DIR
    else:
        member_type = MemberType.OTHER

    mode = entry.mode
    if mode is not None:
        mode &= 0o7777

    return ArchiveMember(
        filename=filename,
        file_size=entry.size if entry.isfile else entry.size,
        compress_size=None,
        mtime_with_tz=mtime,
        type=member_type,
        mode=mode,
        link_target=entry.linkpath if entry.linkpath else None,
        raw_info=entry,
    )


class LibarchiveReader(BaseArchiveReader):
    """Reader that delegates to libarchive for all formats."""

    def __init__(
        self,
        archive_path: BinaryIO | str | os.PathLike,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ):
        super().__init__(
            format,
            archive_path,
            random_access_supported=True,
            members_list_supported=True,
            pwd=pwd,
        )
        self._passphrase = pwd.decode("utf-8") if isinstance(pwd, bytes) else pwd

    def _close_archive(self) -> None:
        pass  # libarchive is stateless in this implementation

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        passphrase = self._passphrase
        with libarchive.file_reader(self.archive_path, passphrase=passphrase) as archive:
            for entry in archive:
                yield _convert_entry(entry, self.format)

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(format=self.format, is_solid=False)

    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        member, filename = self._resolve_member_to_open(member_or_filename)
        if not member.is_file:
            raise ArchiveMemberCannotBeOpenedError(
                f"Member is not a file: {filename}"
            )

        passphrase = (
            pwd.decode("utf-8") if isinstance(pwd, bytes) else pwd
        ) or self._passphrase

        with libarchive.file_reader(self.archive_path, passphrase=passphrase) as archive:
            for entry in archive:
                if entry.pathname == member.filename.rstrip("/"):
                    data = b"".join(entry.get_blocks())
                    return io.BytesIO(data)
        raise ArchiveMemberNotFoundError(f"Member not found: {filename}")
