import os
import struct
from datetime import datetime, timezone
from typing import BinaryIO, Iterator, Optional

import ar  # type: ignore

from archivey.base_reader import BaseArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveFormatError,
    ArchiveIOError,
)
from archivey.io_helpers import ExceptionTranslatingIO
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

ENTRY_STRUCT = struct.Struct("16s12s6s6s8s10sbb")
MAGIC = b"!<arch>\n"


def _padding(n: int, pad_size: int) -> int:
    reminder = n % pad_size
    return pad_size - reminder if reminder else 0


def _pad(n: int, pad_size: int) -> int:
    return n + _padding(n, pad_size)


class _ArEntry:
    __slots__ = ("name", "offset", "size", "mtime", "mode")

    def __init__(self, name: str, offset: int, size: int, mtime: int, mode: int):
        self.name = name
        self.offset = offset
        self.size = size
        self.mtime = mtime
        self.mode = mode


class ArReader(BaseArchiveReader):
    """Reader for Unix ar archives."""

    def __init__(
        self,
        archive_path: BinaryIO | str | os.PathLike,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ) -> None:
        if pwd is not None:
            raise ValueError("AR archives do not support password protection")
        if streaming_only:
            # ar archives require random access to read members
            raise ValueError("AR archives do not support streaming-only mode")

        super().__init__(
            ArchiveFormat.AR,
            archive_path,
            random_access_supported=True,
            members_list_supported=True,
        )

        if isinstance(archive_path, (str, os.PathLike)):
            self._fileobj: BinaryIO | None = open(archive_path, "rb")
            self._needs_close = True
        else:
            self._fileobj = archive_path
            self._needs_close = False

        try:
            self._entries = list(self._load_entries(self._fileobj))
        except ar.ArchiveError as e:
            raise ArchiveCorruptedError(
                f"Invalid AR archive {archive_path}: {e}"
            ) from e

    def _load_entries(self, stream: BinaryIO) -> Iterator[_ArEntry]:
        magic = stream.read(len(MAGIC))
        if magic != MAGIC:
            raise ar.ArchiveError(f"Unexpected magic: {magic!r}")

        lookup_data: Optional[bytes] = None
        while True:
            buffer = stream.read(ENTRY_STRUCT.size)
            if len(buffer) < ENTRY_STRUCT.size:
                break
            name, timestamp, owner, group, mode, size, _, _ = ENTRY_STRUCT.unpack(
                buffer
            )
            name = name.decode().rstrip()
            timestamp = int(timestamp.decode().rstrip() or "0")
            mode = int(mode.decode().rstrip() or "0", 8)
            size = int(size.decode().rstrip() or "0")

            if name == "/":
                stream.seek(_pad(size, 2), 1)
                continue
            elif name == "//":
                lookup_data = stream.read(size)
                stream.seek(_padding(size, 2), 1)
                continue
            elif name.startswith("/"):
                if lookup_data is None:
                    raise ArchiveFormatError("GNU long filename without lookup table")
                lookup_offset = int(name[1:])
                end = lookup_data.find(b"\n", lookup_offset)
                if end == -1:
                    end = len(lookup_data)
                name = lookup_data[lookup_offset:end].decode()
            elif name.startswith("#1/"):
                name_length = int(name[3:])
                name_bytes = stream.read(name_length)
                name = name_bytes.rstrip(b"\x00").decode()
                size -= name_length
            offset = stream.tell()
            stream.seek(_pad(size, 2), 1)
            yield _ArEntry(name, offset, size, timestamp, mode)

    def _close_archive(self) -> None:
        if self._needs_close and self._fileobj is not None:
            self._fileobj.close()
        self._fileobj = None

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(format=self.format, is_solid=True)

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        for entry in self._entries:
            mtime_dt = (
                datetime.fromtimestamp(entry.mtime, tz=timezone.utc)
                if entry.mtime > 0
                else None
            )
            member = ArchiveMember(
                filename=entry.name,
                file_size=entry.size,
                compress_size=entry.size,
                mtime_with_tz=mtime_dt,
                type=MemberType.FILE,
                mode=entry.mode,
                raw_info=entry,
            )
            yield member

    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        self.check_archive_open()
        member, _ = self._resolve_member_to_open(member_or_filename)
        entry = member.raw_info
        if not isinstance(entry, _ArEntry):
            raise ArchiveFormatError("Invalid raw_info for AR member")
        assert self._fileobj is not None
        stream = ar.substream.Substream(self._fileobj, entry.offset, entry.size)  # type: ignore[attr-defined]

        def _translate(exc: Exception):
            if isinstance(exc, OSError):
                return ArchiveIOError(str(exc))
            return None

        return ExceptionTranslatingIO(stream, _translate)
