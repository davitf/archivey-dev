# pyright: reportMissingImports=false
import io
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, BinaryIO, Iterator, Optional

from archivey.exceptions import ArchiveError, ArchiveReadError, PackageNotInstalledError
from archivey.internal.base_reader import ArchiveInfo, ArchiveMember, BaseArchiveReader
from archivey.internal.io_helpers import ensure_binaryio, is_stream
from archivey.types import ArchiveFormat, ContainerFormat, CreateSystem, MemberType

if TYPE_CHECKING:
    import libarchive
else:
    try:
        import libarchive  # type: ignore[import]
    except ImportError:  # pragma: no cover - handled at runtime
        libarchive = None  # type: ignore

logger = logging.getLogger(__name__)


class _LibArchiveEntryStream(io.RawIOBase, BinaryIO):
    """Wrap ``libarchive`` entry blocks as a streaming file-like object."""

    def __init__(self, entry: "libarchive.ArchiveEntry") -> None:
        super().__init__()
        self._blocks = entry.get_blocks()
        self._buffer = bytearray()
        self._eof = False

    def readable(self) -> bool:  # pragma: no cover - trivial
        return True

    def writable(self) -> bool:  # pragma: no cover - trivial
        return False

    def seekable(self) -> bool:  # pragma: no cover - trivial
        return False

    def _fill_buffer(self, size: int) -> None:
        while not self._eof and (size < 0 or len(self._buffer) < size):
            try:
                self._buffer.extend(next(self._blocks))
            except StopIteration:
                self._eof = True
                break

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        self._fill_buffer(size)
        if size < 0:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:size])
            del self._buffer[:size]
        return data

    def readinto(self, b: bytearray | memoryview) -> int:
        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def close(self) -> None:  # pragma: no cover - simple
        if not self._eof:
            for _ in self._blocks:
                pass
            self._eof = True
        super().close()

    def seek(
        self, offset: int, whence: int = io.SEEK_SET
    ) -> int:  # pragma: no cover - trivial
        raise io.UnsupportedOperation("seek")

    def tell(self) -> int:  # pragma: no cover - trivial
        raise io.UnsupportedOperation("tell")


class LibArchiveReader(BaseArchiveReader):
    """ArchiveReader implementation using libarchive.

    This reader only supports streaming access via ``iter_members_with_streams``.
    """

    def __init__(
        self,
        archive_path: BinaryIO | str,
        format: ArchiveFormat,
        *,
        streaming_only: bool = False,
        pwd: bytes | str | None = None,
    ):
        if libarchive is None:
            raise PackageNotInstalledError("libarchive-c is not installed")
        if not streaming_only:
            raise ValueError("LibArchiveReader only supports streaming_only mode")

        super().__init__(
            format=format,
            archive_path=archive_path,
            pwd=pwd,
            streaming_only=True,
            members_list_supported=False,
        )

        self._pwd = pwd.encode("utf-8") if isinstance(pwd, str) else pwd

        if is_stream(archive_path):
            stream = ensure_binaryio(archive_path)
            self._ctx: Any = libarchive.stream_reader(stream, passphrase=self._pwd)
        else:
            self._ctx = libarchive.file_reader(str(archive_path), passphrase=self._pwd)

        self._archive = self._ctx.__enter__()
        self._entry_iter: Iterator[Any] = iter(self._archive)
        self._current_entry: Any = None

    def _translate_exception(
        self, e: Exception
    ) -> Optional[ArchiveError]:  # pragma: no cover - thin wrapper
        if libarchive is not None and isinstance(e, libarchive.exception.ArchiveError):  # type: ignore[attr-defined]
            return ArchiveReadError(str(e))
        return None

    def _close_archive(self) -> None:
        if getattr(self, "_ctx", None) is not None:
            self._ctx.__exit__(None, None, None)
            self._archive = None
            self._ctx = None

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        for entry in self._entry_iter:
            self._current_entry = entry
            yield self._entry_to_member(entry)
        self._close_archive()

    def _entry_to_member(self, entry: "libarchive.ArchiveEntry") -> ArchiveMember:
        filename = entry.pathname
        if entry.isdir and not filename.endswith("/"):
            filename += "/"
        tzinfo = (
            timezone.utc
            if self.format.container in {ContainerFormat.TAR, ContainerFormat.SEVENZIP}
            else None
        )
        mtime_raw = entry.mtime
        mtime_val = float(mtime_raw) if mtime_raw is not None else 0.0
        mtime = (
            datetime.fromtimestamp(mtime_val, tz=tzinfo)
            if tzinfo is not None
            else datetime.fromtimestamp(mtime_val)
        )
        if entry.isdir:
            member_type = MemberType.DIR
        elif entry.issym:
            member_type = MemberType.SYMLINK
        elif entry.islnk:
            member_type = MemberType.HARDLINK
        else:
            member_type = MemberType.FILE
        link_target_raw = (
            entry.linkpath
            if member_type in {MemberType.SYMLINK, MemberType.HARDLINK}
            else None
        )
        if isinstance(link_target_raw, bytes):
            link_target = link_target_raw.decode("utf-8")
        else:
            link_target = link_target_raw
        uid = entry.uid if entry.uid != 0 else None
        gid = entry.gid if entry.gid != 0 else None
        mode = entry.mode & 0o7777 if entry.mode != 0 else None
        return ArchiveMember(
            filename=filename,
            file_size=entry.size if entry.isfile else None,
            compress_size=None,
            mtime_with_tz=mtime,
            type=member_type,
            mode=mode,
            uid=uid,
            gid=gid,
            uname=entry.uname or None,
            gname=entry.gname or None,
            crc32=None,
            compression_method=None,
            comment=None,
            create_system=CreateSystem.UNIX,
            encrypted=False,
            extra={},
            link_target=link_target,
            raw_info=entry,
        )

    def _open_member(
        self, member: ArchiveMember, pwd: bytes | str | None, for_iteration: bool
    ) -> BinaryIO:
        assert self._current_entry is not None
        stream = _LibArchiveEntryStream(self._current_entry)
        return ensure_binaryio(stream)

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(format=self.format, comment=None, is_solid=False, extra={})
