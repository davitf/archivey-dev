# pyright: reportMissingImports=false
import io
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, BinaryIO, Iterator, Optional

from archivey.exceptions import ArchiveError, ArchiveReadError, PackageNotInstalledError
from archivey.internal.base_reader import ArchiveInfo, ArchiveMember, BaseArchiveReader
from archivey.internal.io_helpers import ensure_binaryio, is_stream
from archivey.types import ArchiveFormat, CreateSystem, MemberType

if TYPE_CHECKING:
    import libarchive
else:
    try:
        import libarchive  # type: ignore[import]
    except ImportError:  # pragma: no cover - handled at runtime
        libarchive = None  # type: ignore

logger = logging.getLogger(__name__)


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
        mtime = datetime.fromtimestamp(entry.mtime, tz=timezone.utc)
        if entry.isdir:
            member_type = MemberType.DIR
        elif entry.issym:
            member_type = MemberType.SYMLINK
        elif entry.islnk:
            member_type = MemberType.HARDLINK
        else:
            member_type = MemberType.FILE
        link_target = (
            entry.linkpath
            if member_type in {MemberType.SYMLINK, MemberType.HARDLINK}
            else None
        )
        uid = entry.uid if entry.uid != 0 else None
        gid = entry.gid if entry.gid != 0 else None
        mode = entry.mode if entry.mode != 0 else None
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
        data = b"".join(self._current_entry.get_blocks())
        return io.BytesIO(data)

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(format=self.format, comment=None, is_solid=False, extra={})
