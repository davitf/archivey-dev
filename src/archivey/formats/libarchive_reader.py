# pyright: reportMissingImports=false
import contextlib
import io
import itertools
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, BinaryIO, Iterator, Optional, cast

from archivey.exceptions import ArchiveError, ArchiveReadError, PackageNotInstalledError
from archivey.internal.base_reader import ArchiveInfo, ArchiveMember, BaseArchiveReader
from archivey.internal.io_helpers import ensure_binaryio, is_stream
from archivey.internal.utils import bytes_to_str
from archivey.types import (
    ArchiveFormat,
    ContainerFormat,
    CreateSystem,
    MemberType,
    StreamFormat,
)

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


LIBARCHIVE_FORMAT_TO_CONTAINER_FORMAT = {
    "RAR5": ContainerFormat.RAR,
    "RAR": ContainerFormat.RAR,
    "7-Zip": ContainerFormat.SEVENZIP,
    "GNU tar format": ContainerFormat.TAR,  # tarcmd files
    "POSIX ustar format": ContainerFormat.TAR,  # tarfile archives
    "POSIX pax interchange format": ContainerFormat.TAR,  # encoding__tarfile.tar
    "raw": ContainerFormat.RAW_STREAM,
}

LIBARCHIVE_FORMAT_TO_VERSION = {
    "RAR5": "5",
    "RAR": "3",
    # "7-Zip": "22.00",
    # "GNU tar format": "1.34",
    # "POSIX ustar format": "1.34",
    # "POSIX pax interchange format": "1.34",
}


class LibArchiveReader(BaseArchiveReader):
    """ArchiveReader implementation using libarchive.

    This reader only supports streaming access via ``iter_members_with_streams``.

    For some reason, some py7zr archives result in a "Truncated 7-zip file body error"
    which seems related to https://github.com/libarchive/libarchive/issues/2106

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

        self._context_manager, self._archive = self._open_archive()
        self._members_iter = iter(self._archive)
        self._first_entry = next(self._members_iter)

        self.format_name = bytes_to_str(self._archive.format_name)
        self.detected_container_format = LIBARCHIVE_FORMAT_TO_CONTAINER_FORMAT.get(
            self.format_name, ContainerFormat.UNKNOWN
        )
        self.filter_names = self._archive.filter_names

    def _translate_exception(
        self, e: Exception
    ) -> Optional[ArchiveError]:  # pragma: no cover - thin wrapper
        if libarchive is not None and isinstance(e, libarchive.exception.ArchiveError):  # type: ignore[attr-defined]
            return ArchiveReadError(str(e))
        return None

    def _close_archive(self) -> None:
        pass
        # self._context_manager.__exit__(None, None, None)

    def _open_archive(
        self,
        format_name: str | None = None,
    ) -> tuple[
        contextlib.AbstractContextManager["libarchive.read.ArchiveRead"],
        "libarchive.read.ArchiveRead",
    ]:
        try:
            logger.info("Opening archive with format_name: %s", format_name)
            if is_stream(self.path_or_stream):
                stream = ensure_binaryio(self.path_or_stream)
                context_manager = libarchive.stream_reader(
                    stream,
                    passphrase=self._archive_password,
                    format_name=format_name or "all",
                )
            else:
                assert isinstance(self.path_or_stream, str)
                context_manager = libarchive.file_reader(
                    self.path_or_stream,
                    passphrase=self._archive_password,
                    format_name=format_name or "all",
                )

            return context_manager, context_manager.__enter__()

        except libarchive.exception.ArchiveError as e:
            logger.error("Error opening archive: %s", e)
            if format_name is None and "Unrecognized archive format" in str(e):
                logger.error("Unrecognized archive format, trying raw stream")
                # Try to open as a raw stream
                return self._open_archive(format_name="raw")

            raise

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        for entry in itertools.chain([self._first_entry], self._members_iter):
            logger.info("entry: %s", entry)
            logger.info("archive.format_name: %s", self.format_name)
            logger.info("archive.filter_names: %s", self.filter_names)
            logger.info("archive.bytes_read: %s", self._archive.bytes_read)
            yield self._entry_to_member(entry)

        self._context_manager.__exit__(None, None, None)

    def _entry_to_member(self, entry: "libarchive.ArchiveEntry") -> ArchiveMember:
        filename = entry.pathname
        if (
            filename == "data"
            and self.detected_container_format == ContainerFormat.RAW_STREAM
        ):
            if self.path_str is not None:
                base_name, ext = os.path.splitext(os.path.basename(self.path_str))
                if ext == "":
                    filename = base_name + ".uncompressed"
                else:
                    filename = base_name

            else:
                filename = "uncompressed"

        if entry.isdir and not filename.endswith("/"):
            filename += "/"

        tzinfo = (
            timezone.utc
            if self.detected_container_format
            in (ContainerFormat.TAR, ContainerFormat.SEVENZIP)
            or self.format_name == "RAR5"
            else None
        )
        mtime = (
            datetime.fromtimestamp(entry.mtime, tz=tzinfo)
            if entry.mtime is not None
            else None
        )
        if (
            mtime is None
            and self.detected_container_format == ContainerFormat.RAW_STREAM
            and self.path_str is not None
        ):
            mtime = datetime.fromtimestamp(
                os.path.getmtime(self.path_str), tz=timezone.utc
            )

        if entry.isdir:
            member_type = MemberType.DIR
        elif entry.issym:
            member_type = MemberType.SYMLINK
        elif entry.islnk:
            member_type = MemberType.HARDLINK
        # py7zr archives have a filetype of 0 for files for some reason, which result in
        # entry.isfile being False.
        elif entry.isfile or entry.filetype == 0:
            member_type = MemberType.FILE
        else:
            member_type = MemberType.OTHER
        link_target = (
            bytes_to_str(entry.linkpath)
            if member_type in {MemberType.SYMLINK, MemberType.HARDLINK}
            else None
        )
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
        stream = _LibArchiveEntryStream(
            cast("libarchive.ArchiveEntry", member.raw_info)
        )
        return ensure_binaryio(stream)

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(
            format=ArchiveFormat(
                self.detected_container_format, StreamFormat.UNCOMPRESSED
            ),
            version=LIBARCHIVE_FORMAT_TO_VERSION.get(self.format_name, None),
            comment=None,
            is_solid=self.detected_container_format == ContainerFormat.TAR,
            extra={
                "libarchive_format_name": self.format_name,
                "libarchive_filter_names": self.filter_names,
            },
        )
