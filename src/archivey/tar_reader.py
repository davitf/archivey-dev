import logging
import os
import stat
import tarfile
from datetime import datetime, timezone
from typing import IO, BinaryIO, Iterator, List, Optional, Union, cast

from archivey.base_reader import (
    ArchiveInfo,
    ArchiveInfo,
    ArchiveMember,
    BaseArchiveReader,
)
from archivey.compressed_streams import open_stream, open_stream_fileobj
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveMemberCannotBeOpenedError,
)
from archivey.io_helpers import ExceptionTranslatingIO
from archivey.types import (
    TAR_FORMAT_TO_COMPRESSION_FORMAT,
    ArchiveFormat,
    CreateSystem,
    MemberType,
)

logger = logging.getLogger(__name__)


def _translate_tar_exception(e: Exception) -> Optional[ArchiveError]:
    if isinstance(e, tarfile.ReadError):
        if "unexpected end of data" in str(e).lower():
            exc = ArchiveEOFError("TAR archive is truncated")
        else:
            exc = ArchiveCorruptedError(f"Error reading TAR archive: {e}")
        exc.__cause__ = e
        return exc

    return None


class TarReader(BaseArchiveReader):
    """Reader for TAR archives and compressed TAR archives."""

    def __init__(
        self,
        archive_path: BinaryIO | str,
        format: ArchiveFormat,
        *,
        streaming_only: bool = False,
        pwd: bytes | str | None = None,
    ):
        """Initialize the reader.

        Args:
            archive_path: Path to the TAR archive
            pwd: Password for decryption (not supported for TAR)
            format: The format of the archive. If None, will be detected from the file extension.
        """
        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        super().__init__(
            format,
            archive_path,
            random_access_supported=not streaming_only,
            members_list_supported=not streaming_only,
            pwd=pwd,
        )
        self._streaming_only = streaming_only
        self._format_info: ArchiveInfo | None = None
        self._fileobj: BinaryIO | None = None

        logger.debug(f"TarReader init: {archive_path} {format} {streaming_only}")

        if format in TAR_FORMAT_TO_COMPRESSION_FORMAT:
            self.compression_method = TAR_FORMAT_TO_COMPRESSION_FORMAT[format]
            if isinstance(archive_path, str):
                self._fileobj = open_stream(
                    self.compression_method, archive_path, self.config
                )
            else:
                self._fileobj = open_stream_fileobj(
                    self.compression_method, archive_path, self.config
                )
            logger.debug(
                f"Compressed tar opened: {self._fileobj} seekable={self._fileobj.seekable()}"
            )

            if not streaming_only and not self._fileobj.seekable():
                raise ArchiveError(
                    f"Tried to open a random-access {format.value} file, but inner stream is not seekable ({self._fileobj})"
                )

        elif format == ArchiveFormat.TAR:
            self.compression_method = "store"
            if isinstance(archive_path, str):
                self._fileobj = open(archive_path, "rb")
            else:
                self._fileobj = archive_path
        else:
            raise ValueError(f"Unsupported archive format: {format}")

        open_mode = "r|" if streaming_only else "r:"
        try:
            # Fail on any error.
            self._archive = tarfile.open(
                name=archive_path if isinstance(archive_path, str) else None,
                fileobj=self._fileobj,
                mode=open_mode,
                errorlevel=2,
            )
            logger.debug(
                f"Tar opened: {self._archive} seekable={self._fileobj.seekable()}"
            )
        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated is not None:
                raise translated from e
            raise

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None
        if self._fileobj is not None:
            self._fileobj.close()
            self._fileobj = None

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        if self._streaming_only:
            return None
        return self.get_members()

    def _tarinfo_to_archive_member(self, info: tarfile.TarInfo) -> ArchiveMember:
        filename = info.name
        if info.isdir() and not filename.endswith("/"):
            filename += "/"

        atime_with_tz: Optional[datetime] = None
        ctime_with_tz: Optional[datetime] = None

        if hasattr(info, "pax_headers") and info.pax_headers:
            pax_atime = info.pax_headers.get("atime")
            pax_ctime = info.pax_headers.get("ctime")
            if pax_atime:
                try:
                    atime_with_tz = datetime.fromtimestamp(float(pax_atime), tz=timezone.utc)
                except ValueError:
                    logger.warning(f"Could not parse pax_header atime: {pax_atime}")
            if pax_ctime:
                try:
                    ctime_with_tz = datetime.fromtimestamp(float(pax_ctime), tz=timezone.utc)
                except ValueError:
                    logger.warning(f"Could not parse pax_header ctime: {pax_ctime}")

        create_system = CreateSystem.UNKNOWN
        # If we have uname, gname, or POSIX-like mode bits, assume UNIX.
        # Tar files are predominantly from Unix-like systems.
        if info.uname or info.gname or (hasattr(info, "mode") and info.mode & 0o700): # Check for user permission bits
            create_system = CreateSystem.UNIX

        return ArchiveMember(
            filename=filename,
            file_size=info.size,
            compress_size=None, # Tar itself doesn't compress members, the stream might be compressed
            mtime_with_tz=datetime.fromtimestamp(info.mtime, tz=timezone.utc)
            if info.mtime
            else None,
            atime_with_tz=atime_with_tz,
            ctime_with_tz=ctime_with_tz,
            type=(
                MemberType.FILE
                if info.isfile()
                else MemberType.DIR
                if info.isdir()
                else MemberType.SYMLINK
                if info.issym()
                else MemberType.HARDLINK
                if info.islnk()
                else MemberType.OTHER
            ),
            mode=stat.S_IMODE(info.mode) if hasattr(info, "mode") else None, # Extracts permission bits
            link_target=info.linkname if info.issym() or info.islnk() else None,
            crc32=None,  # TAR doesn't have CRC
            compression_method=self.compression_method, # This is the compression of the tar stream itself
            uid=info.uid if hasattr(info, "uid") else None,
            gid=info.gid if hasattr(info, "gid") else None,
            user_name=info.uname if hasattr(info, "uname") else None,
            group_name=info.gname if hasattr(info, "gname") else None,
            create_system=create_system,
            extra={ # Keep other potentially useful tar-specific info
                "type": info.type,
                "original_mode": info.mode, # Store original mode if needed elsewhere
                "linkname": info.linkname, # Redundant with link_target but part of tar specifics
                "linkpath": info.linkpath,
                "devmajor": info.devmajor if hasattr(info, 'devmajor') else None,
                "devminor": info.devminor if hasattr(info, 'devminor') else None,
                "pax_headers": info.pax_headers if hasattr(info, "pax_headers") else None,
            },
            raw_info=info,
        )

    def _check_tar_integrity(self, last_tarinfo: tarfile.TarInfo) -> None:
        # See what's after the last tarinfo. It should be an empty block.
        data_size = last_tarinfo.size
        # Round up to the next multiple of 512.
        data_blocks = (data_size + 511) & ~511
        next_member_offset = last_tarinfo.offset_data + data_blocks

        if self._fileobj is None:
            logger.warning("Cannot check tar integrity: file object is missing")
            return

        if self._fileobj.seekable():
            self._fileobj.seek(next_member_offset)
        else:
            remaining = next_member_offset - self._fileobj.tell()
            if remaining > 0:
                self._fileobj.read(remaining)
        data = self._fileobj.read(512 * 2)
        if len(data) < 512 * 2:
            raise ArchiveCorruptedError("Missing data after last tarinfo")
        if data != b"\x00" * (512 * 2):
            raise ArchiveCorruptedError("Invalid data after last tarinfo")

    def open(
        self,
        member_or_filename: Union[str, ArchiveMember],
        *,
        pwd: bytes | str | None = None,
        is_streaming_mode: bool = False,
    ) -> BinaryIO:
        if self._archive is None:
            raise ValueError("Archive is closed")
        if self._streaming_only and not is_streaming_mode:
            raise ValueError(
                "Archive opened in streaming mode does not support opening specific members."
            )

        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        member, filename = self._resolve_member_to_open(member_or_filename)
        tarinfo = cast(tarfile.TarInfo, member.raw_info)

        def _open_stream() -> IO[bytes]:
            assert self._archive is not None
            stream = self._archive.extractfile(tarinfo)
            if stream is None:
                raise ArchiveMemberCannotBeOpenedError(
                    f"Member {filename} cannot be opened"
                )
            return stream

        try:
            return ExceptionTranslatingIO(_open_stream, _translate_tar_exception)

        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated is not None:
                raise translated from e
            raise

    def open_for_iteration(
        self, member: ArchiveMember, *, pwd: bytes | str | None = None
    ) -> BinaryIO:
        return self.open(member, is_streaming_mode=True, pwd=pwd)

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._format_info is None:
            format = self.format
            self._format_info = ArchiveInfo(
                format=format,
                is_solid=format != ArchiveFormat.TAR,  # True for compressed TAR formats
                extra={
                    "format_version": self._archive.format
                    if hasattr(self._archive, "format")
                    else None,
                    "encoding": self._archive.encoding
                    if hasattr(self._archive, "encoding")
                    else None,
                },
            )
        return self._format_info

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        try:
            tarinfo: tarfile.TarInfo | None = None
            for tarinfo in self._archive:
                yield self._tarinfo_to_archive_member(tarinfo)

            if self.config.tar_check_integrity and tarinfo is not None:
                self._check_tar_integrity(tarinfo)
        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated is not None:
                raise translated from e
            raise

    @classmethod
    def is_tar_file(cls, file: BinaryIO | str | os.PathLike) -> bool:
        return tarfile.is_tarfile(file)
