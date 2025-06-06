import logging
import stat
import tarfile
from datetime import datetime, timezone
from typing import IO, Callable, Iterator, List, Optional, Union, cast

from archivey.base_reader import (
    ArchiveInfo,
    ArchiveMember,
    BaseArchiveReaderRandomAccess,
)
from archivey.compressed_streams import open_stream
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveMemberCannotBeOpenedError,
)
from archivey.io_helpers import ErrorIOStream, ExceptionTranslatingIO, LazyOpenIO
from archivey.types import TAR_FORMAT_TO_COMPRESSION_FORMAT, ArchiveFormat, MemberType

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


class TarReader(BaseArchiveReaderRandomAccess):
    """Reader for TAR archives and compressed TAR archives."""

    def __init__(
        self,
        archive_path: str,
        format: ArchiveFormat,
        streaming_only: bool = False,
        *,
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

        super().__init__(format, archive_path)
        self._streaming_only = streaming_only
        self._members = None
        self._format_info = None
        self._fileobj = None

        logger.info(f"TarReader init: {archive_path} {format} {streaming_only}")

        if format in TAR_FORMAT_TO_COMPRESSION_FORMAT:
            self.compression_method = TAR_FORMAT_TO_COMPRESSION_FORMAT[format]
            self._fileobj = open_stream(
                self.compression_method, archive_path, self.config
            )
            logger.info(
                f"Compressed tar opened: {self._fileobj} seekable={self._fileobj.seekable()}"
            )
            if self._fileobj.seekable():
                logger.info(
                    f"Compressed tar seeked: initial tell={self._fileobj.tell()}"
                )
                self._fileobj.seek(0)
                logger.info(f"Compressed tar seeked: after seek={self._fileobj.tell()}")

        elif format == ArchiveFormat.TAR:
            self.compression_method = "store"
            self._fileobj = None
        else:
            raise ValueError(f"Unsupported archive format: {format}")

        open_mode = "r|" if streaming_only else "r:"
        try:
            # Fail on any error.
            self._archive = tarfile.open(
                name=archive_path, fileobj=self._fileobj, mode=open_mode, errorlevel=2
            )
            logger.info(f"Tar opened: {self._archive}")
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
            self._members = None
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

        return ArchiveMember(
            filename=filename,
            file_size=info.size,
            compress_size=None,
            mtime=datetime.fromtimestamp(info.mtime, tz=timezone.utc).replace(
                tzinfo=None
            )
            if info.mtime
            else None,
            type=(
                MemberType.FILE
                if info.isfile()
                else MemberType.DIR
                if info.isdir()
                else MemberType.LINK
                if info.issym() or info.islnk()
                else MemberType.OTHER
            ),
            mode=stat.S_IMODE(info.mode) if hasattr(info, "mode") else None,
            link_target=info.linkname if info.issym() or info.islnk() else None,
            crc32=None,  # TAR doesn't have CRC
            compression_method=self.compression_method,
            extra={
                "type": info.type,
                "mode": info.mode,
                "uid": info.uid,
                "gid": info.gid,
                "uname": info.uname,
                "gname": info.gname,
                "linkname": info.linkname,
                "linkpath": info.linkpath,
                "devmajor": info.devmajor,
                "devminor": info.devminor,
            },
            raw_info=info,
        )

    def _check_tar_integrity(self, last_tarinfo: tarfile.TarInfo) -> None:
        # See what's after the last tarinfo. It should be an empty block.
        data_size = last_tarinfo.size
        # Round up to the next multiple of 512.
        data_blocks = (data_size + 511) & ~511
        next_member_offset = last_tarinfo.offset_data + data_blocks

        if self._fileobj is not None and self._fileobj.seekable():
            self._fileobj.seek(next_member_offset)
            data = self._fileobj.read(512 * 2)
            if len(data) < 512 * 2:
                raise ArchiveCorruptedError("Missing data after last tarinfo")
            if data != b"\x00" * 512:
                # tarfile likely read the block and the checksum failed, so it assumed
                # it's the end of the file.
                raise ArchiveCorruptedError("Invalid data after last tarinfo")

        else:
            logger.warning("Cannot check tar integrity: file is not seekable")

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            tarinfo = None
            for tarinfo in self._archive.getmembers():
                self._members.append(self._tarinfo_to_archive_member(tarinfo))

            if self.config.check_tar_integrity and tarinfo is not None:
                self._check_tar_integrity(tarinfo)

        return self._members

    def open(
        self,
        member_or_filename: Union[str, ArchiveMember],
        *,
        pwd: bytes | str | None = None,
    ) -> IO[bytes]:
        if self._archive is None:
            raise ValueError("Archive is closed")
        if self._streaming_only:
            raise ValueError(
                "Archive opened in streaming mode does not support opening specific members."
            )

        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        info_or_filename = (
            cast(tarfile.TarInfo, member_or_filename.raw_info)
            if isinstance(member_or_filename, ArchiveMember)
            else member_or_filename
        )
        filename = (
            member_or_filename.filename
            if isinstance(member_or_filename, ArchiveMember)
            else member_or_filename
        )

        try:
            stream = self._archive.extractfile(info_or_filename)
            if stream is None:
                raise ArchiveMemberCannotBeOpenedError(
                    f"Member {filename} cannot be opened"
                )
            return ExceptionTranslatingIO(stream, _translate_tar_exception)

        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated is not None:
                raise translated from e
            raise

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

    def iter_members_with_io(
        self,
        filter: Callable[[ArchiveMember], bool] | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, IO[bytes] | None]]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        iterator = iter(self._archive)
        try:
            tarinfo = None

            for tarinfo in iterator:
                member = self._tarinfo_to_archive_member(tarinfo)
                if filter is not None and not filter(member):
                    continue

                try:
                    if self._streaming_only:
                        stream_obj = self._archive.extractfile(tarinfo)
                        if stream_obj is None:
                            raise ArchiveMemberCannotBeOpenedError(
                                f"Member {member.filename} cannot be opened"
                            )
                        stream = ExceptionTranslatingIO(
                            stream_obj, _translate_tar_exception
                        )
                        yield member, stream
                        stream.close()
                    else:
                        stream = LazyOpenIO(self.open, member, seekable=True)
                        yield member, stream
                        stream.close()

                except tarfile.ReadError as e:
                    logger.warning("Error opening member %s: %s", member.filename, e)
                    translated = _translate_tar_exception(e)
                    yield (
                        member,
                        ErrorIOStream(translated or ArchiveCorruptedError(str(e))),
                    )

            if self.config.check_tar_integrity and tarinfo is not None:
                self._check_tar_integrity(tarinfo)

        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated:
                raise translated from e
            raise

        # Try reading a bit further, so that any checksum in the compressed data is
        # checked and corruption is detected.
        try:
            if self._fileobj is not None:
                logger.info(
                    f"Tar closing: Will read a few bytes from {self.archive_path}"
                )
                data = self._fileobj.read(65536)
                logger.info(
                    f"Tar closing: Read {len(data)} bytes from {self.archive_path}"
                )

        except tarfile.ReadError as e:
            translated = _translate_tar_exception(e)
            if translated is not None:
                raise translated from e
            raise
