import gzip
import logging
import lzma
import stat
import tarfile
from datetime import datetime, timezone
from typing import IO, Callable, Iterator, List, Union, cast
import io

from archivey.base_reader import (
    ArchiveInfo,
    ArchiveMember,
    BaseArchiveReaderRandomAccess,
)
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveError,
    ArchiveMemberCannotBeOpenedError,
    PackageNotInstalledError,
)
from archivey.io_helpers import ErrorIOStream, LazyOpenIO
from archivey.types import ArchiveFormat, MemberType

logger = logging.getLogger(__name__)


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

        try:
            if format == ArchiveFormat.TAR_ZSTD:
                try:
                    import zstandard
                except ImportError:
                    raise PackageNotInstalledError(
                        "zstandard package is not installed, required for Zstandard archives"
                    ) from None
                try:
                    self._fileobj = zstandard.open(archive_path, "rb")
                    tar_mode = "r|"
                    self._streaming_only = True
                    self._archive = tarfile.open(fileobj=self._fileobj, mode=tar_mode)
                except zstandard.ZstdError as e:
                    if self._fileobj is not None:
                        self._fileobj.close()
                        self._fileobj = None
                    raise ArchiveCorruptedError(
                        f"Invalid compressed TAR archive {archive_path}: {e}"
                    ) from e
            elif format == ArchiveFormat.TAR_LZ4:
                try:
                    import lz4.frame
                except ImportError:
                    raise PackageNotInstalledError(
                        "lz4 package is not installed, required for LZ4 archives"
                    ) from None
                try:
                    self._fileobj = lz4.frame.open(archive_path, "rb")
                    tar_mode = "r|"
                    self._streaming_only = True
                    self._archive = tarfile.open(fileobj=self._fileobj, mode=tar_mode)
                except lz4.frame.LZ4FrameError as e:
                    if self._fileobj is not None:
                        self._fileobj.close()
                        self._fileobj = None
                    raise ArchiveCorruptedError(
                        f"Invalid compressed TAR archive {archive_path}: {e}"
                    ) from e
            else:
                mode_dict = {
                    ArchiveFormat.TAR: "r",
                    ArchiveFormat.TAR_GZ: "r:gz",
                    ArchiveFormat.TAR_BZ2: "r:bz2",
                    ArchiveFormat.TAR_XZ: "r:xz",
                }
                mode: str = mode_dict.get(format, "r")
                if streaming_only:
                    if ":" in mode:
                        mode = mode.replace(":", "|")
                    else:
                        mode += "|"

                self._archive = tarfile.open(archive_path, mode)  # type: ignore

        except tarfile.ReadError as e:
            raise ArchiveCorruptedError(f"Invalid TAR archive {archive_path}: {e}")
        except (gzip.BadGzipFile, lzma.LZMAError) as e:
            raise ArchiveCorruptedError(
                f"Invalid compressed TAR archive {archive_path}: {e}"
            )

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None
            self._members = None
        if getattr(self, "_fileobj", None) is not None:
            self._fileobj.close()
            self._fileobj = None

    def get_members_if_available(self) -> List[ArchiveMember] | None:
        if self._streaming_only:
            return None
        return self.get_members()

    def _tarinfo_to_archive_member(self, info: tarfile.TarInfo) -> ArchiveMember:
        # Get compression method based on format
        compression_method: str | None = {
            ArchiveFormat.TAR: "store",
            ArchiveFormat.TAR_GZ: ArchiveFormat.GZIP,
            ArchiveFormat.TAR_BZ2: ArchiveFormat.BZIP2,
            ArchiveFormat.TAR_XZ: ArchiveFormat.XZ,
            ArchiveFormat.TAR_ZSTD: ArchiveFormat.ZSTD,
            ArchiveFormat.TAR_LZ4: ArchiveFormat.LZ4,
        }.get(self.format, None)

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
            compression_method=compression_method,
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

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            for info in self._archive.getmembers():
                self._members.append(self._tarinfo_to_archive_member(info))

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
            return stream

        except tarfile.ReadError as e:
            raise ArchiveCorruptedError(f"Error reading member {filename}: {e}")

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
        self, filter: Callable[[ArchiveMember], bool] | None = None
    ) -> Iterator[tuple[ArchiveMember, IO[bytes] | None]]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        # TODO: check if this actually works in streaming mode
        for tarinfo in self._archive:
            member = self._tarinfo_to_archive_member(tarinfo)
            if filter is None or filter(member):
                try:
                    stream = LazyOpenIO(self.open, member, seekable=True)
                    yield member, stream
                    stream.close()
                except (ArchiveError, OSError) as e:
                    logger.warning("Error opening member %s: %s", member.filename, e)
                    yield member, ErrorIOStream(e)
