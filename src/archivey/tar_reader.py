import io
import os
import stat
import tarfile
import gzip
import lzma
from datetime import datetime
from typing import List, Iterator, Union
from archivey.base_reader import (
    ArchiveReader,
    ArchiveMember,
    ArchiveInfo,
)
from archivey.exceptions import ArchiveCorruptedError, ArchiveMemberNotFoundError
from archivey.types import ArchiveFormat, MemberType


class TarReader(ArchiveReader):
    """Reader for TAR archives and compressed TAR archives."""

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        self.archive_path = archive_path
        self._members = None
        self._format_info = None
        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        try:
            # Determine if this is a compressed TAR
            ext = os.path.splitext(archive_path)[1].lower()
            mode: str = "r" if ext == ".tar" else f"r:{ext[1:]}"  # r:gz, r:bz2, r:xz

            # Pylance knows the mode argument can only accept some specific values,
            # and doesn't understand that we're building one of them above.
            # (possible Pylance or typeshed bug: it's not actually listing all possible values)
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

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            for info in self._archive.getmembers():
                # Get compression method based on format
                compression_method = None
                if self.get_format() != ArchiveFormat.TAR:
                    if hasattr(self._archive, "compression"):
                        compression_method = self._archive.compression
                    else:
                        # Map format to compression method
                        compression_method = {
                            ArchiveFormat.TAR_GZ: ArchiveFormat.GZIP,
                            ArchiveFormat.TAR_BZ2: ArchiveFormat.BZIP2,
                            ArchiveFormat.TAR_XZ: ArchiveFormat.XZ,
                            ArchiveFormat.TAR_ZSTD: ArchiveFormat.ZSTD,
                            ArchiveFormat.TAR_LZ4: ArchiveFormat.LZ4,
                        }.get(self.get_format())

                filename = info.name
                if info.isdir() and not filename.endswith("/"):
                    filename += "/"

                member = ArchiveMember(
                    filename=filename,
                    size=info.size,
                    mtime=datetime.fromtimestamp(info.mtime) if info.mtime else None,
                    type=(
                        MemberType.FILE
                        if info.isfile()
                        else MemberType.DIR
                        if info.isdir()
                        else MemberType.LINK
                        if info.issym() or info.islnk()
                        else MemberType.OTHER
                    ),
                    permissions=stat.S_IMODE(info.mode) if hasattr(info, 'mode') else None,
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
                self._members.append(member)

        return self._members

    def open(
        self, member: Union[str, ArchiveMember], *, pwd: bytes | str | None = None
    ) -> io.IOBase:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if pwd is not None:
            raise ValueError("TAR format does not support password protection.")

        if isinstance(member, str):
            try:
                info = self._archive.getmember(member)
            except KeyError:
                raise ArchiveMemberNotFoundError(
                    f"Member {member} not found in archive"
                )
        else:
            try:
                info = self._archive.getmember(member.filename)
            except KeyError:
                raise ArchiveMemberNotFoundError(
                    f"Member {member.filename} not found in archive"
                )

        try:
            return self._archive.extractfile(info)
        except tarfile.ReadError as e:
            raise ArchiveCorruptedError(f"Error reading member {info.name}: {e}")

    def iter_members(self) -> Iterator[ArchiveMember]:
        return iter(self.get_members())

    def get_format(self) -> ArchiveFormat:
        """Get the compression format of the archive.

        Returns:
            ArchiveFormat: The format of the archive (TAR or compressed TAR)
        """
        ext = os.path.splitext(self.archive_path)[1].lower()
        if ext == ".tar":
            return ArchiveFormat.TAR
        elif ext == ".gz" or self.archive_path.endswith(".tgz"):
            return ArchiveFormat.TAR_GZ
        elif ext == ".bz2" or self.archive_path.endswith(".tbz"):
            return ArchiveFormat.TAR_BZ2
        elif ext == ".xz" or self.archive_path.endswith(".txz"):
            return ArchiveFormat.TAR_XZ
        elif ext == ".zst":
            return ArchiveFormat.TAR_ZSTD
        elif ext == ".lz4":
            return ArchiveFormat.TAR_LZ4
        return ArchiveFormat.TAR

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._format_info is None:
            format = self.get_format()
            self._format_info = ArchiveInfo(
                format=format,
                is_solid=format
                != ArchiveFormat.TAR,  # True for all compressed TAR formats
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

    def is_solid(self) -> bool:
        """Check if the archive is solid (all files compressed together).

        Returns:
            bool: True for compressed TAR formats (gz, bz2, xz, etc.), False for plain TAR
        """
        # Compressed TAR formats are effectively solid as they require sequential decompression
        format = self.get_format()
        return format != ArchiveFormat.TAR  # True for all compressed TAR formats
