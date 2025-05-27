import io
import os
import tarfile
import gzip
import lzma
from datetime import datetime
from typing import List, Iterator, Union
from archivey.base import (
    ArchiveReader,
    ArchiveMember,
    ArchiveCorruptedError,
    ArchiveMemberNotFoundError,
    ArchiveInfo,
    MemberType,
)
from archivey.formats import CompressionFormat


class TarReader(ArchiveReader):
    """Reader for TAR archives and compressed TAR archives."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self._members = None
        self._format_info = None
        try:
            # Determine if this is a compressed TAR
            ext = os.path.splitext(archive_path)[1].lower()
            mode = "r" if ext == ".tar" else f"r:{ext[1:]}"  # r:gz, r:bz2, r:xz

            self._archive = tarfile.open(archive_path, mode)
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
        if self._members is None:
            self._members = []
            for info in self._archive.getmembers():
                # Get compression method based on format
                compression_method = None
                if self.get_format() != CompressionFormat.TAR:
                    if hasattr(self._archive, "compression"):
                        compression_method = self._archive.compression
                    else:
                        # Map format to compression method
                        compression_method = {
                            CompressionFormat.TAR_GZ: CompressionFormat.GZIP,
                            CompressionFormat.TAR_BZ2: CompressionFormat.BZIP2,
                            CompressionFormat.TAR_XZ: CompressionFormat.XZ,
                            CompressionFormat.TAR_ZSTD: CompressionFormat.ZSTD,
                            CompressionFormat.TAR_LZ4: CompressionFormat.LZ4,
                        }.get(self.get_format())

                member = ArchiveMember(
                    filename=info.name,
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

    def open(self, member: Union[str, ArchiveMember]) -> io.IOBase:
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

    def get_format(self) -> CompressionFormat:
        """Get the compression format of the archive.

        Returns:
            CompressionFormat: The format of the archive (TAR or compressed TAR)
        """
        ext = os.path.splitext(self.archive_path)[1].lower()
        if ext == ".tar":
            return CompressionFormat.TAR
        elif ext == ".gz" or self.archive_path.endswith(".tgz"):
            return CompressionFormat.TAR_GZ
        elif ext == ".bz2" or self.archive_path.endswith(".tbz"):
            return CompressionFormat.TAR_BZ2
        elif ext == ".xz" or self.archive_path.endswith(".txz"):
            return CompressionFormat.TAR_XZ
        elif ext == ".zst":
            return CompressionFormat.TAR_ZSTD
        elif ext == ".lz4":
            return CompressionFormat.TAR_LZ4
        return CompressionFormat.TAR

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._format_info is None:
            format = self.get_format()
            self._format_info = ArchiveInfo(
                format=format,
                is_solid=format
                != CompressionFormat.TAR,  # True for all compressed TAR formats
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
        return format != CompressionFormat.TAR  # True for all compressed TAR formats
