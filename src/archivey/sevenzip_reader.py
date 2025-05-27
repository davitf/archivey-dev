import io
import py7zr
from typing import List, Iterator
from archivey.base import (
    ArchiveReader,
    ArchiveMember,
    ArchiveEncryptedError,
    ArchiveCorruptedError,
    ArchiveInfo,
    MemberType,
)
from archivey.formats import CompressionFormat


class SevenZipReader(ArchiveReader):
    """Reader for 7-Zip archives."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self._members = None
        self._format_info = None
        try:
            self._archive = py7zr.SevenZipFile(archive_path, "r")
            if self._archive.password_protected:
                raise ArchiveEncryptedError(
                    f"7-Zip archive {archive_path} is encrypted"
                )
        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(f"Invalid 7-Zip archive {archive_path}") from e
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"7-Zip archive {archive_path} is encrypted"
            ) from e

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None
            self._members = None

    def get_members(self) -> List[ArchiveMember]:
        if self._members is None:
            self._members = []

            links_to_resolve = {}

            for file in self._archive.files:
                member = ArchiveMember(
                    filename=file.filename,
                    size=file.uncompressed,
                    mtime=py7zr.helpers.filetime_to_dt(file.lastwritetime)
                    if file.lastwritetime
                    else None,
                    type=(
                        MemberType.DIR
                        if file.is_directory
                        else MemberType.LINK
                        if file.is_symlink
                        else MemberType.OTHER
                        if file.is_junction or file.is_socket
                        else MemberType.FILE
                    ),
                    crc32=file.crc32,
                    compression_method=None,  # Not exposed by py7zr
                    encrypted=False,  # If encrypted, __init__ will raise an exception
                    raw_info=file,
                )
                if member.is_link:
                    links_to_resolve[member.filename] = member
                self._members.append(member)

            if links_to_resolve:
                self._archive.reset()
                files = self._archive.read(list(links_to_resolve.keys()))
                for filename, file in files.items():
                    links_to_resolve[filename].link_target = file.read().decode("utf-8")

        return self._members

    def open(self, member: ArchiveMember) -> io.IOBase:
        self._archive.reset()  # Needed after each read() call

        # TODO: can we pass all files to read() at once and return the IO objects for each file?
        # Will it decompress all files at once, or only when each IO object is read?

        try:
            return self._archive.read([member.filename])[member.filename]
        except py7zr.exceptions.ArchiveError as e:
            raise ArchiveCorruptedError(f"Error reading member {member.filename}: {e}")

    def iter_members(self) -> Iterator[ArchiveMember]:
        return iter(self.get_members())

    def get_format(self) -> CompressionFormat:
        """Get the compression format of the archive.

        Returns:
            CompressionFormat: Always returns CompressionFormat.SEVENZIP for SevenZipReader
        """
        return CompressionFormat.SEVENZIP

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._format_info is None:
            self._format_info = ArchiveInfo(
                format=CompressionFormat.SEVENZIP,
                is_solid=self.is_solid(),
                extra={
                    "is_encrypted": self._archive.password_protected,
                    "header_compressed": self._archive.header_compressed
                    if hasattr(self._archive, "header_compressed")
                    else None,
                    "header_crc": self._archive.header_crc
                    if hasattr(self._archive, "header_crc")
                    else None,
                    "version": self._archive.version
                    if hasattr(self._archive, "version")
                    else None,
                },
            )
        return self._format_info

    def is_solid(self) -> bool:
        """Check if the archive is solid (all files compressed together).

        Returns:
            bool: True if the archive is solid, False otherwise
        """
        # 7-Zip archives are solid if they have the solid flag set
        return bool(self._archive.solid) if hasattr(self._archive, "solid") else False
