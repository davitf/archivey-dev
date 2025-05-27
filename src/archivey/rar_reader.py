import io
import rarfile
import subprocess
import threading
import zlib
from datetime import datetime
from typing import List, Iterator, Optional, IO, Iterable, Any
from archivey.base_reader import ArchiveReader
from archivey.exceptions import ArchiveCorruptedError, ArchiveError
from archivey.formats import CompressionFormat
from archivey.types import ArchiveInfo, ArchiveMember, MemberType


_RAR_COMPRESSION_METHODS = {
    0x30: "store",
    0x31: "fastest",
    0x32: "fast",
    0x33: "normal",
    0x34: "good",
    0x35: "best",
}


class BaseRarReader(ArchiveReader):
    """Base class for RAR archive readers."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self._members: Optional[list[ArchiveMember]] = None
        self._format_info: Optional[ArchiveInfo] = None

        try:
            self._archive = rarfile.RarFile(archive_path, "r")
            self._is_solid = (
                bool(self._archive.solid) if hasattr(self._archive, "solid") else False
            )
            # if self._archive.needs_password():
            #     raise ArchiveEncryptedError(f"RAR archive {archive_path} is encrypted")
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(f"Invalid RAR archive {archive_path}: {e}")
        except rarfile.NotRarFile as e:
            raise ArchiveCorruptedError(f"Not a RAR archive {archive_path}: {e}")
        except rarfile.NeedFirstVolume as e:
            raise ArchiveError(
                f"Need first volume of multi-volume RAR archive {archive_path}: {e}"
            )

    def close(self):
        if self._archive:
            self._archive.close()
            self._archive = None
            self._members = None

    def _get_link_target(self, info: rarfile.RarInfo) -> Optional[str]:
        if not info.is_symlink():
            return None
        if info.file_redir:
            return info.file_redir[2]
        elif not info.needs_password():
            if self._archive is None:
                raise ArchiveError("Archive is closed")
            return self._archive.read(info.filename).decode("utf-8")

        # If the link target is encrypted, we can't read it.
        return None

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ArchiveError("Archive is closed")

        if self._members is None:
            self._members = []
            rarinfos: list[rarfile.RarInfo] = self._archive.infolist()
            for info in rarinfos:
                compression_method = (
                    _RAR_COMPRESSION_METHODS.get(info.compress_type, "unknown")
                    if info.compress_type is not None
                    else None
                )

                member = ArchiveMember(
                    filename=info.filename,
                    size=info.file_size,
                    mtime=datetime(*info.date_time) if info.date_time else None,
                    type=(
                        MemberType.DIR
                        if info.is_dir()
                        else MemberType.FILE
                        if info.is_file()
                        else MemberType.LINK
                        if info.is_symlink()
                        else MemberType.OTHER
                    ),
                    crc32=info.CRC if hasattr(info, "CRC") else None,
                    compression_method=compression_method,
                    comment=info.comment,
                    encrypted=info.needs_password(),
                    extra=None,
                    raw_info=info,
                    link_target=self._get_link_target(info),
                )
                self._members.append(member)

        return self._members

    def iter_members(self) -> Iterator[ArchiveMember]:
        return iter(self.get_members())

    def get_format(self) -> CompressionFormat:
        """Get the compression format of the archive.

        Returns:
            CompressionFormat: Always returns CompressionFormat.RAR
        """
        return CompressionFormat.RAR

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ArchiveError("Archive is closed")

        if self._format_info is None:
            # RAR5 archives have a different magic number and structure
            with open(self.archive_path, "rb") as f:
                magic = f.read(8)
                version = (
                    "5"
                    if magic.startswith(b"\x52\x61\x72\x21\x1a\x07\x01\x00")
                    else "4"
                )

            self._format_info = ArchiveInfo(
                format=CompressionFormat.RAR,
                version=version,
                is_solid=self._is_solid,
                extra={
                    # "is_multivolume": self._archive.is_multivolume(),
                    "needs_password": self._archive.needs_password(),
                    "comment": self._archive.comment
                    if hasattr(self._archive, "comment")
                    else None,
                },
            )

        return self._format_info


class RarReader(BaseRarReader):
    """Reader for RAR archives using rarfile."""

    def __init__(self, archive_path: str):
        super().__init__(archive_path)

    def open(self, member: ArchiveMember) -> IO[bytes]:
        if self._archive is None:
            raise ArchiveError("Archive is closed")

        try:
            return self._archive.open(member.filename)
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(
                f"Error reading member {member.filename}"
            ) from e


class CRCMismatchError(ArchiveCorruptedError):
    def __init__(self, filename: str, expected: int, actual: int):
        super().__init__(
            f"CRC mismatch in {filename}: expected {expected:08x}, got {actual:08x}"
        )


class RarSolidMemberFile(io.RawIOBase, IO[bytes]):
    def __init__(self, member: ArchiveMember, shared_stream: IO[bytes], lock: threading.Lock):
        super().__init__()
        self._stream = shared_stream
        self._remaining = member.size
        self._expected_crc = (
            member.crc32 & 0xFFFFFFFF if member.crc32 is not None else None
        )
        self._actual_crc = 0
        self._lock = lock
        self._closed = False
        self._filename = member.filename
        self._fully_read = False

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError(f"Cannot read from closed/expired file: {self._filename}")

        with self._lock:
            if self._remaining == 0:
                self._fully_read = True
                self._check_crc()
                return b""

            to_read = self._remaining if n < 0 else min(self._remaining, n)
            data = self._stream.read(to_read)
            if not data:
                raise EOFError(f"Unexpected EOF while reading {self._filename}")
            self._remaining -= len(data)
            self._actual_crc = zlib.crc32(data, self._actual_crc)

            if self._remaining == 0:
                self._fully_read = True
                self._check_crc()

            return data

    def _check_crc(self):
        if self._expected_crc is None:
            return
        if (self._actual_crc & 0xFFFFFFFF) != self._expected_crc:
            raise CRCMismatchError(self._filename, self._expected_crc, self._actual_crc)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def write(self, b: Any) -> int:
        raise io.UnsupportedOperation("write")

    def writelines(self, lines: Iterable[Any]) -> None:
        raise io.UnsupportedOperation("writelines")

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            while self._remaining > 0:
                chunk = self._stream.read(min(65536, self._remaining))
                if not chunk:
                    raise EOFError(f"Unexpected EOF while skipping {self._filename}")
                self._actual_crc = zlib.crc32(chunk, self._actual_crc)
                self._remaining -= len(chunk)
            self._check_crc()
            self._closed = True
        super().close()


class RarStreamReader(BaseRarReader):
    """Reader for RAR archives using the solid stream reader."""

    def __init__(self, archive_path: str):
        super().__init__(archive_path)
        self._proc = None
        self._stream: IO[bytes]
        self._lock = threading.Lock()
        self._active_member = None
        self._active_index = -1

        try:
            # Open an unrar process that outputs the contents of all files in the archive to stdout.
            self._proc = subprocess.Popen(
                ["unrar", "p", "-inul", archive_path],
                stdout=subprocess.PIPE,
                bufsize=1024 * 1024,
            )
            if self._proc.stdout is None:
                raise RuntimeError("Could not open unrar output stream")
            self._stream = self._proc.stdout # type: ignore
        except Exception as e:
            raise ArchiveError(f"Error opening RAR archive {archive_path}: {e}")

    def close(self) -> None:
        if self._active_member:
            self._active_member.close()
        if self._stream:
            self._stream.close()
        if self._proc:
            self._proc.wait()

    def _get_member_file(self, member: ArchiveMember) -> RarSolidMemberFile:
        return RarSolidMemberFile(member, self._stream, self._lock)

    def open(self, member: ArchiveMember) -> IO[bytes]:
        if self._archive is None or self._members is None:
            raise ValueError("Archive is closed")

        try:
            index = self._members.index(member)
        except ValueError:
            raise ValueError("Requested member is not part of this archive")

        if index <= self._active_index:
            raise ValueError(
                f"Cannot re-open already closed/skipped file: {member.filename}"
            )

        # Drain previous active file
        if self._active_member:
            self._active_member.close()

        # Skip any intermediate files between last read and this one
        for i in range(self._active_index + 1, index):
            self._get_member_file(self._members[i]).close()

        f = self._get_member_file(member)
        self._active_member = f
        self._active_index = index
        return f
