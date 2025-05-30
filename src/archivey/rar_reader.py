import enum
import hashlib
import io
import logging
import stat
import subprocess
import threading
import zlib
from datetime import datetime
from typing import IO, Any, Iterable, Iterator, List, Optional, cast

import rarfile

from archivey.base_reader import ArchiveReader
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveError,
)
from archivey.formats import ArchiveFormat
from archivey.io_wrappers import ExceptionTranslatingIO
from archivey.types import ArchiveInfo, ArchiveMember, MemberType
from archivey.utils import bytes_to_str, str_to_bytes

logger = logging.getLogger(__name__)


_RAR_COMPRESSION_METHODS = {
    0x30: "store",
    0x31: "fastest",
    0x32: "fast",
    0x33: "normal",
    0x34: "good",
    0x35: "best",
}


class PasswordCheckResult(enum.Enum):
    CORRECT = 1
    INCORRECT = 2
    UNKNOWN = 3


def verify_rar5_password(
    password: bytes | None, rar_info: rarfile.RarInfo
) -> PasswordCheckResult:
    """
    Verifies whether the given password matches the check value in RAR5 encryption data.
    Returns True if the password is correct, False if not.
    """
    if not rar_info.needs_password():
        return PasswordCheckResult.CORRECT
    if password is None:
        return PasswordCheckResult.INCORRECT
    if not isinstance(rar_info, rarfile.Rar5Info):
        return (
            PasswordCheckResult.UNKNOWN
        )  # We can't know if the password is correct for non-RAR5 archives

    assert rar_info.file_encryption is not None, rar_info
    encdata = rar_info.file_encryption

    logger.info("Encdata: %s", encdata)
    (algo, flags, kdf_count, salt, iv, check_value) = encdata

    # Mostly copied from RAR5Parser._check_password
    RAR5_PW_CHECK_SIZE = 8
    RAR5_PW_SUM_SIZE = 4

    if len(check_value) != RAR5_PW_CHECK_SIZE + RAR5_PW_SUM_SIZE:
        return PasswordCheckResult.UNKNOWN  # Unnown algorithm

    hdr_check = check_value[:RAR5_PW_CHECK_SIZE]
    hdr_sum = check_value[RAR5_PW_CHECK_SIZE:]
    sum_hash = hashlib.sha256(hdr_check).digest()
    if sum_hash[:RAR5_PW_SUM_SIZE] != hdr_sum:
        # Unknown algorithm?
        return PasswordCheckResult.UNKNOWN

    kdf_count = (1 << kdf_count) + 32
    pwd_hash = hashlib.pbkdf2_hmac("sha256", password, salt, kdf_count)

    pwd_check = bytearray(RAR5_PW_CHECK_SIZE)
    len_mask = RAR5_PW_CHECK_SIZE - 1
    for i, v in enumerate(pwd_hash):
        pwd_check[i & len_mask] ^= v

    if pwd_check != hdr_check:
        return PasswordCheckResult.INCORRECT

    return PasswordCheckResult.CORRECT


class BaseRarReader(ArchiveReader):
    """Base class for RAR archive readers."""

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        super().__init__(ArchiveFormat.RAR)
        self.archive_path = archive_path
        self._members: Optional[list[ArchiveMember]] = None
        self._format_info: Optional[ArchiveInfo] = None

        try:
            self._archive = rarfile.RarFile(archive_path, "r")
            if pwd:
                self._archive.setpassword(pwd)
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(f"Invalid RAR archive {archive_path}: {e}")
        except rarfile.NotRarFile as e:
            raise ArchiveCorruptedError(f"Not a RAR archive {archive_path}: {e}")
        except rarfile.NeedFirstVolume as e:
            raise ArchiveError(
                f"Need first volume of multi-volume RAR archive {archive_path}: {e}"
            )
        except rarfile.RarWrongPassword as e:
            raise ArchiveEncryptedError(
                f"Wrong password specified for {archive_path}"
            ) from e

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

        # According to https://documentation.help/WinRAR/HELPArcEncryption.htm :
        # If "Encrypt file names" [i.e. header encryption] option is off,
        # file checksums for encrypted RAR 5.0 files are modified using a
        # special password dependent algorithm. [...] So do not expect checksums
        # for encrypted RAR 5.0 files to match actual CRC32 or BLAKE2 values.
        # If "Encrypt file names" option is on, checksums are stored without modification,
        # because they can be accessed only after providing a valid password.

        if self._members is None:
            self._members = []
            rarinfos: list[rarfile.RarInfo] = self._archive.infolist()
            for info in rarinfos:
                compression_method = (
                    _RAR_COMPRESSION_METHODS.get(info.compress_type, "unknown")
                    if info.compress_type is not None
                    else None
                )

                has_encrypted_crc: bool
                if isinstance(info, rarfile.Rar5Info):
                    enc_flags = info.file_encryption and info.file_encryption[1] or 0  # type: ignore
                    RAR_TWEAKED_CHECKSUMS_FLAG = 0x2
                    has_encrypted_crc = bool(enc_flags & RAR_TWEAKED_CHECKSUMS_FLAG)
                else:
                    has_encrypted_crc = False

                member = ArchiveMember(
                    filename=info.filename or "",  # Will never actually be None
                    file_size=info.file_size,
                    compress_size=info.compress_size,
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
                    mode=stat.S_IMODE(info.mode)
                    if hasattr(info, "mode") and isinstance(info.mode, int)
                    else None,
                    crc32=info.CRC if not has_encrypted_crc else None,
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

            has_header_encryption = (
                self._archive._file_parser is not None
                and self._archive._file_parser.has_header_encryption()
            )

            self._format_info = ArchiveInfo(
                format=self.get_format(),
                version=version,
                is_solid=self._archive.is_solid(),
                comment=self._archive.comment,
                extra={
                    # "is_multivolume": self._archive.is_multivolume(),
                    "needs_password": self._archive.needs_password(),
                    "header_encrypted": has_header_encryption,
                },
            )

        return self._format_info


class RarReader(BaseRarReader):
    """Reader for RAR archives using rarfile."""

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        super().__init__(archive_path, pwd=pwd)
        self._pwd = pwd

    def _exception_translator(self, e: Exception) -> Optional[Exception]:
        if isinstance(e, rarfile.BadRarFile):
            return ArchiveCorruptedError(f"Error reading member {self.archive_path}")
        return None

    def open(
        self, member: ArchiveMember, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        if member.encrypted:
            pwd_check = verify_rar5_password(
                str_to_bytes(pwd or self._pwd), cast(rarfile.RarInfo, member.raw_info)
            )
            logger.info(
                "Verifying password for %s and pwd %s: %s",
                member.filename,
                pwd,
                pwd_check,
            )

        if self._archive is None:
            raise ValueError("Archive is closed")

        try:
            # Apparently pwd can be either bytes or str
            inner = self._archive.open(member.filename, pwd=bytes_to_str(pwd))
            return ExceptionTranslatingIO(inner, self._exception_translator)  # type: ignore[arg-type]
        except rarfile.BadRarFile as e:
            raise ArchiveCorruptedError(
                f"Error reading member {member.filename}"
            ) from e
        except rarfile.RarWrongPassword as e:
            raise ArchiveEncryptedError(
                f"Wrong password specified for {member.filename}"
            ) from e
        except rarfile.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"Password required for {member.filename}"
            ) from e
        except rarfile.Error as e:
            raise ArchiveError(
                f"Unknown error reading member {member.filename}: {e}"
            ) from e


class CRCMismatchError(ArchiveCorruptedError):
    def __init__(self, filename: str, expected: int, actual: int):
        super().__init__(
            f"CRC mismatch in {filename}: expected {expected:08x}, got {actual:08x}"
        )


class RarStreamMemberFile(io.RawIOBase, IO[bytes]):
    def __init__(
        self, member: ArchiveMember, shared_stream: IO[bytes], lock: threading.Lock
    ):
        super().__init__()
        self._stream = shared_stream
        self._remaining = member.file_size
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

            logger.info(
                f"Read {len(data)} bytes from {self._filename}, {self._remaining} remaining: {data}"
            )
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
    """Reader for RAR archives using the solid stream reader.

    This may fail for non-solid archives where some files are encrypted and others not,
    or there are multiple passwords. If the password is incorrect for some files,
    they will be silently skipped, so the successfully output data will be associated
    with the wrong files. (ideally, use this only for solid archives, which are
    guaranteed to have the same password for all files)
    """

    def __init__(self, archive_path: str, *, pwd: bytes | str | None = None):
        super().__init__(archive_path, pwd=pwd)
        self._proc: subprocess.Popen | None = None
        self._stream: IO[bytes] | None = None
        self._lock = threading.Lock()
        self._active_member: RarStreamMemberFile | None = None
        self._active_index = -1
        self._pwd = bytes_to_str(pwd)
        self.archive_path = archive_path

    def close(self) -> None:
        if self._active_member:
            self._active_member.close()
            self._active_member = None
        if self._stream:
            self._stream.close()
            self._stream = None
        if self._proc:
            self._proc.wait()
            self._proc = None

    def _get_member_file(self, member: ArchiveMember) -> RarStreamMemberFile:
        assert self._stream is not None
        return RarStreamMemberFile(member, self._stream, self._lock)

    def _open_stream(self) -> None:
        try:
            # Open an unrar process that outputs the contents of all files in the archive to stdout.
            password_args = ["-p" + self._pwd] if self._pwd else ["-p-"]
            cmd = ["unrar", "p", "-inul", *password_args, self.archive_path]
            logger.info(
                f"Opening RAR archive {self.archive_path} with command: {' '.join(cmd)}"
            )
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                bufsize=1024 * 1024,
            )
            if self._proc.stdout is None:
                raise RuntimeError("Could not open unrar output stream")
            self._stream = self._proc.stdout  # type: ignore
        except Exception as e:
            raise ArchiveError(f"Error opening RAR archive {self.archive_path}: {e}")

    def open(
        self, member: ArchiveMember, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        if self._archive is None or self._members is None:
            raise ValueError("Archive is closed")

        if pwd is not None:
            pwd = bytes_to_str(pwd)
            if self._pwd is None:
                if self._stream is not None:
                    raise ValueError(
                        "RarStreamReader needs the password to be set in the constructor or first open() call"
                    )
                self._pwd = pwd

            elif pwd != self._pwd:
                raise ValueError("RarStreamReader does not support different passwords")

        if self._stream is None:
            self._open_stream()

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
