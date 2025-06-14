import copy
import os
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional

# import lz4.frame
# import zstandard
from archivey.types import ArchiveFormat, MemberType


class GenerationMethod(Enum):
    ZIPFILE = "zipfile"
    INFOZIP = "infozip"
    TAR_COMMAND_LINE = "tar_cmd"
    TAR_LIBRARY = "tarfile"
    PY7ZR = "py7zr"
    SEVENZIP_COMMAND_LINE = "7z_cmd"
    RAR_COMMAND_LINE = "rar_cmd"
    SINGLE_FILE_COMMAND_LINE = "single_file_cmd"
    SINGLE_FILE_LIBRARY = "single_file_lib"
    ISO_PYCDLIB = "iso_pycdlib"
    ISO_GENISOIMAGE = "iso_genisoimage"

    EXTERNAL = "external"


@dataclass
class FileInfo:
    name: str
    mtime: datetime
    contents: bytes | None = None
    password: str | None = None
    comment: str | None = None
    type: MemberType = MemberType.FILE
    link_target: str | None = None
    link_target_type: MemberType | None = MemberType.FILE
    compression_method: str | None = None
    permissions: Optional[int] = None


@dataclass
class ArchiveContents:
    file_basename: str  # Base name for the archive (e.g., "basic", "encryption")
    files: list[FileInfo]  # List of files to include
    archive_comment: str | None = None  # Optional archive comment
    solid: Optional[bool] = None  # Whether archive should be solid
    header_password: str | None = None  # Optional header password
    generate_corrupted_variants: bool = (
        True  # Whether to generate corrupted variants for testing
    )

    def has_password(self) -> bool:
        return (
            any(f.password is not None for f in self.files)
            or self.header_password is not None
        )

    def has_password_in_files(self) -> bool:
        return any(f.password is not None for f in self.files)

    def has_multiple_passwords(self) -> bool:
        return len({f.password for f in self.files if f.password is not None}) > 1


@dataclass(frozen=True)
class ArchiveFormatFeatures:
    dir_entries: bool = True
    file_comments: bool = False
    archive_comment: bool = False
    mtime: bool = True
    rounded_mtime: bool = False
    file_size: bool = True
    duplicate_files: bool = False
    hardlink_mtime: bool = False


DEFAULT_FORMAT_FEATURES = ArchiveFormatFeatures()


@dataclass(frozen=True)
class ArchiveCreationInfo:
    file_suffix: str  # e.g., ".zip", "py7zr.7z", "7zcmd.7z"
    format: ArchiveFormat  # The archive format enum
    generation_method: GenerationMethod  # How to generate the archive
    generation_method_options: dict[str, Any] = field(
        default_factory=dict
    )  # Additional options for generation
    features: ArchiveFormatFeatures = DEFAULT_FORMAT_FEATURES


DEFAULT_ARCHIVES_BASE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..")
)

TEST_ARCHIVES_DIR = "test_archives"
TEST_ARCHIVES_EXTERNAL_DIR = "test_archives_external"


@dataclass
class SampleArchive:
    # Will be constructed as f"{contents.file_basename}__{format.file_suffix}"
    filename: str

    contents: ArchiveContents
    creation_info: ArchiveCreationInfo
    skip_test: bool = False

    def get_archive_name(self, variant: str | None = None) -> str:
        if variant is None:
            return self.filename
        first_dot = self.filename.find(".")
        if first_dot == -1:
            name = self.filename
            ext = ""
        else:
            name = self.filename[:first_dot]
            ext = self.filename[first_dot:]
        return f"{name}.{variant}{ext}"

    def get_archive_path(
        self, base_dir: str = DEFAULT_ARCHIVES_BASE_DIR, variant: str | None = None
    ) -> str:
        name = self.get_archive_name(variant)
        if self.creation_info.generation_method == GenerationMethod.EXTERNAL:
            return os.path.join(base_dir, TEST_ARCHIVES_EXTERNAL_DIR, name)
        else:
            return os.path.join(base_dir, TEST_ARCHIVES_DIR, name)


# Generation method constants
ZIP_ZIPFILE_STORE = ArchiveCreationInfo(
    file_suffix="zipfile_store.zip",
    format=ArchiveFormat.ZIP,
    generation_method=GenerationMethod.ZIPFILE,
    features=ArchiveFormatFeatures(
        file_comments=True,
        archive_comment=True,
        rounded_mtime=True,
        duplicate_files=True,
    ),
    generation_method_options={"compression_method": "store"},
)
ZIP_ZIPFILE_DEFLATE = ArchiveCreationInfo(
    file_suffix="zipfile_deflate.zip",
    format=ArchiveFormat.ZIP,
    generation_method=GenerationMethod.ZIPFILE,
    features=ArchiveFormatFeatures(
        file_comments=True,
        archive_comment=True,
        rounded_mtime=True,
        duplicate_files=True,
    ),
    generation_method_options={"compression_method": "deflate"},
)
ZIP_INFOZIP = ArchiveCreationInfo(
    file_suffix="infozip.zip",
    format=ArchiveFormat.ZIP,
    generation_method=GenerationMethod.INFOZIP,
    # Times are not rounded, as infozip adds the timestamps extra field
    features=ArchiveFormatFeatures(
        file_comments=True, archive_comment=True, rounded_mtime=False
    ),
)

# 7z formats
SEVENZIP_PY7ZR = ArchiveCreationInfo(
    file_suffix="py7zr.7z",
    format=ArchiveFormat.SEVENZIP,
    generation_method=GenerationMethod.PY7ZR,
    features=ArchiveFormatFeatures(
        dir_entries=False,
        archive_comment=True,
        duplicate_files=True,
    ),
)
SEVENZIP_7ZCMD = ArchiveCreationInfo(
    file_suffix="7zcmd.7z",
    format=ArchiveFormat.SEVENZIP,
    generation_method=GenerationMethod.SEVENZIP_COMMAND_LINE,
    features=ArchiveFormatFeatures(dir_entries=False, archive_comment=True),
)

# RAR format
RAR_CMD = ArchiveCreationInfo(
    file_suffix=".rar",
    format=ArchiveFormat.RAR,
    generation_method=GenerationMethod.RAR_COMMAND_LINE,
    features=ArchiveFormatFeatures(dir_entries=True, archive_comment=True),
)

_TAR_FORMAT_FEATURES_TARCMD = ArchiveFormatFeatures()
_TAR_FORMAT_FEATURES_TARFILE = ArchiveFormatFeatures(
    duplicate_files=True, hardlink_mtime=True
)

# TAR formats
TAR_PLAIN_CMD = ArchiveCreationInfo(
    file_suffix="tarcmd.tar",
    format=ArchiveFormat.TAR,
    generation_method=GenerationMethod.TAR_COMMAND_LINE,
    features=_TAR_FORMAT_FEATURES_TARCMD,
)

# TAR formats
TAR_PLAIN_TARFILE = ArchiveCreationInfo(
    file_suffix="tarfile.tar",
    format=ArchiveFormat.TAR,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)

TAR_GZ_CMD = ArchiveCreationInfo(
    file_suffix="tarcmd.tar.gz",
    format=ArchiveFormat.TAR_GZ,
    generation_method=GenerationMethod.TAR_COMMAND_LINE,
    features=_TAR_FORMAT_FEATURES_TARCMD,
)
TAR_GZ_TARFILE = ArchiveCreationInfo(
    file_suffix="tarfile.tar.gz",
    format=ArchiveFormat.TAR_GZ,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)
TAR_ZSTD_CMD = ArchiveCreationInfo(
    file_suffix="tarcmd.tar.zst",
    format=ArchiveFormat.TAR_ZSTD,
    generation_method=GenerationMethod.TAR_COMMAND_LINE,
    features=_TAR_FORMAT_FEATURES_TARCMD,
)
TAR_ZSTD_TARFILE = ArchiveCreationInfo(
    file_suffix="tarfile.tar.zst",
    format=ArchiveFormat.TAR_ZSTD,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)

# No need to test both tarfile and cmdline for the other formats, as there shouldn't
# be significant differences that won't be caught by the gz format.
TAR_BZ2 = ArchiveCreationInfo(
    file_suffix=".tar.bz2",
    format=ArchiveFormat.TAR_BZ2,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)
TAR_XZ = ArchiveCreationInfo(
    file_suffix=".tar.xz",
    format=ArchiveFormat.TAR_XZ,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)
TAR_LZ4 = ArchiveCreationInfo(
    file_suffix=".tar.lz4",
    format=ArchiveFormat.TAR_LZ4,
    generation_method=GenerationMethod.TAR_LIBRARY,
    features=_TAR_FORMAT_FEATURES_TARFILE,
)

# Single file compression formats
GZIP_CMD = ArchiveCreationInfo(
    file_suffix="cmd.gz",
    format=ArchiveFormat.GZIP,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    # Dp not preserve filename and timestamp
    generation_method_options={"compression_cmd": "gzip", "cmd_args": ["-n"]},
    features=ArchiveFormatFeatures(file_size=True),
)
GZIP_CMD_PRESERVE_METADATA = ArchiveCreationInfo(
    file_suffix="cmd.gz",
    format=ArchiveFormat.GZIP,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    generation_method_options={"compression_cmd": "gzip", "cmd_args": ["-N"]},
    features=ArchiveFormatFeatures(file_size=True),
)

BZIP2_CMD = ArchiveCreationInfo(
    file_suffix="cmd.bz2",
    format=ArchiveFormat.BZIP2,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    generation_method_options={"compression_cmd": "bzip2"},
    features=ArchiveFormatFeatures(file_size=False),
)
XZ_CMD = ArchiveCreationInfo(
    file_suffix="cmd.xz",
    format=ArchiveFormat.XZ,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    generation_method_options={"compression_cmd": "xz"},
    features=ArchiveFormatFeatures(file_size=True),
)
ZSTD_CMD = ArchiveCreationInfo(
    file_suffix="cmd.zst",
    format=ArchiveFormat.ZSTD,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    generation_method_options={"compression_cmd": "zstd"},
    features=ArchiveFormatFeatures(file_size=False),
)
LZ4_CMD = ArchiveCreationInfo(
    file_suffix="cmd.lz4",
    format=ArchiveFormat.LZ4,
    generation_method=GenerationMethod.SINGLE_FILE_COMMAND_LINE,
    generation_method_options={"compression_cmd": "lz4"},
    features=ArchiveFormatFeatures(file_size=False),
)

GZIP_LIBRARY = ArchiveCreationInfo(
    file_suffix="lib.gz",
    format=ArchiveFormat.GZIP,
    generation_method=GenerationMethod.SINGLE_FILE_LIBRARY,
    generation_method_options={"opener_kwargs": {"mtime": 0}},
    features=ArchiveFormatFeatures(file_size=True),
)
BZIP2_LIBRARY = ArchiveCreationInfo(
    file_suffix="lib.bz2",
    format=ArchiveFormat.BZIP2,
    generation_method=GenerationMethod.SINGLE_FILE_LIBRARY,
    features=ArchiveFormatFeatures(file_size=False),
)
XZ_LIBRARY = ArchiveCreationInfo(
    file_suffix="lib.xz",
    format=ArchiveFormat.XZ,
    generation_method=GenerationMethod.SINGLE_FILE_LIBRARY,
    features=ArchiveFormatFeatures(file_size=True),
)
ZSTD_LIBRARY = ArchiveCreationInfo(
    file_suffix="lib.zst",
    format=ArchiveFormat.ZSTD,
    generation_method=GenerationMethod.SINGLE_FILE_LIBRARY,
    features=ArchiveFormatFeatures(file_size=False),
)
LZ4_LIBRARY = ArchiveCreationInfo(
    file_suffix="lib.lz4",
    format=ArchiveFormat.LZ4,
    generation_method=GenerationMethod.SINGLE_FILE_LIBRARY,
    features=ArchiveFormatFeatures(file_size=False),
)

# ISO format
ISO_PYCDLIB = ArchiveCreationInfo(
    file_suffix="pycdlib.iso",
    format=ArchiveFormat.ISO,
    generation_method=GenerationMethod.ISO_PYCDLIB,
)
ISO_GENISOIMAGE = ArchiveCreationInfo(
    file_suffix="genisoimage.iso",
    format=ArchiveFormat.ISO,
    generation_method=GenerationMethod.ISO_GENISOIMAGE,
)

ALL_SINGLE_FILE_FORMATS = [
    GZIP_CMD,
    BZIP2_CMD,
    XZ_CMD,
    ZSTD_CMD,
    LZ4_CMD,
    GZIP_LIBRARY,
    BZIP2_LIBRARY,
    XZ_LIBRARY,
    ZSTD_LIBRARY,
    LZ4_LIBRARY,
]

BASIC_TAR_FORMATS = [
    TAR_PLAIN_CMD,
    TAR_PLAIN_TARFILE,
    TAR_GZ_CMD,
    TAR_GZ_TARFILE,
    TAR_ZSTD_CMD,
    TAR_ZSTD_TARFILE,
]

ALL_TAR_FORMATS = BASIC_TAR_FORMATS + [
    TAR_BZ2,
    TAR_XZ,
    TAR_LZ4,
]

ZIP_FORMATS = [
    ZIP_ZIPFILE_STORE,
    ZIP_ZIPFILE_DEFLATE,
    ZIP_INFOZIP,
]

RAR_FORMATS = [
    RAR_CMD,
]

SEVENZIP_FORMATS = [
    SEVENZIP_PY7ZR,
    SEVENZIP_7ZCMD,
]

ISO_FORMATS = [
    ISO_PYCDLIB,
    ISO_GENISOIMAGE,
]

ZIP_RAR_7Z_FORMATS = ZIP_FORMATS + RAR_FORMATS + SEVENZIP_FORMATS

# Skip test filenames
SKIP_TEST_FILENAMES = set(
    # "basic_nonsolid__genisoimage.iso",
    # "basic_nonsolid__pycdlib.iso",
)


def _create_random_data(size: int, seed: int, chars: bytes = b"0123456789 ") -> bytes:
    r = random.Random(seed)
    memview = memoryview(bytearray(size))
    for i in range(size):
        memview[i] = r.choice(chars)
    return memview.tobytes()


def _fake_mtime(i: int) -> datetime:
    def _mod_1(i: int, mod: int) -> int:
        return (i - 1) % mod + 1

    if i == 0:
        return datetime(1980, 1, 1, 0, 0, 0)

    return datetime(
        2000 + i, _mod_1(i, 12), _mod_1(i, 28), i % 24, (i + 1) % 60, (i + 2) % 60
    )


BASIC_FILES = [
    # Use odd seconds to test that the ZIP extended timestamp is being read correctly
    # (as the standard timestamp is rounded to the nearest 2 seconds)
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(1),
        contents=b"Hello, world!",
    ),
    FileInfo(
        name="subdir/",
        mtime=_fake_mtime(2),
        type=MemberType.DIR,
    ),
    FileInfo(
        name="empty_file.txt",
        mtime=_fake_mtime(3),
        contents=b"",
    ),
    FileInfo(
        name="empty_subdir/",
        mtime=_fake_mtime(4),
        type=MemberType.DIR,
    ),
    FileInfo(
        name="subdir/file2.txt",
        mtime=_fake_mtime(5),
        contents=b"Hello, universe!",
    ),
    FileInfo(
        name="implicit_subdir/file3.txt",
        mtime=_fake_mtime(6),
        contents=b"Hello there!",
    ),
]


COMMENT_FILES = [
    FileInfo(
        name="abc.txt",
        mtime=_fake_mtime(1),
        contents=b"ABC",
        comment="Contains some letters",
    ),
    FileInfo(
        name="subdir/",
        mtime=_fake_mtime(7),
        type=MemberType.DIR,
        comment="Contains some files",
    ),
    FileInfo(
        name="subdir/123.txt",
        mtime=_fake_mtime(8),
        contents=b"1234567890",
        comment="Contains some numbers",
    ),
]

ENCRYPTION_SEVERAL_PASSWORDS_FILES = [
    FileInfo(
        name="plain.txt",
        mtime=_fake_mtime(1),
        contents=b"This is plain",
    ),
    # For 7zip archives to be considered solid, they need to have at least two files
    # in the same folder. To make that possible, we need two consecutive files with the
    # same password.
    FileInfo(
        name="secret.txt",
        mtime=_fake_mtime(2),
        contents=b"This is secret",
        password="password",
    ),
    FileInfo(
        name="also_secret.txt",
        mtime=_fake_mtime(3),
        contents=b"This is also secret",
        password="password",
    ),
    FileInfo(
        name="not_secret.txt",
        mtime=_fake_mtime(4),
        contents=b"This is not secret",
        comment="Contains some information",
    ),
    FileInfo(
        name="very_secret.txt",
        contents=b"This is very secret",
        mtime=_fake_mtime(5),
        password="very_secret_password",
        comment="Contains some very secret information",
    ),
]

ENCRYPTION_SINGLE_PASSWORD_FILES = [
    FileInfo(
        name="secret.txt",
        mtime=_fake_mtime(1),
        contents=b"This is secret",
        password="password",
    ),
    FileInfo(
        name="also_secret.txt",
        mtime=_fake_mtime(2),
        contents=b"This is also secret",
        password="password",
    ),
]

ENCRYPTION_ENCRYPTED_AND_PLAIN_FILES = ENCRYPTION_SINGLE_PASSWORD_FILES + [
    FileInfo(
        name="not_secret.txt",
        mtime=_fake_mtime(3),
        contents=b"This is not secret",
    ),
]

SYMLINKS_FILES = [
    FileInfo(name="file1.txt", contents=b"Hello, world!", mtime=_fake_mtime(1)),
    FileInfo(
        name="symlink_to_file1.txt",
        mtime=_fake_mtime(2),
        type=MemberType.SYMLINK,
        link_target="file1.txt",
        contents=b"Hello, world!",
    ),
    FileInfo(
        name="subdir/",
        mtime=_fake_mtime(3),
        type=MemberType.DIR,
    ),
    FileInfo(
        name="subdir/link_to_file1.txt",
        mtime=_fake_mtime(4),
        type=MemberType.SYMLINK,
        link_target="../file1.txt",
        contents=b"Hello, world!",
    ),
    FileInfo(
        name="subdir_link",
        mtime=_fake_mtime(5),
        type=MemberType.SYMLINK,
        link_target="subdir",
        link_target_type=MemberType.DIR,
    ),
]

SYMLINK_LOOP_FILES = [
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(1),
        type=MemberType.SYMLINK,
        link_target="file2.txt",
    ),
    FileInfo(
        name="file2.txt",
        mtime=_fake_mtime(2),
        type=MemberType.SYMLINK,
        link_target="file3.txt",
    ),
    FileInfo(
        name="file3.txt",
        mtime=_fake_mtime(3),
        type=MemberType.SYMLINK,
        link_target="file1.txt",
    ),
    FileInfo(
        name="file4.txt",
        mtime=_fake_mtime(4),
        type=MemberType.SYMLINK,
        link_target="file5.txt",
        contents=b"this is file 5",
    ),
    FileInfo(
        name="file5.txt",
        mtime=_fake_mtime(5),
        contents=b"this is file 5",
    ),
]

HARDLINKS_FILES = [
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(1),
        contents=b"Hello 1!",
    ),
    FileInfo(
        name="subdir/file2.txt",
        mtime=_fake_mtime(2),
        contents=b"Hello 2!",
    ),
    FileInfo(
        name="subdir/hardlink_to_file1.txt",
        mtime=_fake_mtime(3),
        type=MemberType.HARDLINK,
        link_target="file1.txt",
        contents=b"Hello 1!",
    ),
    FileInfo(
        name="hardlink_to_file2.txt",
        mtime=_fake_mtime(4),
        type=MemberType.HARDLINK,
        link_target="subdir/file2.txt",
        contents=b"Hello 2!",
    ),
]


# In tar archives, a hard link refers to the entry with the same name previously in
# the archive, even if that entry is later overwritten. So in this case, the first
# hard link should refer to the first file version, and the second hard link should
# refer to the second file version.
HARDLINKS_WITH_DUPLICATE_FILES = [
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(1),
        contents=b"Old contents",
    ),
    FileInfo(
        name="hardlink_to_file1_old.txt",
        mtime=_fake_mtime(2),
        type=MemberType.HARDLINK,
        link_target="file1.txt",
        contents=b"Old contents",
    ),
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(3),
        contents=b"New contents!",
    ),
    FileInfo(
        name="hardlink_to_file1_new.txt",
        mtime=_fake_mtime(4),
        type=MemberType.HARDLINK,
        link_target="file1.txt",
        contents=b"New contents!",
    ),
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(5),
        contents=b"Newer contents!!",
    ),
]

HARDLINKS_RECURSIVE_AND_BROKEN = [
    FileInfo(
        name="a_file.txt",
        mtime=_fake_mtime(1),
        contents=b"Hello!",
    ),
    FileInfo(
        name="b_broken_forward_hardlink.txt",
        mtime=_fake_mtime(2),
        type=MemberType.HARDLINK,
        link_target="d_hardlink.txt",
        # This is a broken hardlink, as the target is not earlier in the archive.
    ),
    FileInfo(
        name="c_forward_symlink.txt",
        mtime=_fake_mtime(3),
        type=MemberType.SYMLINK,
        link_target="d_hardlink.txt",
        contents=b"Hello!",
    ),
    FileInfo(
        name="d_hardlink.txt",
        mtime=_fake_mtime(4),
        type=MemberType.HARDLINK,
        link_target="a_file.txt",
        contents=b"Hello!",
    ),
    FileInfo(
        name="e_double_hardlink.txt",
        mtime=_fake_mtime(5),
        type=MemberType.HARDLINK,
        link_target="d_hardlink.txt",
        contents=b"Hello!",
    ),
    FileInfo(
        name="f_hardlink_to_broken.txt",
        mtime=_fake_mtime(6),
        type=MemberType.HARDLINK,
        link_target="b_broken_forward_hardlink.txt",
    ),
    FileInfo(
        name="g_symlink_to_broken.txt",
        mtime=_fake_mtime(7),
        type=MemberType.SYMLINK,
        link_target="b_broken_forward_hardlink.txt",
    ),
    # Sometimes tar files can contain hardlinks to the same file (particularly if we
    # call tar with the filename twice in the command line)
    FileInfo(
        name="a_file.txt",
        mtime=_fake_mtime(8),
        type=MemberType.HARDLINK,
        link_target="a_file.txt",
        contents=b"Hello!",
    ),
]


ENCODING_FILES = [
    FileInfo(
        name="Español.txt",
        contents=b"Hola, mundo!",
        mtime=_fake_mtime(1),
    ),
    FileInfo(
        name="Català.txt",
        contents="Hola, món!".encode("utf-8"),
        mtime=_fake_mtime(1),
    ),
    FileInfo(
        name="Português.txt",
        contents="Olá, mundo!".encode("utf-8"),
        mtime=_fake_mtime(1),
    ),
    FileInfo(
        name="emoji_😀.txt",
        contents=b"I'm happy",
        mtime=_fake_mtime(1),
    ),
]

COMPRESSION_METHODS_FILES = [
    FileInfo(
        name="store.txt",
        contents=b"I am stored\n" * 1000,
        mtime=_fake_mtime(1),
        compression_method="store",
    ),
    FileInfo(
        name="deflate.txt",
        contents=b"I am deflated\n" * 1000,
        mtime=_fake_mtime(2),
        compression_method="deflate",
    ),
    FileInfo(
        name="bzip2.txt",
        contents=b"I am bzip'd\n" * 1000,
        mtime=_fake_mtime(3),
        compression_method="bzip2",
    ),
]

COMPRESSION_METHOD_FILES_LZMA = COMPRESSION_METHODS_FILES + [
    FileInfo(
        name="lzma.txt",
        contents=b"I am lzma'd\n" * 1000,
        mtime=_fake_mtime(4),
        compression_method="lzma",
    ),
]

MARKER_FILENAME_BASED_ON_ARCHIVE_NAME = "SINGLE_FILE_MARKER"
MARKER_MTIME_BASED_ON_ARCHIVE_NAME = datetime(3141, 5, 9, 2, 6, 53)

# Single compressed files (e.g. .gz, .bz2, .xz)
SINGLE_FILE_TXT_CONTENT = b"This is a single test file for compression.\n"
SINGLE_FILE_INFO_FIXED_FILENAME_AND_MTIME = FileInfo(
    name="single_file_fixed.txt",
    mtime=_fake_mtime(1),
    contents=SINGLE_FILE_TXT_CONTENT,
)
SINGLE_FILE_INFO_NO_METADATA = FileInfo(
    name=MARKER_FILENAME_BASED_ON_ARCHIVE_NAME,
    mtime=MARKER_MTIME_BASED_ON_ARCHIVE_NAME,
    contents=SINGLE_FILE_TXT_CONTENT,
)

TEST_PERMISSIONS_FILES = [
    FileInfo(
        name="standard.txt",
        mtime=_fake_mtime(1),
        contents=b"Standard permissions.",
        permissions=0o644,
    ),
    FileInfo(
        name="readonly.txt",
        mtime=_fake_mtime(2),
        contents=b"Read-only permissions.",
        permissions=0o444,
    ),
    FileInfo(
        name="executable.sh",
        mtime=_fake_mtime(3),
        contents=b"#!/bin/sh\necho 'Executable permissions.'",
        permissions=0o755,
    ),
    FileInfo(
        name="world_readable.txt",
        mtime=_fake_mtime(4),
        contents=b"World readable permissions.",
        permissions=0o666,
    ),
]

LARGE_FILES = [
    FileInfo(
        name=f"large{i}.txt",
        contents=f"Large file #{i}\n".encode() + _create_random_data(200000, i),
        mtime=_fake_mtime(i),
    )
    for i in range(1, 6)
]

SINGLE_LARGE_FILE = FileInfo(
    name=MARKER_FILENAME_BASED_ON_ARCHIVE_NAME,
    contents=_create_random_data(1000000, 1),
    mtime=MARKER_MTIME_BASED_ON_ARCHIVE_NAME,
)

DUPLICATE_FILES = [
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(1),
        contents=b"Old contents",  # len: 12, CRC: e8c902a6
    ),
    FileInfo(
        name="file2.txt",
        mtime=_fake_mtime(2),
        contents=b"Duplicate contents",
    ),
    FileInfo(
        name="file1.txt",
        mtime=_fake_mtime(3),
        contents=b"New contents!",  # len: 13, CRC: d61d71d2
    ),
    # Might get turned into a link or reference
    FileInfo(
        name="file2_dupe.txt",
        mtime=_fake_mtime(4),
        contents=b"Duplicate contents",
    ),
]


def build_archive_infos() -> list[SampleArchive]:
    """Build all ArchiveInfo objects from the definitions."""
    archives = []
    for contents, format_infos in ARCHIVE_DEFINITIONS:
        for format_info in format_infos:
            filename = f"{contents.file_basename}__{format_info.file_suffix}"
            archive_info = SampleArchive(
                filename=filename,
                contents=contents,
                creation_info=format_info,
                skip_test=filename in SKIP_TEST_FILENAMES,
            )

            if any(
                MARKER_FILENAME_BASED_ON_ARCHIVE_NAME in a.name
                for a in archive_info.contents.files
            ):
                archive_info.contents = copy.deepcopy(archive_info.contents)
                for file in archive_info.contents.files:
                    if file.name == MARKER_FILENAME_BASED_ON_ARCHIVE_NAME:
                        archive_name_without_ext = os.path.splitext(
                            archive_info.filename
                        )[0]
                        file.name = archive_name_without_ext

            archives.append(archive_info)

    # Verify all skip test filenames were created
    created_filenames = {a.filename for a in archives}
    missing_skip_tests = SKIP_TEST_FILENAMES - created_filenames
    if missing_skip_tests:
        raise ValueError(
            f"Some skip test filenames were not created: {missing_skip_tests}. Created filenames: {created_filenames}"
        )

    return archives


def filter_archives(
    archives: list[SampleArchive],
    prefixes: list[str] | None = None,
    extensions: list[str] | None = None,
    custom_filter: Callable[[SampleArchive], bool] | None = None,
) -> list[SampleArchive]:
    """Filter archives by filename prefixes and/or extensions."""

    if prefixes:
        filtered = []
        for prefix in prefixes:
            prefix_found = False
            for a in archives:
                if a.filename.startswith(prefix + "__"):
                    filtered.append(a)
                    prefix_found = True
            if not prefix_found:
                raise ValueError(f"No archives match prefix {prefix}")
    else:
        filtered = archives

    if extensions:
        filtered = [
            a for a in filtered if any(a.filename.endswith(e) for e in extensions)
        ]

    if custom_filter:
        filtered = [a for a in filtered if custom_filter(a)]

    if not filtered:
        raise ValueError("No archives match the filter criteria")

    return filtered


# Archive definitions
ARCHIVE_DEFINITIONS: list[tuple[ArchiveContents, list[ArchiveCreationInfo]]] = [
    (
        ArchiveContents(
            file_basename="basic_nonsolid",
            files=BASIC_FILES,
        ),
        ZIP_RAR_7Z_FORMATS,  # + ISO_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="basic_solid",
            files=BASIC_FILES,
            solid=True,
        ),
        RAR_FORMATS + SEVENZIP_FORMATS + ALL_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="comment",
            files=COMMENT_FILES,
            archive_comment="This is a\nmulti-line comment",
        ),
        ZIP_FORMATS + RAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encryption",
            files=ENCRYPTION_SINGLE_PASSWORD_FILES,
            solid=False,
        ),
        # Zipfile library doesn't support writing encrypted archives.
        [ZIP_INFOZIP] + RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encryption_several_passwords",
            files=ENCRYPTION_SEVERAL_PASSWORDS_FILES,
            solid=False,
        ),
        # Zipfile library doesn't support writing encrypted archives.
        [ZIP_INFOZIP] + RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encryption_with_plain",
            files=ENCRYPTION_ENCRYPTED_AND_PLAIN_FILES,
            solid=False,
        ),
        # Zipfile library doesn't support writing encrypted archives.
        [ZIP_INFOZIP] + RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encryption_solid",
            files=ENCRYPTION_SINGLE_PASSWORD_FILES,
            solid=True,
        ),
        RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encrypted_header",
            files=BASIC_FILES,
            header_password="header_password",
        ),
        RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encrypted_header_solid",
            files=BASIC_FILES,
            solid=True,
            header_password="header_password",
        ),
        RAR_FORMATS + SEVENZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="symlinks",
            files=SYMLINKS_FILES,
            solid=False,
        ),
        ZIP_RAR_7Z_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="symlinks_solid",
            files=SYMLINKS_FILES,
            solid=True,
        ),
        RAR_FORMATS + SEVENZIP_FORMATS + ALL_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="hardlinks_nonsolid",
            files=HARDLINKS_FILES,
        ),
        RAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="hardlinks_solid",
            files=HARDLINKS_FILES,
            solid=True,
        ),
        RAR_FORMATS + BASIC_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="hardlinks_with_duplicate_files",
            files=HARDLINKS_WITH_DUPLICATE_FILES,
        ),
        [TAR_PLAIN_TARFILE],  # , TAR_GZ_TARFILE],
    ),
    (
        ArchiveContents(
            file_basename="hardlinks_recursive_and_broken",
            files=HARDLINKS_RECURSIVE_AND_BROKEN,
        ),
        [TAR_PLAIN_TARFILE, TAR_GZ_TARFILE],
    ),
    (
        ArchiveContents(
            file_basename="encoding",
            files=ENCODING_FILES,
        ),
        ZIP_RAR_7Z_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encoding",
            files=ENCODING_FILES,
            solid=True,
        ),
        BASIC_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="encoding_comment",
            files=ENCODING_FILES,
            archive_comment="Comentário em português 😀",
        ),
        ZIP_FORMATS + RAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="compression_methods",
            files=COMPRESSION_METHODS_FILES,
        ),
        ZIP_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="compression_methods_lzma",
            files=COMPRESSION_METHOD_FILES_LZMA,
        ),
        [ZIP_ZIPFILE_STORE],  # Infozip doesn't support lzma
    ),
    (
        ArchiveContents(
            file_basename="single_file_with_metadata",
            files=[SINGLE_FILE_INFO_FIXED_FILENAME_AND_MTIME],
        ),
        [GZIP_CMD_PRESERVE_METADATA],
    ),
    (
        ArchiveContents(
            file_basename="single_file",
            files=[SINGLE_FILE_INFO_NO_METADATA],
        ),
        ALL_SINGLE_FILE_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="permissions",
            files=TEST_PERMISSIONS_FILES,
        ),
        ZIP_RAR_7Z_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="permissions_solid",
            files=TEST_PERMISSIONS_FILES,
            solid=True,
        ),
        BASIC_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="large_files_nonsolid",
            files=LARGE_FILES,
        ),
        ZIP_RAR_7Z_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="large_files_solid",
            files=LARGE_FILES,
            solid=True,
        ),
        RAR_FORMATS + SEVENZIP_FORMATS + ALL_TAR_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="large_single_file",
            files=[SINGLE_LARGE_FILE],
        ),
        ALL_SINGLE_FILE_FORMATS,
    ),
    (
        ArchiveContents(
            file_basename="fixture_zip",
            files=[
                FileInfo(
                    name="fixture.txt",
                    mtime=_fake_mtime(1),
                    contents=b"fixture zip",
                )
            ],
        ),
        [ZIP_ZIPFILE_STORE],
    ),
    (
        ArchiveContents(
            file_basename="fixture_tar",
            files=[
                FileInfo(
                    name="fixture.txt",
                    mtime=_fake_mtime(1),
                    contents=b"fixture tar",
                )
            ],
            solid=True,
        ),
        [TAR_PLAIN_TARFILE],
    ),
    (
        ArchiveContents(
            file_basename="symlink_loop",
            files=SYMLINK_LOOP_FILES,
        ),
        [ZIP_INFOZIP, TAR_PLAIN_TARFILE],
    ),
    # (
    #     ArchiveContents(
    #         file_basename="basic_iso",
    #         files=BASIC_FILES,
    #     ),
    #     ISO_FORMATS,
    # ),
    (
        ArchiveContents(
            file_basename="duplicate_files",
            files=DUPLICATE_FILES,
        ),
        ZIP_RAR_7Z_FORMATS + [TAR_PLAIN_TARFILE, TAR_GZ_TARFILE],
    ),
]

# Build all archive infos
SAMPLE_ARCHIVES = build_archive_infos()

BASIC_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES,
    prefixes=["basic_nonsolid", "basic_solid"],
    custom_filter=lambda x: x.creation_info.format != ArchiveFormat.ISO,
)

DUPLICATE_FILES_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES,
    prefixes=["duplicate_files"],
    custom_filter=lambda x: x.creation_info.format != ArchiveFormat.ISO,
)


# TODO: add tests and fixes for:
#   - rar4 archives
#   - hard links (tar and rar)
#      - open() hard links should open the referenced file
#   - duplicate files:
#      - open(member) should open the correct file
#      - open(filename) should open the last file with that name
#   - filter function
#      - open() with filtered members should work
#   - encrypted files:
#      passing password in constructor should work
#      passing password in iter_members_with_io() should work
#      passing password in open() should work
#      passing wrong password in open() should raise an exception
#      passing wrong password in iter_members_with_io() should raise an exception only if trying to read the stream


# Currently failing with uncaught exception:
#  archivey tests/test_archives/encryption_several_passwords__7zcmd.7z  --password password --hide-progress
