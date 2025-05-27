from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from archivey.types import CompressionFormat, MemberType


class GenerationMethod(Enum):
    ZIPFILE = "zipfile"
    INFOZIP = "zip_command_line"
    EXTERNAL = "external"
    TAR_COMMAND_LINE = "tar_command_line"


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


@dataclass
class ArchiveInfo:
    filename: str
    generation_method: GenerationMethod
    format: CompressionFormat
    files: list[FileInfo]
    archive_comment: str | None = None


def _fake_mtime(i: int) -> datetime:
    def _mod_1(i: int, mod: int) -> int:
        return (i - 1) % mod + 1

    years = [-1000, 1980, 1990, 2000, 2010]
    if i == 0:
        return datetime(1980, 1, 1, 0, 0, 0)

    if i < len(years):
        year = years[i]
    else:
        year = 2020 + (i - len(years))

    return datetime(
        year, _mod_1(i, 12), _mod_1(i, 28), i % 24, (i + 1) % 60, (i + 2) % 60
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
        name="empty_subdir/",
        mtime=_fake_mtime(3),
        type=MemberType.DIR,
    ),
    FileInfo(
        name="subdir/file2.txt",
        mtime=_fake_mtime(4),
        contents=b"Hello, universe!",
    ),
    FileInfo(
        name="implicit_subdir/file3.txt",
        mtime=_fake_mtime(5),
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

ENCRYPTION_FILES = [
    FileInfo(
        name="plain.txt",
        mtime=_fake_mtime(1),
        contents=b"This is plain",
    ),
    FileInfo(
        name="secret.txt",
        mtime=_fake_mtime(2),
        contents=b"This is secret",
        password="password",
    ),
    FileInfo(
        name="not_secret.txt",
        mtime=_fake_mtime(3),
        contents=b"This is not secret",
        comment="Contains some information",
    ),
    FileInfo(
        name="very_secret.txt",
        contents=b"This is very secret",
        mtime=_fake_mtime(4),
        password="very_secret_password",
        comment="Contains some very secret information",
    ),
]

SYMLINK_FILES = [
    FileInfo(name="file1.txt", contents=b"Hello, world!", mtime=_fake_mtime(1)),
    FileInfo(
        name="symlink_to_file1.txt",
        mtime=_fake_mtime(2),
        type=MemberType.LINK,
        link_target="file1.txt",
    ),
    FileInfo(
        name="subdir/",
        mtime=_fake_mtime(3),
        type=MemberType.DIR,
    ),
    FileInfo(
        name="subdir/link_to_file1.txt",
        mtime=_fake_mtime(4),
        type=MemberType.LINK,
        link_target="../file1.txt",
    ),
    FileInfo(
        name="subdir_link",
        mtime=_fake_mtime(5),
        type=MemberType.LINK,
        link_target="subdir",
        link_target_type=MemberType.DIR,
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


SAMPLE_ARCHIVES = [
    ArchiveInfo(
        filename="basic_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=CompressionFormat.ZIP,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="comment_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=CompressionFormat.ZIP,
        files=COMMENT_FILES,
        archive_comment="This is a\nmulti-line comment",
    ),
    ArchiveInfo(
        filename="comment_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=COMMENT_FILES,
        archive_comment="This is a\nmulti-line comment",
    ),
    # zipfile does not support writing encrypted files
    ArchiveInfo(
        filename="encryption.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=ENCRYPTION_FILES,
    ),
    # zipfile does not support symlinks
    ArchiveInfo(
        filename="symlinks.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=SYMLINK_FILES,
    ),
    ArchiveInfo(
        filename="encoding_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=CompressionFormat.ZIP,
        files=ENCODING_FILES,
        archive_comment="Comentário em português 😀",
    ),
    ArchiveInfo(
        filename="encoding_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=ENCODING_FILES,
        archive_comment="Comentário em português 😀",
    ),
    # info-zip does not support LZMA
    ArchiveInfo(
        filename="compression_methods_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=CompressionFormat.ZIP,
        files=COMPRESSION_METHOD_FILES_LZMA,
    ),
    ArchiveInfo(
        filename="compression_methods_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=CompressionFormat.ZIP,
        files=COMPRESSION_METHODS_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="symlinks.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR,
        files=SYMLINK_FILES,
    ),
    ArchiveInfo(
        filename="encoding.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR,
        files=ENCODING_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.gz",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR_GZ,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.bz2",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR_BZ2,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.xz",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=CompressionFormat.TAR_XZ,
        files=BASIC_FILES,
    ),
]
