from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import os
from archivey.types import ArchiveFormat, MemberType


class GenerationMethod(Enum):
    ZIPFILE = "zipfile"
    INFOZIP = "zip_command_line"
    EXTERNAL = "external"
    TAR_COMMAND_LINE = "tar_command_line"
    PY7ZR = "py7zr"
    SEVENZIP_COMMAND_LINE = "7z_command_line"
    RAR_COMMAND_LINE = "rar_command_line"
    COMMAND_LINE = "command_line"


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


TEST_ARCHIVES_DIR = "test_archives"
TEST_ARCHIVES_EXTERNAL_DIR = "test_archives_external"


@dataclass
class ArchiveInfo:
    filename: str
    generation_method: GenerationMethod
    format: ArchiveFormat
    files: list[FileInfo]
    archive_comment: str | None = None
    skip_test: bool = False
    solid: bool = False
    header_password: str | None = None

    def get_archive_path(self, base_dir: str) -> str:
        if self.generation_method == GenerationMethod.EXTERNAL:
            return os.path.join(base_dir, TEST_ARCHIVES_EXTERNAL_DIR, self.filename)
        else:
            return os.path.join(base_dir, TEST_ARCHIVES_DIR, self.filename)


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

# SOME_TEST_SOLID_FILES = [
#     FileInfo(
#         name="plain.txt",
#         mtime=_fake_mtime(1),
#         contents=b"This is plain",
#         password="password",
#     ),
#     FileInfo(
#         name="secret.txt",
#         mtime=_fake_mtime(1),
#         contents=b"This is secret",
#         # password="password",
#     ),
#     FileInfo(
#         name="also_secret.txt",
#         mtime=_fake_mtime(2),
#         contents=b"This is also secret",
#         # password="password",
#     ),
# ]


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


ZIP_ARCHIVES = [
    ArchiveInfo(
        filename="basic_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=ArchiveFormat.ZIP,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="comment_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=ArchiveFormat.ZIP,
        files=COMMENT_FILES,
        archive_comment="This is a\nmulti-line comment",
    ),
    ArchiveInfo(
        filename="comment_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=COMMENT_FILES,
        archive_comment="This is a\nmulti-line comment",
    ),
    # zipfile does not support writing encrypted files
    ArchiveInfo(
        filename="encryption.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=ENCRYPTION_SINGLE_PASSWORD_FILES,
    ),
    ArchiveInfo(
        filename="encryption_several_passwords.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=ENCRYPTION_SEVERAL_PASSWORDS_FILES,
    ),
    # zipfile does not support symlinks
    ArchiveInfo(
        filename="symlinks.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=SYMLINK_FILES,
    ),
    ArchiveInfo(
        filename="encoding_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=ArchiveFormat.ZIP,
        files=ENCODING_FILES,
        archive_comment="Comentário em português 😀",
    ),
    ArchiveInfo(
        filename="encoding_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=ENCODING_FILES,
        archive_comment="Comentário em português 😀",
    ),
    ArchiveInfo(
        filename="encoding_infozip_jules.zip",
        generation_method=GenerationMethod.EXTERNAL,
        format=ArchiveFormat.ZIP,
        files=ENCODING_FILES,
        archive_comment="Comentário em português 😀",
        skip_test=True,
    ),
    # info-zip does not support LZMA
    ArchiveInfo(
        filename="compression_methods_zipfile.zip",
        generation_method=GenerationMethod.ZIPFILE,
        format=ArchiveFormat.ZIP,
        files=COMPRESSION_METHOD_FILES_LZMA,
    ),
    ArchiveInfo(
        filename="compression_methods_infozip.zip",
        generation_method=GenerationMethod.INFOZIP,
        format=ArchiveFormat.ZIP,
        files=COMPRESSION_METHODS_FILES,
    ),
]

TAR_ARCHIVES = [
    ArchiveInfo(
        filename="basic.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="symlinks.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR,
        files=SYMLINK_FILES,
    ),
    ArchiveInfo(
        filename="encoding.tar",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR,
        files=ENCODING_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.gz",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR_GZ,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.bz2",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR_BZ2,
        files=BASIC_FILES,
    ),
    ArchiveInfo(
        filename="basic.tar.xz",
        generation_method=GenerationMethod.TAR_COMMAND_LINE,
        format=ArchiveFormat.TAR_XZ,
        files=BASIC_FILES,
    ),
]

# RAR Archives
RAR_ARCHIVES = [
    ArchiveInfo(
        filename="basic.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=BASIC_FILES,
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="basic_solid.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=BASIC_FILES,
        solid=True,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="comment.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=COMMENT_FILES,
        archive_comment="RAR archive comment",
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="comment_solid.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=COMMENT_FILES,
        archive_comment="Solid RAR archive comment",
        solid=True,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encryption.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=ENCRYPTION_SINGLE_PASSWORD_FILES,
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encryption_with_plain.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=ENCRYPTION_ENCRYPTED_AND_PLAIN_FILES,
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encryption_solid.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=ENCRYPTION_SINGLE_PASSWORD_FILES,
        solid=True,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encryption_several_passwords.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=ENCRYPTION_SEVERAL_PASSWORDS_FILES,
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encrypted_header.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=BASIC_FILES,
        solid=False,
        header_password="header_password",
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="encrypted_header_solid.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=BASIC_FILES,
        solid=True,
        header_password="header_password",
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="symlinks.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=SYMLINK_FILES,
        solid=False,
        skip_test=False,  # Changed from True
    ),
    ArchiveInfo(
        filename="symlinks_solid.rar",
        generation_method=GenerationMethod.RAR_COMMAND_LINE,
        format=ArchiveFormat.RAR,
        files=SYMLINK_FILES,
        solid=True,
        skip_test=False,  # Changed from True
    ),
]

# 7z Archives (py7zr)
SEVENZIP_PY7ZR_ARCHIVES = [
    ArchiveInfo(
        filename="basic_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=BASIC_FILES,
        solid=False,
    ),
    ArchiveInfo(
        filename="basic_solid_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=BASIC_FILES,
        solid=True,
    ),
    ArchiveInfo(
        filename="encryption_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=ENCRYPTION_SINGLE_PASSWORD_FILES,
        solid=False,
    ),
    ArchiveInfo(
        filename="encryption_with_plain_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=ENCRYPTION_ENCRYPTED_AND_PLAIN_FILES,
        solid=False,
    ),
    ArchiveInfo(
        filename="encryption_several_passwords_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=ENCRYPTION_SEVERAL_PASSWORDS_FILES,
        solid=False,
    ),
    ArchiveInfo(
        filename="encryption_solid_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=ENCRYPTION_SEVERAL_PASSWORDS_FILES,
        solid=True,
        skip_test=True,
    ),
    # ArchiveInfo(
    #     filename="some_test_solid_py7zr.7z",
    #     generation_method=GenerationMethod.PY7ZR,
    #     format=ArchiveFormat.SEVENZIP,
    #     files=SOME_TEST_SOLID_FILES,
    #     solid=True,
    # ),
    ArchiveInfo(
        filename="encrypted_header_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=BASIC_FILES,
        solid=False,
        header_password="header_password",
    ),
    ArchiveInfo(
        filename="encrypted_header_solid_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=BASIC_FILES,
        solid=True,
        header_password="header_password",
    ),
    ArchiveInfo(
        filename="symlinks_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=SYMLINK_FILES,
        solid=False,
        skip_test=True,  # py7zr may not support symlinks
    ),
    ArchiveInfo(
        filename="symlinks_solid_py7zr.7z",
        generation_method=GenerationMethod.PY7ZR,
        format=ArchiveFormat.SEVENZIP,
        files=SYMLINK_FILES,
        solid=True,
        skip_test=True,  # py7zr may not support symlinks
    ),
]

# 7z Archives (7z command line)
SEVENZIP_CMD_ARCHIVES = [
    ArchiveInfo(
        filename=archive_info.filename.replace("_py7zr.7z", "_7zcmd.7z"),
        generation_method=GenerationMethod.SEVENZIP_COMMAND_LINE,
        format=ArchiveFormat.SEVENZIP,
        files=archive_info.files,
        solid=archive_info.solid,
        header_password=archive_info.header_password,
        skip_test=archive_info.skip_test
        or archive_info.filename
        in (
            "symlinks_py7zr.7z",  # already skipped
            "symlinks_solid_py7zr.7z",  # already skipped
        ),  # Propagate skip_test for symlinks
    )
    for archive_info in SEVENZIP_PY7ZR_ARCHIVES
]

assert all(archive.filename.endswith("_7zcmd.7z") for archive in SEVENZIP_CMD_ARCHIVES)


# Single compressed files (e.g. .gz, .bz2, .xz)
SINGLE_FILE_TXT_CONTENT = b"This is a single test file for compression.\n"
SINGLE_FILE_INFO = FileInfo(
    name="single_file.txt",
    mtime=datetime(2023, 1, 1, 12, 0, 0),
    contents=SINGLE_FILE_TXT_CONTENT,
)
SINGLE_FILE_INFO_SPECIAL_MTIME = FileInfo(
    name="single_file.txt",
    mtime=datetime(1970, 1, 1, 0, 0, 0),
    contents=SINGLE_FILE_TXT_CONTENT,
)

SINGLE_FILE_COMPRESSED_ARCHIVES = [
    ArchiveInfo(
        filename="single_file.txt.gz",
        generation_method=GenerationMethod.COMMAND_LINE,
        format=ArchiveFormat.GZIP,
        files=[SINGLE_FILE_INFO_SPECIAL_MTIME],
        archive_comment=None,
    ),
    ArchiveInfo(
        filename="single_file.txt.bz2",
        generation_method=GenerationMethod.COMMAND_LINE,
        format=ArchiveFormat.BZIP2,
        files=[SINGLE_FILE_INFO_SPECIAL_MTIME],
        archive_comment=None,
    ),
    ArchiveInfo(
        filename="single_file.txt.xz",
        generation_method=GenerationMethod.COMMAND_LINE,
        format=ArchiveFormat.XZ,
        files=[SINGLE_FILE_INFO_SPECIAL_MTIME],
        archive_comment=None,
    ),
]

SAMPLE_ARCHIVES = (
    ZIP_ARCHIVES
    + TAR_ARCHIVES
    + RAR_ARCHIVES
    + SEVENZIP_PY7ZR_ARCHIVES
    + SEVENZIP_CMD_ARCHIVES
    + SINGLE_FILE_COMPRESSED_ARCHIVES
)
