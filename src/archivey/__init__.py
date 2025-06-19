from archivey.config import (
    ArchiveyConfig,
    default_config,
    get_default_config,
    set_default_config,
)
from archivey.core import open_archive
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError,
)
from archivey.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    CreateSystem,
    MemberType,
)

__all__ = [
    "open_archive",
    "ArchiveError",
    "ArchiveFormatError",
    "ArchiveCorruptedError",
    "ArchiveEncryptedError",
    "ArchiveEOFError",
    "ArchiveMemberNotFoundError",
    "ArchiveNotSupportedError",
    "ArchiveMember",
    "ArchiveInfo",
    "ArchiveFormat",
    "MemberType",
    "CreateSystem",
    "ArchiveyConfig",
    "default_config",
    "get_default_config",
    "set_default_config",
]

__version__ = "0.1.0"

# Import builtin readers so they register themselves on package import
from . import (
    folder_reader,
    single_file_reader,
    sevenzip_reader,
    tar_reader,
    rar_reader,
    zip_reader,
)
