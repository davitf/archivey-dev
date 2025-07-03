from archivey.api.archive_reader import ArchiveReader
from archivey.api.config import (
    ArchiveyConfig,
    default_config,
    get_default_config,
    set_default_config,
)
from archivey.api.core import open_archive, open_compressed_stream
from archivey.api.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError,
)
from archivey.api.filters import (
    create_filter,
    data_filter,
    fully_trusted,
    tar_filter,
)
from archivey.api.types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    CreateSystem,
    MemberType,
)

__all__ = [
    # Core
    "open_archive",
    "open_compressed_stream",
    "ArchiveReader",
    "ArchiveInfo",
    "ArchiveMember",
    # Enums
    "ArchiveFormat",
    "MemberType",
    "CreateSystem",
    # Config
    "ArchiveyConfig",
    "default_config",
    "get_default_config",
    "set_default_config",
    # Exceptions
    "ArchiveError",
    "ArchiveFormatError",
    "ArchiveCorruptedError",
    "ArchiveEncryptedError",
    "ArchiveEOFError",
    "ArchiveMemberNotFoundError",
    "ArchiveNotSupportedError",
    # Filters
    "create_filter",
    "data_filter",
    "tar_filter",
    "fully_trusted",
]

__version__ = "0.1.0"
