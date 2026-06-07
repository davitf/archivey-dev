from archivey.archive_reader import ArchiveReader
from archivey.config import (
    ArchiveyConfig,
    archivey_config,
    get_archivey_config,
    set_archivey_config,
)
from archivey.core import open_archive, open_compressed_stream
from archivey.exceptions import ArchiveError
from archivey.types import (
    AccessCost,
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    CompressionMethod,
    ContainerFormat,
    ExtractionFilter,
    MemberListingCost,
    MemberType,
    StreamFormat,
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
    "AccessCost",
    "CompressionMethod",
    "ContainerFormat",
    "MemberListingCost",
    "StreamFormat",
    "MemberType",
    "ExtractionFilter",
    # Config
    "ArchiveyConfig",
    "archivey_config",
    "get_archivey_config",
    "set_archivey_config",
    # Exceptions
    "ArchiveError",
]

__version__ = "0.1.0a4"
