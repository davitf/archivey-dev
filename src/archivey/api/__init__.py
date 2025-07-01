
"""Public API for the Archivey library.

This package exposes the stable entry points and type definitions that
applications are expected to import.  Anything listed in ``__all__`` is
considered part of the public API and will follow semantic versioning
rules.
"""

from .core import open_archive, open_compressed_stream
from .config import (
    ArchiveyConfig,
    OverwriteMode,
    default_config,
    get_default_config,
)
from .types import (
    ArchiveFormat,
    ArchiveInfo,
    ArchiveMember,
    ArchiveReader,
    CreateSystem,
    ExtractionFilter,
    FilterFunc,
    MemberType,
)

__all__ = [
    "ArchiveyConfig",
    "OverwriteMode",
    "ArchiveFormat",
    "ArchiveInfo",
    "ArchiveMember",
    "ArchiveReader",
    "MemberType",
    "CreateSystem",
    "ExtractionFilter",
    "FilterFunc",
    "get_default_config",
    "default_config",
    "open_archive",
    "open_compressed_stream",
]
