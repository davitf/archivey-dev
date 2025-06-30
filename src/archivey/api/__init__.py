
"""Public API for the Archivey library."""

from .core import open_archive, open_compressed_stream
from .config import ArchiveyConfig, default_config, get_default_config

__all__ = [
    "ArchiveyConfig",
    "get_default_config",
    "default_config",
    "open_archive",
    "open_compressed_stream",
]
