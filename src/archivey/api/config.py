from __future__ import annotations

import contextvars
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from .types import ExtractionFilter, FilterFunc

if TYPE_CHECKING:
    from enum import StrEnum
elif sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class OverwriteMode(StrEnum):
    OVERWRITE = "overwrite"
    SKIP = "skip"
    ERROR = "error"


@dataclass
class ArchiveyConfig:
    """Configuration for :func:`archivey.open_archive`.

    See the developer guide for a description of each option and when they are
    used.
    """

    use_rapidgzip: bool = False
    "Alternative library that can be used instead of the builtin modules to read gzip stream formats. Provides multithreaded decompression and improved random access support (i.e. supports jumping to arbitrary positions in the stream without (re-)decompressing the entire stream, which is particularly useful for accessing random members in compressed tar files)."
    
    use_indexed_bzip2: bool = False
    "Alternative library that can be used instead of the builtin modules to read bzip2 stream formats. Provides multithreaded decompression and improved random access support."
    
    use_python_xz: bool = False
    "Alternative library that can be used instead of the builtin modules to read xz stream formats. Provides multithreaded decompression and improved random access support."

    use_zstandard: bool = False
    "An alternative to pyzstd. Not as good at error reporting."

    use_rar_stream: bool = False
    "If set, use an alternative approach instead of calling rarfile when iterating over RAR archive members. This supports decompressing multiple members in a solid archive by going through the archive only once, instead of once per member."

    use_single_file_stored_metadata: bool = False
    "If set, data stored in compressed stream headers is set in the ArchiveMember object for single-file compressed archives, instead of basing it only on the file itself. (filename and modification time for gzip archives only)"

    tar_check_integrity: bool = True
    "If a tar archive is corrupted in a metadata section, tarfile simply stops reading further and acts as if the file has ended. If set, we perform a check that the tar archive has actually been read fully, and raise an error if it's actually corrupted."

    overwrite_mode: OverwriteMode = OverwriteMode.ERROR
    "What to do with existing files when extracting. OVERWRITE: overwrite existing files. SKIP: skip existing files. ERROR: raise an error if a file already exists, and stop extracting."

    extraction_filter: ExtractionFilter | FilterFunc = ExtractionFilter.DATA
    "A filter function that can be used to filter members when iterating over an archive. It can be a function that takes an ArchiveMember and returns a possibly-modified ArchiveMember object, or None to skip the member."


_default_config_var: contextvars.ContextVar[ArchiveyConfig] = contextvars.ContextVar(
    "archivey_default_config", default=ArchiveyConfig()
)


def get_default_config() -> ArchiveyConfig:
    """Return the current default configuration."""
    return _default_config_var.get()


def set_default_config(config: ArchiveyConfig) -> None:
    """Set the default configuration for :func:`open_archive`."""
    _default_config_var.set(config)


def set_default_config_fields(**kwargs: dict[str, Any]) -> None:
    """Set the default configuration for :func:`open_archive`."""
    config = get_default_config()
    config = replace(config, **kwargs)
    set_default_config(config)


@contextmanager
def default_config(config: ArchiveyConfig | None = None, **kwargs: dict[str, Any]):
    """Temporarily use ``config`` as the default configuration."""
    if config is None:
        config = get_default_config()

    if kwargs:
        config = replace(config, **kwargs)

    token = _default_config_var.set(config)
    try:
        yield
    finally:
        _default_config_var.reset(token)
