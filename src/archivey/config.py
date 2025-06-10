from __future__ import annotations

import contextvars
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum


class OverwriteMode(Enum):
    OVERWRITE = 1
    SKIP = 2
    ERROR = 3



@dataclass
class ArchiveyConfig:
    """Configuration for :func:`archivey.open_archive`."""

    use_libarchive: bool = False
    use_rar_stream: bool = False
    use_single_file_stored_metadata: bool = False
    use_rapidgzip: bool = False
    use_indexed_bzip2: bool = False
    use_python_xz: bool = False
    use_zstandard: bool = False

    check_tar_integrity: bool = True
    overwrite_mode: OverwriteMode = OverwriteMode.ERROR



_default_config_var: contextvars.ContextVar[ArchiveyConfig] = contextvars.ContextVar(
    "archivey_default_config", default=ArchiveyConfig()
)


def get_default_config() -> ArchiveyConfig:
    """Return the current default configuration."""
    return _default_config_var.get()


def set_default_config(config: ArchiveyConfig) -> None:
    """Set the default configuration for :func:`open_archive`."""
    _default_config_var.set(config)


@contextmanager
def default_config(config: ArchiveyConfig):
    """Temporarily use ``config`` as the default configuration."""
    token = _default_config_var.set(config)
    try:
        yield
    finally:
        _default_config_var.reset(token)
