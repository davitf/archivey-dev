from __future__ import annotations

import contextvars
import sys
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from archivey.types import FilterFunc

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


class ExtractionFilter(StrEnum):
    FULLY_TRUSTED = "fully_trusted"
    TAR = "tar"
    DATA = "data"


@dataclass
class ArchiveyConfig:
    """Configuration for :func:`archivey.open_archive`."""

    use_rar_stream: bool = False
    use_libarchive: bool = False
    use_single_file_stored_metadata: bool = False
    use_rapidgzip: bool = False
    use_indexed_bzip2: bool = False
    use_python_xz: bool = False
    use_zstandard: bool = False

    tar_check_integrity: bool = True

    sevenzip_read_link_targets_eagerly: bool = False

    overwrite_mode: OverwriteMode = OverwriteMode.ERROR

    extraction_filter: ExtractionFilter | FilterFunc = ExtractionFilter.DATA


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
