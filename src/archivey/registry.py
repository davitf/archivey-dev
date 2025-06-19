from __future__ import annotations

"""Registry for ArchiveReader factories."""

from importlib import metadata
from typing import Callable, Dict, Optional

from .base_reader import ArchiveReader
from .types import ArchiveFormat

# Type alias for reader factory functions
ReaderFactory = Callable[..., ArchiveReader]

_reader_factories: Dict[ArchiveFormat, ReaderFactory] = {}


def register_reader(format: ArchiveFormat, factory: ReaderFactory) -> None:
    """Register a factory for the given archive ``format``."""
    _reader_factories[format] = factory


def get_reader_factory(format: ArchiveFormat) -> Optional[ReaderFactory]:
    """Return the registered factory for ``format`` or ``None``."""
    return _reader_factories.get(format)


def unregister_reader(format: ArchiveFormat) -> None:
    """Remove the factory associated with ``format`` if present."""
    _reader_factories.pop(format, None)


def _load_entry_points() -> None:
    """Load reader factories from ``archivey.readers`` entry points."""
    try:
        eps = metadata.entry_points(group="archivey.readers")
    except Exception:
        return
    for ep in eps:
        try:
            factory = ep.load()
        except Exception:
            continue
        if not callable(factory):
            continue
        try:
            fmt = ArchiveFormat(ep.name)
        except ValueError:
            # Unknown format name
            continue
        register_reader(fmt, factory)


_load_entry_points()
