"""Registry for mapping archive formats to reader factories."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Dict, Optional

from .types import ArchiveFormat


ReaderFactory = Callable[[str, ArchiveFormat, bool], Any]


_registry: Dict[ArchiveFormat, ReaderFactory] = {}


def register_reader(format: ArchiveFormat, factory: ReaderFactory) -> None:
    """Register a factory for a specific :class:`ArchiveFormat`."""
    _registry[format] = factory


def unregister_reader(format: ArchiveFormat) -> None:
    """Remove the factory for ``format`` if present."""
    _registry.pop(format, None)


def get_reader_factory(format: ArchiveFormat) -> Optional[ReaderFactory]:
    """Return the factory registered for ``format`` or ``None``."""
    return _registry.get(format)


def clear_registry() -> None:
    """Remove all registered factories (mainly for testing)."""
    _registry.clear()
