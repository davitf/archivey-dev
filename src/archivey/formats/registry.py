from dataclasses import dataclass, field
from typing import Any, BinaryIO, Callable, List, Optional, Tuple

from archivey.exceptions import ArchiveError
from archivey.types import ArchiveFormat


@dataclass(frozen=True)
class Format:
    """Represents a format that can be handled by archivey."""

    format: ArchiveFormat
    extensions: List[str] = field(default_factory=list)
    magic: List[Tuple[bytes, int]] = field(default_factory=list)
    open: Optional[Callable[[str | BinaryIO], BinaryIO]] = None
    exception_translator: Optional[Callable[[Exception], Optional[ArchiveError]]] = None
    detector: Optional[Callable[[BinaryIO], bool]] = None


class FormatRegistry:
    """A registry for all supported formats."""

    _instance = None
    _formats: List[Format] = []

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def register(self, format: Format) -> None:
        """Register a new format."""
        self._formats.append(format)

    def get_all(self) -> List[Format]:
        """Return all registered formats."""
        return self._formats

    def by_extension(self, extension: str) -> Optional[Format]:
        """Find a format by its extension."""
        for format in self._formats:
            if extension in format.extensions:
                return format
        return None


# Global instance of the registry
registry = FormatRegistry()
