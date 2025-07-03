from typing import (
    TYPE_CHECKING,
    Callable,
    Protocol,
    overload,
)

if TYPE_CHECKING:
    from .data_classes import ArchiveMember


ExtractFilterFunc = Callable[["ArchiveMember", str], "ArchiveMember" | None]

IteratorFilterFunc = Callable[["ArchiveMember"], "ArchiveMember" | None]


# A type that must match both ExtractFilterFunc and IteratorFilterFunc
# The callable must be able to handle both one and two arguments
class FilterFunc(Protocol):
    @overload
    def __call__(self, member: "ArchiveMember") -> "ArchiveMember" | None: ...

    @overload
    def __call__(
        self, member: "ArchiveMember", dest_path: str
    ) -> "ArchiveMember" | None: ...

    def __call__(
        self, member: "ArchiveMember", dest_path: str | None = None
    ) -> "ArchiveMember" | None: ...
