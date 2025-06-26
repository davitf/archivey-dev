"""Protocol definitions for streamadapt."""
from typing import Protocol, runtime_checkable, Union


@runtime_checkable
class ReadableBinaryStream(Protocol):
    """Any object supporting ``read()`` that returns bytes."""

    def read(self, size: int = -1) -> bytes: ...


@runtime_checkable
class WritableBinaryStream(Protocol):
    """Any object supporting ``write()`` of bytes."""

    def write(self, data: bytes) -> int: ...


BinaryStreamLike = Union[ReadableBinaryStream, WritableBinaryStream]


@runtime_checkable
class ReadableTextStream(Protocol):
    """Any object supporting ``read()`` that returns ``str``."""

    def read(self, size: int = -1) -> str: ...


@runtime_checkable
class WritableTextStream(Protocol):
    """Any object supporting ``write()`` of ``str``."""

    def write(self, data: str) -> int: ...


TextStreamLike = Union[ReadableTextStream, WritableTextStream]
