"""Registry for stream handlers and detection helpers."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import IO, TYPE_CHECKING, BinaryIO, Callable, Dict, List, Optional, Tuple

from archivey.internal.io_helpers import ExceptionTranslatorFn

if TYPE_CHECKING:
    from archivey.config import ArchiveyConfig
    from archivey.types import StreamFormat

DetectorFn = Callable[[IO[bytes]], bool]
OpenFn = Callable[[str | BinaryIO], BinaryIO]
HandlerFactory = Callable[["ArchiveyConfig"], Tuple[OpenFn, ExceptionTranslatorFn]]


@dataclass
class StreamHandler:
    handler_factory: HandlerFactory
    magic_bytes: List[bytes]
    extra_detector: Optional[DetectorFn] = None


_stream_handlers: Dict["StreamFormat", StreamHandler] = {}


def register_stream_handler(format: "StreamFormat", handler: StreamHandler) -> None:
    _stream_handlers[format] = handler


def get_stream_handler(format: "StreamFormat") -> StreamHandler | None:
    return _stream_handlers.get(format)


# Import built-in handlers to populate the registry
for _mod in (
    "archivey.formats.stream_handlers.gzip",
    "archivey.formats.stream_handlers.bzip2",
    "archivey.formats.stream_handlers.xz",
    "archivey.formats.stream_handlers.zstd",
    "archivey.formats.stream_handlers.lz4",
    "archivey.formats.stream_handlers.zlib",
    "archivey.formats.stream_handlers.brotli",
    "archivey.formats.stream_handlers.unix_compress",
):
    try:
        import_module(_mod)
    except Exception:  # noqa: BLE001  # pragma: no cover - optional deps may be missing
        pass

stream_handlers = _stream_handlers
