from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO, Callable

from archivey.config import ArchiveyConfig, get_archivey_config
from archivey.formats.registry import get_stream_handler
from archivey.internal.archive_stream import ArchiveStream

if TYPE_CHECKING:
    from archivey.internal.io_helpers import ExceptionTranslatorFn
    from archivey.types import StreamFormat


def get_stream_open_fn(
    format: "StreamFormat", config: ArchiveyConfig | None = None
) -> tuple[Callable[[str | BinaryIO], BinaryIO], ExceptionTranslatorFn]:
    if config is None:
        config = get_archivey_config()
    handler = get_stream_handler(format)
    if handler is None:
        raise ValueError(f"Unsupported archive format: {format}")
    return handler.handler_factory(config)


def open_stream(
    format: "StreamFormat", path_or_stream: str | BinaryIO, config: ArchiveyConfig
) -> BinaryIO:
    open_fn, exception_translator = get_stream_open_fn(format, config)
    return ArchiveStream(
        open_fn=lambda: open_fn(path_or_stream),
        exception_translator=exception_translator,
        lazy=False,
        archive_path=path_or_stream if isinstance(path_or_stream, str) else None,
        member_name="<stream>",
        seekable=True,
    )
