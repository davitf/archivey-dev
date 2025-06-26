"""Public API for streamadapt."""
from .core import (
    ensure_stream,
    ensure_binaryio,
    ensure_textio,
    ensure_bufferedio,
    ensure_buffered_textio,
    ensure_seekable,
    slice_stream,
    detect_stream_mode,
)

__all__ = [
    "ensure_stream",
    "ensure_binaryio",
    "ensure_textio",
    "ensure_bufferedio",
    "ensure_buffered_textio",
    "ensure_seekable",
    "slice_stream",
    "detect_stream_mode",
]
