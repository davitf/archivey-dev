"""Utilities for detecting archive and stream compression formats."""

import logging
import os
import tarfile
import zipfile
from typing import IO, TYPE_CHECKING

from archivey.config import get_archivey_config
from archivey.formats.compressed_streams import open_stream
from archivey.internal.io_helpers import (
    ReadableStreamLikeOrSimilar,
    is_seekable,
    open_if_file,
    read_exact,
)
from archivey.types import ArchiveFormat, StreamCompressionFormat

if TYPE_CHECKING:
    import brotli
    import rarfile
else:  # pragma: no cover - optional dependencies
    try:  # pragma: no cover - optional dependency
        import rarfile
    except ImportError:  # pragma: no cover
        rarfile = None  # type: ignore[assignment]

    try:  # pragma: no cover - optional dependency
        import brotli
    except ImportError:  # pragma: no cover
        brotli = None  # type: ignore[assignment]

# Taken from the pycdlib code
_ISO_MAGIC_BYTES = (
    b"CD001",
    b"CDW02",
    b"BEA01",
    b"NSR02",
    b"NSR03",
    b"TEA01",
    b"BOOT2",
)


def _is_executable(stream: IO[bytes]) -> bool:
    EXECUTABLE_MAGICS = {
        "pe": b"MZ",
        "elf": b"\x7fELF",
        "macho": b"\xcf\xfa\xed\xfe",
        "macho-fat": b"\xca\xfe\xba\xbe",
        "script": b"#!",
    }

    stream.seek(0)
    header = read_exact(stream, 16)
    return any(header.startswith(magic) for magic in EXECUTABLE_MAGICS.values())


def is_uncompressed_tarfile(stream: IO[bytes]) -> bool:
    if is_seekable(stream):
        stream.seek(257)
    else:
        read_exact(stream, 257)

    data = read_exact(stream, 5)
    return data == b"ustar"


# (signatures, offset, archive format)
ARCHIVE_SIGNATURES = [
    ([b"\x50\x4b\x03\x04"], 0, ArchiveFormat.ZIP),
    (
        [
            b"\x52\x61\x72\x21\x1a\x07\x00",
            b"\x52\x61\x72\x21\x1a\x07\x01\x00",
        ],
        0,
        ArchiveFormat.RAR,
    ),
    ([b"\x37\x7a\xbc\xaf\x27\x1c"], 0, ArchiveFormat.SEVENZIP),
    ([b"ustar"], 257, ArchiveFormat.TAR),
    (_ISO_MAGIC_BYTES, 0x8001, ArchiveFormat.ISO),
]


# (signatures, offset, stream format)
STREAM_SIGNATURES = [
    ([b"\x1f\x8b"], 0, StreamCompressionFormat.GZIP),
    ([b"\x42\x5a\x68"], 0, StreamCompressionFormat.BZIP2),
    ([b"\xfd\x37\x7a\x58\x5a\x00"], 0, StreamCompressionFormat.XZ),
    ([b"\x28\xb5\x2f\xfd"], 0, StreamCompressionFormat.ZSTD),
    ([b"\x04\x22\x4d\x18"], 0, StreamCompressionFormat.LZ4),
    (
        [b"\x78\x01", b"\x78\x5e", b"\x78\x9c", b"\x78\xda"],
        0,
        StreamCompressionFormat.ZLIB,
    ),
    ([b"\x1f\x9d"], 0, StreamCompressionFormat.UNIX_COMPRESS),
]


def _is_brotli_stream(stream: IO[bytes]) -> bool:
    if brotli is None:
        return False
    try:
        sample = stream.read(256)
        decompressor = brotli.Decompressor()
        decompressor.process(sample)
        return True
    except brotli.error:
        return False


_EXTRA_DETECTORS = [
    (tarfile.is_tarfile, (ArchiveFormat.TAR, StreamCompressionFormat.NONE)),
    (zipfile.is_zipfile, (ArchiveFormat.ZIP, StreamCompressionFormat.NONE)),
    (_is_brotli_stream, (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.BROTLI)),
]

_SFX_DETECTORS = []
if rarfile is not None:  # pragma: no cover - optional dependency
    _SFX_DETECTORS.append((rarfile.is_rarfile_sfx, ArchiveFormat.RAR))


def detect_format_by_signature(
    path_or_file: str | bytes | ReadableStreamLikeOrSimilar,
    detect_compressed_tar: bool = True,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    if isinstance(path_or_file, (str, bytes, os.PathLike)) and os.path.isdir(
        path_or_file
    ):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE

    with open_if_file(path_or_file) as f:
        for magics, offset, fmt in ARCHIVE_SIGNATURES:
            bytes_to_read = max(len(m) for m in magics)
            f.seek(offset)
            data = read_exact(f, bytes_to_read)
            if any(data.startswith(m) for m in magics):
                return fmt, StreamCompressionFormat.NONE

        stream_fmt: StreamCompressionFormat | None = None
        for magics, offset, s_fmt in STREAM_SIGNATURES:
            bytes_to_read = max(len(m) for m in magics)
            f.seek(offset)
            data = read_exact(f, bytes_to_read)
            if any(data.startswith(m) for m in magics):
                stream_fmt = s_fmt
                break

        f.seek(0)

        if stream_fmt is not None:
            if detect_compressed_tar:
                with open_stream(stream_fmt, f, get_archivey_config()) as ds:
                    if is_uncompressed_tarfile(ds):
                        return ArchiveFormat.TAR, stream_fmt
            return ArchiveFormat.RAW_STREAM, stream_fmt

        for detector, (a_fmt, s_fmt) in _EXTRA_DETECTORS:
            if detector(f):
                return a_fmt, s_fmt
            f.seek(0)

        if _is_executable(f):
            for detector, a_fmt in _SFX_DETECTORS:
                if detector(f):
                    return a_fmt, StreamCompressionFormat.NONE
                f.seek(0)

        return ArchiveFormat.UNKNOWN, StreamCompressionFormat.NONE


EXTENSION_TO_FORMAT: dict[str, tuple[ArchiveFormat, StreamCompressionFormat]] = {
    ".tar": (ArchiveFormat.TAR, StreamCompressionFormat.NONE),
    ".tar.gz": (ArchiveFormat.TAR, StreamCompressionFormat.GZIP),
    ".tar.bz2": (ArchiveFormat.TAR, StreamCompressionFormat.BZIP2),
    ".tar.xz": (ArchiveFormat.TAR, StreamCompressionFormat.XZ),
    ".tar.zst": (ArchiveFormat.TAR, StreamCompressionFormat.ZSTD),
    ".tar.lz4": (ArchiveFormat.TAR, StreamCompressionFormat.LZ4),
    ".tar.Z": (ArchiveFormat.TAR, StreamCompressionFormat.UNIX_COMPRESS),
    ".tgz": (ArchiveFormat.TAR, StreamCompressionFormat.GZIP),
    ".tbz2": (ArchiveFormat.TAR, StreamCompressionFormat.BZIP2),
    ".txz": (ArchiveFormat.TAR, StreamCompressionFormat.XZ),
    ".tzst": (ArchiveFormat.TAR, StreamCompressionFormat.ZSTD),
    ".tlz4": (ArchiveFormat.TAR, StreamCompressionFormat.LZ4),
    ".gz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.GZIP),
    ".bz2": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.BZIP2),
    ".xz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.XZ),
    ".zst": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.ZSTD),
    ".lz4": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.LZ4),
    ".zz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.ZLIB),
    ".br": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.BROTLI),
    ".z": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.UNIX_COMPRESS),
    ".zip": (ArchiveFormat.ZIP, StreamCompressionFormat.NONE),
    ".rar": (ArchiveFormat.RAR, StreamCompressionFormat.NONE),
    ".7z": (ArchiveFormat.SEVENZIP, StreamCompressionFormat.NONE),
    ".iso": (ArchiveFormat.ISO, StreamCompressionFormat.NONE),
}


def has_tar_extension(filename: str) -> bool:
    base_filename, ext = os.path.splitext(filename.lower())
    info = EXTENSION_TO_FORMAT.get(ext)
    return (info and info[0] == ArchiveFormat.TAR) or base_filename.endswith(".tar")


def detect_format_by_filename(
    filename: str,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    if os.path.isdir(filename):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE
    filename_lower = filename.lower()
    for ext, formats in EXTENSION_TO_FORMAT.items():
        if filename_lower.endswith(ext):
            return formats
    return ArchiveFormat.UNKNOWN, StreamCompressionFormat.NONE


logger = logging.getLogger(__name__)


def detect_format(
    filename: str | os.PathLike | ReadableStreamLikeOrSimilar,
    detect_compressed_tar: bool = True,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    if isinstance(filename, os.PathLike):
        filename = str(filename)

    if isinstance(filename, str) and os.path.isdir(filename):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE

    format_by_signature = detect_format_by_signature(filename, detect_compressed_tar)

    if isinstance(filename, str):
        format_by_filename = detect_format_by_filename(filename)
    else:
        format_by_filename = (ArchiveFormat.UNKNOWN, StreamCompressionFormat.NONE)

    if format_by_signature == (
        ArchiveFormat.UNKNOWN,
        StreamCompressionFormat.NONE,
    ) and format_by_filename == (
        ArchiveFormat.UNKNOWN,
        StreamCompressionFormat.NONE,
    ):
        logger.warning("%s: Can't detect format by signature or filename", filename)
        return format_by_signature

    if format_by_signature[0] == ArchiveFormat.UNKNOWN:
        logger.warning(
            "%s: Couldn't detect format by signature. Assuming %s",
            filename,
            format_by_filename[0],
        )
        return format_by_filename

    if format_by_filename[0] == ArchiveFormat.UNKNOWN:
        logger.warning(
            "%s: Unknown extension. Detected %s",
            filename,
            format_by_signature[0],
        )
    elif format_by_signature != format_by_filename:
        logger.warning(
            f"{filename}: Extension indicates {format_by_filename[0]}, but detected ({format_by_signature[0]})"
        )

    return format_by_signature
