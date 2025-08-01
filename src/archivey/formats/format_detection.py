import logging
import os
import tarfile
import zipfile
from typing import IO, TYPE_CHECKING, Callable, Tuple, Union

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
else:
    try:
        import rarfile
    except ImportError:
        rarfile = None  # type: ignore[assignment]

    try:
        import brotli
    except ImportError:
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


FormatEnum = Union[ArchiveFormat, StreamCompressionFormat]


# [signature, ...], offset, format
SIGNATURES: list[tuple[list[bytes], int, FormatEnum]] = [
    ([b"\x50\x4b\x03\x04"], 0, ArchiveFormat.ZIP),
    (
        [
            b"\x52\x61\x72\x21\x1a\x07\x00",  # RAR4
            b"\x52\x61\x72\x21\x1a\x07\x01\x00",  # RAR5
        ],
        0,
        ArchiveFormat.RAR,
    ),
    ([b"\x37\x7a\xbc\xaf\x27\x1c"], 0, ArchiveFormat.SEVENZIP),
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
    ([b"ustar"], 257, ArchiveFormat.TAR),  # TAR "ustar" magic
    (_ISO_MAGIC_BYTES, 0x8001, ArchiveFormat.ISO),  # ISO9660
]


def _is_brotli_stream(stream: IO[bytes]) -> bool:
    """Attempt to decompress a small chunk to see if it is Brotli."""
    if brotli is None:
        return False
    try:
        sample = stream.read(256)
        decompressor = brotli.Decompressor()
        decompressor.process(sample)
        return True
    except brotli.error:
        return False


_EXTRA_DETECTORS: list[tuple[Callable[[IO[bytes]], bool], FormatEnum]] = [
    # There may be other tar variants supported by tarfile.
    # TODO: this tries decompressing with the builtin formats. Avoid it.
    (tarfile.is_tarfile, ArchiveFormat.TAR),
    # zipfiles can have something prepended; is_zipfile checks the end of the file.
    # TODO: is this reading the whole stream for non-seekable streams?
    (zipfile.is_zipfile, ArchiveFormat.ZIP),
    (_is_brotli_stream, StreamCompressionFormat.BROTLI),
]

_SFX_DETECTORS: list[tuple[Callable[[IO[bytes]], bool], ArchiveFormat]] = []
if rarfile is not None:
    _SFX_DETECTORS.append((rarfile.is_rarfile_sfx, ArchiveFormat.RAR))


def detect_archive_format_by_signature(
    path_or_file: str | bytes | ReadableStreamLikeOrSimilar,
    detect_archive_inside_stream: bool = True,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    if isinstance(path_or_file, (str, bytes, os.PathLike)) and os.path.isdir(
        path_or_file
    ):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE

    with open_if_file(path_or_file) as f:
        detected_format: FormatEnum | None = None
        for magics, offset, fmt in SIGNATURES:
            bytes_to_read = max(len(magic) for magic in magics)
            f.seek(offset)
            data = read_exact(f, bytes_to_read)
            if any(data.startswith(magic) for magic in magics):
                detected_format = fmt
                break

        f.seek(0)

        if detected_format is None:
            for detector, fmt in _EXTRA_DETECTORS:
                if detector(f):
                    detected_format = fmt
                    break
                f.seek(0)

        # Handle compressed streams
        if isinstance(detected_format, StreamCompressionFormat):
            stream_format = detected_format
            if not detect_archive_inside_stream:
                return ArchiveFormat.RAW_STREAM, stream_format

            # It's a compressed stream, now check what's inside
            with open_stream(
                stream_format, f, get_archivey_config()
            ) as decompressed_stream:
                # For now, we only check for TAR inside compressed streams
                if is_uncompressed_tarfile(decompressed_stream):
                    return ArchiveFormat.TAR, stream_format
                else:
                    # It's some other compressed file
                    return ArchiveFormat.RAW_STREAM, stream_format

        elif isinstance(detected_format, ArchiveFormat):
            # It's an uncompressed archive format

            # Check for SFX files if it's an executable
            if detected_format == ArchiveFormat.UNKNOWN and _is_executable(f):
                for detector, fmt in _SFX_DETECTORS:
                    if detector(f):
                        detected_format = fmt
                        break
                    f.seek(0)

            if isinstance(detected_format, ArchiveFormat):
                return detected_format, StreamCompressionFormat.NONE

        # Fallback for SFX
        if _is_executable(f):
            for detector, fmt in _SFX_DETECTORS:
                if detector(f):
                    return fmt, StreamCompressionFormat.NONE
                f.seek(0)

        return ArchiveFormat.UNKNOWN, StreamCompressionFormat.NONE


EXTENSION_TO_FORMAT: dict[str, tuple[ArchiveFormat, StreamCompressionFormat]] = {
    ".tar": (ArchiveFormat.TAR, StreamCompressionFormat.NONE),
    ".zip": (ArchiveFormat.ZIP, StreamCompressionFormat.NONE),
    ".rar": (ArchiveFormat.RAR, StreamCompressionFormat.NONE),
    ".7z": (ArchiveFormat.SEVENZIP, StreamCompressionFormat.NONE),
    ".iso": (ArchiveFormat.ISO, StreamCompressionFormat.NONE),
    ".tar.gz": (ArchiveFormat.TAR, StreamCompressionFormat.GZIP),
    ".tgz": (ArchiveFormat.TAR, StreamCompressionFormat.GZIP),
    ".tar.bz2": (ArchiveFormat.TAR, StreamCompressionFormat.BZIP2),
    ".tbz2": (ArchiveFormat.TAR, StreamCompressionFormat.BZIP2),
    ".tar.xz": (ArchiveFormat.TAR, StreamCompressionFormat.XZ),
    ".txz": (ArchiveFormat.TAR, StreamCompressionFormat.XZ),
    ".tar.zst": (ArchiveFormat.TAR, StreamCompressionFormat.ZSTD),
    ".tzst": (ArchiveFormat.TAR, StreamCompressionFormat.ZSTD),
    ".tar.lz4": (ArchiveFormat.TAR, StreamCompressionFormat.LZ4),
    ".tlz4": (ArchiveFormat.TAR, StreamCompressionFormat.LZ4),
    ".tar.Z": (ArchiveFormat.TAR, StreamCompressionFormat.UNIX_COMPRESS),
    ".gz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.GZIP),
    ".bz2": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.BZIP2),
    ".xz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.XZ),
    ".zst": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.ZSTD),
    ".lz4": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.LZ4),
    ".zz": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.ZLIB),
    ".br": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.BROTLI),
    ".Z": (ArchiveFormat.RAW_STREAM, StreamCompressionFormat.UNIX_COMPRESS),
}


def detect_archive_format_by_filename(
    filename: str,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    """Detect the compression format of an archive based on its filename."""
    if os.path.isdir(filename):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE
    filename_lower = filename.lower()

    sorted_exts = sorted(EXTENSION_TO_FORMAT.keys(), key=len, reverse=True)

    for ext in sorted_exts:
        if filename_lower.endswith(ext):
            return EXTENSION_TO_FORMAT[ext]

    return ArchiveFormat.UNKNOWN, StreamCompressionFormat.NONE


logger = logging.getLogger(__name__)


def detect_archive_format(
    filename: str | os.PathLike | ReadableStreamLikeOrSimilar,
    detect_compressed_tar: bool = True,
) -> tuple[ArchiveFormat, StreamCompressionFormat]:
    # Check if it's a directory first
    if isinstance(filename, os.PathLike):
        filename = str(filename)

    if isinstance(filename, str) and os.path.isdir(filename):
        return ArchiveFormat.FOLDER, StreamCompressionFormat.NONE

    format_by_signature, stream_format_by_signature = detect_archive_format_by_signature(
        filename, detect_archive_inside_stream=detect_compressed_tar
    )

    if isinstance(filename, str):
        (
            format_by_filename,
            stream_format_by_filename,
        ) = detect_archive_format_by_filename(filename)
    else:
        format_by_filename, stream_format_by_filename = (
            ArchiveFormat.UNKNOWN,
            StreamCompressionFormat.NONE,
        )

    # If signature detection is unknown, trust filename
    if format_by_signature == ArchiveFormat.UNKNOWN:
        if format_by_filename != ArchiveFormat.UNKNOWN:
            logger.warning(
                "%s: Couldn't detect format by signature. Assuming %s/%s from filename",
                filename,
                format_by_filename,
                stream_format_by_filename,
            )
        return format_by_filename, stream_format_by_filename

    # If signature found a compressed stream, but filename indicates it's a tar
    # This avoids corrupted tar archives being misread as valid single-file
    if (
        stream_format_by_signature != StreamCompressionFormat.NONE
        and format_by_signature == ArchiveFormat.RAW_STREAM
        and format_by_filename == ArchiveFormat.TAR
    ):
        logger.warning(
            f"{filename}: Extension indicates TAR, but signature only found compressed stream. Assuming TAR."
        )
        return ArchiveFormat.TAR, stream_format_by_signature

    if (format_by_signature, stream_format_by_signature) != (
        format_by_filename,
        stream_format_by_filename,
    ):
        logger.warning(
            f"{filename}: Extension indicates {format_by_filename}/{stream_format_by_filename}, but detected {format_by_signature}/{stream_format_by_signature}"
        )

    return format_by_signature, stream_format_by_signature
