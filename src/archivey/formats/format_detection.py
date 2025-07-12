import logging
import os
import tarfile
import zipfile
from typing import IO, TYPE_CHECKING

from archivey.config import get_archivey_config
from archivey.internal.io_helpers import (
    ReadableStreamLikeOrSimilar,
    is_seekable,
    open_if_file,
    read_exact,
)
from archivey.types import (
    COMPRESSION_FORMAT_TO_TAR_FORMAT,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)

from .compressed_streams import open_stream

if TYPE_CHECKING:
    import rarfile
else:
    try:
        import rarfile
    except ImportError:
        rarfile = None  # type: ignore[assignment]

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


# [signature, ...], offset, format
SIGNATURES = [
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
    ([b"\x1f\x8b"], 0, ArchiveFormat.GZIP),
    ([b"\x42\x5a\x68"], 0, ArchiveFormat.BZIP2),
    ([b"\xfd\x37\x7a\x58\x5a\x00"], 0, ArchiveFormat.XZ),
    ([b"\x28\xb5\x2f\xfd"], 0, ArchiveFormat.ZSTD),
    ([b"\x04\x22\x4d\x18"], 0, ArchiveFormat.LZ4),
    ([b"ustar"], 257, ArchiveFormat.TAR),  # TAR "ustar" magic
    (_ISO_MAGIC_BYTES, 0x8001, ArchiveFormat.ISO),  # ISO9660
]
_EXTRA_DETECTORS = [
    # There may be other tar variants supported by tarfile.
    (tarfile.is_tarfile, ArchiveFormat.TAR),
    # zipfiles can have something prepended; is_zipfile checks the end of the file
    (zipfile.is_zipfile, ArchiveFormat.ZIP),
]

_SFX_DETECTORS = []
if rarfile is not None:
    _SFX_DETECTORS.append((rarfile.is_rarfile_sfx, ArchiveFormat.RAR))


def detect_archive_format_by_signature(
    path_or_file: str | bytes | ReadableStreamLikeOrSimilar,
) -> ArchiveFormat:
    if isinstance(path_or_file, (str, bytes, os.PathLike)) and os.path.isdir(
        path_or_file
    ):
        return ArchiveFormat.FOLDER

    with open_if_file(path_or_file) as f:
        detected_format = None
        for magics, offset, fmt in SIGNATURES:
            bytes_to_read = max(len(magic) for magic in magics)
            f.seek(offset)
            data = read_exact(f, bytes_to_read)
            if any(data.startswith(magic) for magic in magics):
                detected_format = fmt
                break

        f.seek(0)

        # Check if it is a compressed tar file
        if detected_format in COMPRESSION_FORMAT_TO_TAR_FORMAT:
            assert detected_format is not None
            with open_stream(
                detected_format, f, get_archivey_config()
            ) as decompressed_stream:
                if is_uncompressed_tarfile(decompressed_stream):
                    detected_format = COMPRESSION_FORMAT_TO_TAR_FORMAT[detected_format]

            assert not f.closed
            f.seek(0)

        if detected_format is not None:
            return detected_format

        for detector, format in _EXTRA_DETECTORS:
            if detector(f):
                return format
            f.seek(0)

        # Check for SFX files
        if _is_executable(f):
            for detector, format in _SFX_DETECTORS:
                if detector(f):
                    return format
                f.seek(0)

        return ArchiveFormat.UNKNOWN


EXTENSION_TO_FORMAT = {
    ".tar": ArchiveFormat.TAR,
    ".tar.gz": ArchiveFormat.TAR_GZ,
    ".tar.bz2": ArchiveFormat.TAR_BZ2,
    ".tar.xz": ArchiveFormat.TAR_XZ,
    ".tar.zst": ArchiveFormat.TAR_ZSTD,
    ".tar.lz4": ArchiveFormat.TAR_LZ4,
    ".tgz": ArchiveFormat.TAR_GZ,
    ".tbz2": ArchiveFormat.TAR_BZ2,
    ".txz": ArchiveFormat.TAR_XZ,
    ".tzst": ArchiveFormat.TAR_ZSTD,
    ".tlz4": ArchiveFormat.TAR_LZ4,
    ".gz": ArchiveFormat.GZIP,
    ".bz2": ArchiveFormat.BZIP2,
    ".xz": ArchiveFormat.XZ,
    ".zst": ArchiveFormat.ZSTD,
    ".lz4": ArchiveFormat.LZ4,
    ".zip": ArchiveFormat.ZIP,
    ".rar": ArchiveFormat.RAR,
    ".7z": ArchiveFormat.SEVENZIP,
    ".iso": ArchiveFormat.ISO,
}


def has_tar_extension(filename: str) -> bool:
    base_filename, ext = os.path.splitext(filename.lower())
    return EXTENSION_TO_FORMAT.get(
        ext
    ) in TAR_COMPRESSED_FORMATS or base_filename.endswith(".tar")


def detect_archive_format_by_filename(filename: str) -> ArchiveFormat:
    """Detect the compression format of an archive based on its filename."""
    if os.path.isdir(filename):
        return ArchiveFormat.FOLDER
    filename_lower = filename.lower()
    for ext, format in EXTENSION_TO_FORMAT.items():
        if filename_lower.endswith(ext):
            return format

    return ArchiveFormat.UNKNOWN


logger = logging.getLogger(__name__)


def detect_archive_format(
    filename: str | os.PathLike | ReadableStreamLikeOrSimilar,
) -> ArchiveFormat:
    # Check if it's a directory first
    if isinstance(filename, os.PathLike):
        filename = str(filename)

    if isinstance(filename, str) and os.path.isdir(filename):
        return ArchiveFormat.FOLDER

    format_by_signature = detect_archive_format_by_signature(filename)

    if isinstance(filename, str):
        format_by_filename = detect_archive_format_by_filename(filename)
    else:
        format_by_filename = ArchiveFormat.UNKNOWN

    # If the signature indicates a single-file compression format but the
    # filename suggests a tar archive (e.g. .tar.gz), assume it's a tar file.
    # This avoids corrupted tar archives being misread as valid single-file
    # compressed files.

    if (
        format_by_filename == ArchiveFormat.UNKNOWN
        and format_by_signature == ArchiveFormat.UNKNOWN
    ):
        logger.warning("%s: Can't detect format by signature or filename", filename)
        return ArchiveFormat.UNKNOWN

    if format_by_signature == ArchiveFormat.UNKNOWN:
        logger.warning(
            "%s: Couldn't detect format by signature. Assuming %s",
            filename,
            format_by_filename,
        )
        return format_by_filename
    if format_by_filename == ArchiveFormat.UNKNOWN:
        logger.warning(
            "%s: Unknown extension. Detected %s", filename, format_by_signature
        )
    elif format_by_signature != format_by_filename:
        logger.warning(
            f"{filename}: Extension indicates {format_by_filename}, but detected ({format_by_signature})"
        )

    return format_by_signature
