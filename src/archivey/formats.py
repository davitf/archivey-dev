import logging
import os
from typing import BinaryIO, cast

from archivey.types import (
    COMPRESSION_FORMAT_TO_TAR_FORMAT,
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)

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


def detect_archive_format_by_signature(
    path_or_file: str | bytes | BinaryIO,
) -> ArchiveFormat:
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
        ([b"\x1f\x9d"], 0, ArchiveFormat.COMPRESS_Z),
        ([b"ustar"], 257, ArchiveFormat.TAR),  # TAR "ustar" magic
        (_ISO_MAGIC_BYTES, 0x8001, ArchiveFormat.ISO),  # ISO9660
    ]
    f: BinaryIO

    if isinstance(path_or_file, (str, bytes)):
        # If it's a path, check if it's a directory first
        if os.path.isdir(path_or_file):
            return ArchiveFormat.FOLDER
        try:
            f = open(path_or_file, "rb")
            close_after = True
        except FileNotFoundError:
            return (
                ArchiveFormat.UNKNOWN
            )  # Or raise error, depending on desired behavior
    elif hasattr(path_or_file, "read") and hasattr(path_or_file, "seek"):
        f = cast(BinaryIO, path_or_file)
        # We can't check is_dir on a stream, assume it's not a folder for streams
        close_after = False
    else:
        # Not a path and not a stream
        return ArchiveFormat.UNKNOWN

    try:
        for magics, offset, fmt in SIGNATURES:
            bytes_to_read = max(len(magic) for magic in magics)
            f.seek(offset)
            data = f.read(bytes_to_read)
            if any(data.startswith(magic) for magic in magics):
                return fmt

    finally:
        if close_after:
            f.close()

    return ArchiveFormat.UNKNOWN


_EXTENSION_TO_FORMAT = {
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
    ".z": ArchiveFormat.COMPRESS_Z,
    ".br": ArchiveFormat.BROTLI,
    ".zip": ArchiveFormat.ZIP,
    ".rar": ArchiveFormat.RAR,
    ".7z": ArchiveFormat.SEVENZIP,
    ".iso": ArchiveFormat.ISO,
}


def has_tar_extension(filename: str) -> bool:
    base_filename, ext = os.path.splitext(filename.lower())
    return _EXTENSION_TO_FORMAT.get(
        ext
    ) in TAR_COMPRESSED_FORMATS or base_filename.endswith(".tar")


def detect_archive_format_by_filename(filename: str) -> ArchiveFormat:
    """Detect the compression format of an archive based on its filename."""
    if os.path.isdir(filename):
        return ArchiveFormat.FOLDER
    filename_lower = filename.lower()
    for ext, format in _EXTENSION_TO_FORMAT.items():
        if filename_lower.endswith(ext):
            return format

    return ArchiveFormat.UNKNOWN


logger = logging.getLogger(__name__)


def detect_archive_format(filename: str) -> ArchiveFormat:
    # Check if it's a directory first
    if os.path.isdir(filename):
        return ArchiveFormat.FOLDER

    format_by_signature = detect_archive_format_by_signature(filename)
    format_by_filename = detect_archive_format_by_filename(filename)

    # The signature detection doesn't know if a .gz/.bz2/.xz file is a tar file,
    # so we need to check the filename.
    # To detect cases like a .tar.gz file mistakenly having been renamed to .zip,
    # assume it's a tar file if the compression format is supported by tar
    # and the filename matches any multi-file format.
    if (
        format_by_signature in COMPRESSION_FORMAT_TO_TAR_FORMAT
        and format_by_filename not in SINGLE_FILE_COMPRESSED_FORMATS
    ):
        format_by_signature = COMPRESSION_FORMAT_TO_TAR_FORMAT[format_by_signature]

    if (
        format_by_filename == ArchiveFormat.UNKNOWN
        and format_by_signature == ArchiveFormat.UNKNOWN
    ):
        logger.warning(f"{filename}: Can't detect format by signature or filename")
        return ArchiveFormat.UNKNOWN

    if format_by_signature == ArchiveFormat.UNKNOWN:
        logger.warning(
            f"{filename}: Couldn't detect format by signature. Assuming {format_by_filename}"
        )
        return format_by_filename
    elif format_by_filename == ArchiveFormat.UNKNOWN:
        logger.warning(f"{filename}: Unknown extension. Detected {format_by_signature}")
    elif format_by_signature != format_by_filename:
        logger.warning(
            f"{filename}: Extension indicates {format_by_filename}, but detected ({format_by_signature})"
        )

    return format_by_signature
