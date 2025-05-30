"""Utility functions for detecting archive formats.

This module provides functions to identify the format of an archive file
based on its magic number (signature) and filename extension.
It helps in selecting the appropriate reader backend for a given archive.
"""
import io
import logging
import os

from archivey.types import (
    COMPRESSION_FORMAT_TO_TAR_FORMAT,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)


def detect_archive_format_by_signature(
    path_or_file: str | bytes | io.IOBase,
) -> ArchiveFormat:
    """Detects archive format by reading magic numbers (file signature).

    Args:
        path_or_file: Either a file path (str or bytes) or a file-like
                      object (must be seekable and readable in binary mode).
                      If a file-like object is provided, its current position
                      will be reset after reading.

    Returns:
        The detected `ArchiveFormat` enum member. Returns `ArchiveFormat.UNKNOWN`
        if the signature does not match any known format.
    """
    SIGNATURES = [
        (b"\x50\x4b\x03\x04", ArchiveFormat.ZIP),
        (b"\x52\x61\x72\x21\x1a\x07\x00", ArchiveFormat.RAR),  # RAR4
        (b"\x52\x61\x72\x21\x1a\x07\x01\x00", ArchiveFormat.RAR),  # RAR5
        (b"\x37\x7a\xbc\xaf\x27\x1c", ArchiveFormat.SEVENZIP),
        (b"\x1f\x8b", ArchiveFormat.GZIP),
        (b"\x42\x5a\x68", ArchiveFormat.BZIP2),
        (b"\xfd\x37\x7a\x58\x5a\x00", ArchiveFormat.XZ),
        (b"\x28\xb5\x2f\xfd", ArchiveFormat.ZSTD),
        (b"\x04\x22\x4d\x18", ArchiveFormat.LZ4),
    ]

    # Support both file paths and file-like objects
    close_after = False
    if isinstance(path_or_file, (str, bytes)):
        f = open(path_or_file, "rb")
        close_after = True
    else:
        f = path_or_file
        f.seek(0)

    sig = f.read(8)
    if close_after:
        f.close()

    for magic, name in SIGNATURES:
        if sig.startswith(magic):
            return name

    # Check for tar "ustar" magic at offset 257
    if isinstance(path_or_file, (str, bytes)):
        with open(path_or_file, "rb") as tf:
            tf.seek(257)
            if tf.read(5) == b"ustar":
                return ArchiveFormat.TAR
    else:
        pos = f.tell()
        f.seek(257)
        if f.read(5) == b"ustar":
            return ArchiveFormat.TAR
        f.seek(pos)

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
    ".zip": ArchiveFormat.ZIP,
    ".rar": ArchiveFormat.RAR,
    ".7z": ArchiveFormat.SEVENZIP,
}


def has_tar_extension(filename: str) -> bool:
    """Checks if a filename suggests it's a TAR archive (possibly compressed).

    This function looks for extensions like '.tar', '.tar.gz', '.tgz', etc.

    Args:
        filename: The name of the file to check.

    Returns:
        True if the filename has a common TAR-related extension, False otherwise.
    """
    base_filename, ext = os.path.splitext(filename.lower())
    # Check direct TAR compressed extensions (e.g., .tar.gz)
    if _EXTENSION_TO_FORMAT.get(ext) in TAR_COMPRESSED_FORMATS:
        return True
    # Check for .tar extension on the base filename if ext was a compression ext (e.g. file.tar.gz -> base_filename=file.tar, ext=.gz)
    if ext in {".gz", ".bz2", ".xz", ".zst", ".lz4"} and base_filename.endswith(".tar"):
        return True
    # Check for combined extensions like .tgz
    if ext in {".tgz", ".tbz2", ".txz", ".tzst", ".tlz4"}: # These are already in _EXTENSION_TO_FORMAT
        return True
    # Check if the primary extension is .tar itself
    if ext == ".tar":
        return True

    # A more direct check against known TAR extensions from _EXTENSION_TO_FORMAT
    # This handles cases like 'archive.tar' and 'archive.tar.gz' through the map.
    # Need to iterate carefully to match compound extensions first.
    filename_lower = filename.lower()
    # Prioritize longer extensions
    sorted_extensions = sorted(_EXTENSION_TO_FORMAT.keys(), key=len, reverse=True)
    for known_ext in sorted_extensions:
        if filename_lower.endswith(known_ext):
            return _EXTENSION_TO_FORMAT[known_ext] in TAR_COMPRESSED_FORMATS or \
                   _EXTENSION_TO_FORMAT[known_ext] == ArchiveFormat.TAR
    return False


def detect_archive_format_by_filename(filename: str) -> ArchiveFormat:
    """Detects the archive format based on its filename extension.

    Compound extensions like '.tar.gz' are handled.

    Args:
        filename: The name of the file.

    Returns:
        The detected `ArchiveFormat` enum member. Returns `ArchiveFormat.UNKNOWN`
        if the extension is not recognized.
    """
    filename_lower = filename.lower()
    # Prioritize longer extensions to match e.g. ".tar.gz" before ".gz"
    sorted_extensions = sorted(_EXTENSION_TO_FORMAT.keys(), key=len, reverse=True)
    for ext in sorted_extensions:
        if filename_lower.endswith(ext):
            return _EXTENSION_TO_FORMAT[ext]
    return ArchiveFormat.UNKNOWN


logger = logging.getLogger(__name__)


def detect_archive_format(filename: str) -> ArchiveFormat:
    """Detects the archive format by combining signature and filename analysis.

    It first attempts detection by signature. If the signature indicates a
    compression format (like GZIP, BZIP2) and the filename has a TAR-like
    extension (e.g., '.tar.gz', '.tgz'), it classifies it as the corresponding
    TAR compressed format. Otherwise, the signature-based format is used.

    A warning is logged if the signature-based detection and filename-based
    detection yield different primary formats (ignoring TAR compression).

    Args:
        filename: Path to the archive file.

    Returns:
        The most likely `ArchiveFormat` enum member.
    """
    format_by_signature = detect_archive_format_by_signature(filename)
    format_by_filename = detect_archive_format_by_filename(filename)

    # If signature indicates a compression format (like Gzip) AND filename suggests TAR (e.g. .tar.gz)
    # then it's a TAR compressed archive (e.g. TAR_GZ).
    if format_by_signature in COMPRESSION_FORMAT_TO_TAR_FORMAT and has_tar_extension(filename):
        detected_format = COMPRESSION_FORMAT_TO_TAR_FORMAT[format_by_signature]
    elif format_by_signature != ArchiveFormat.UNKNOWN:
        # Prefer signature if it's definitive and not just a compression layer for TAR
        detected_format = format_by_signature
    else:
        # Fallback to filename detection if signature is UNKNOWN
        detected_format = format_by_filename


    # Log a warning if there's a significant discrepancy.
    # We compare the base format from filename (e.g. TAR from TAR_GZ) vs signature.
    # Or if one is UNKNOWN and the other is not.
    final_signature_format_to_compare = format_by_signature
    if format_by_signature in TAR_COMPRESSED_FORMATS: # e.g. TAR_GZ from signature (less likely but possible)
        final_signature_format_to_compare = ArchiveFormat.TAR
    elif format_by_signature in COMPRESSION_FORMAT_TO_TAR_FORMAT: # e.g. GZIP from signature
         # If has_tar_extension was true, detected_format would be TAR_GZ.
         # If not, it remains GZIP. We compare GZIP.
         pass


    final_filename_format_to_compare = format_by_filename
    if format_by_filename in TAR_COMPRESSED_FORMATS: # e.g. TAR_GZ from filename
        final_filename_format_to_compare = ArchiveFormat.TAR


    if final_signature_format_to_compare != ArchiveFormat.UNKNOWN and \
       final_filename_format_to_compare != ArchiveFormat.UNKNOWN and \
       final_signature_format_to_compare != final_filename_format_to_compare:
        # This condition means, e.g. signature is ZIP, filename is RAR.
        logger.warning(
            f'"{filename}": Format detected by signature ({format_by_signature}) '
            f"differs from format detected by filename ({format_by_filename}). "
            f"Using detected format: {detected_format}."
        )
    elif (final_signature_format_to_compare == ArchiveFormat.UNKNOWN and detected_format != ArchiveFormat.UNKNOWN) or \
         (final_filename_format_to_compare == ArchiveFormat.UNKNOWN and detected_format != ArchiveFormat.UNKNOWN and format_by_signature != ArchiveFormat.UNKNOWN ):
        # Log if one method found something and the other didn't, but we still got a result.
        # This avoids logging if both are UNKNOWN.
        logger.info(
            f'"{filename}": Signature detection: {format_by_signature}, '
            f"Filename detection: {format_by_filename}. "
            f"Effective format: {detected_format}."
        )


    return detected_format
