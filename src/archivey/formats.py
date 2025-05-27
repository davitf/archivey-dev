import io

from archivey.types import CompressionFormat

def detect_archive_format_by_signature(path_or_file: str | bytes | io.IOBase) -> CompressionFormat:
    SIGNATURES = [
        (b"\x50\x4B\x03\x04", CompressionFormat.ZIP),
        (b"\x52\x61\x72\x21\x1A\x07\x00", CompressionFormat.RAR),  # RAR4
        (b"\x52\x61\x72\x21\x1A\x07\x01\x00", CompressionFormat.RAR),  # RAR5
        (b"\x37\x7A\xBC\xAF\x27\x1C", CompressionFormat.SEVENZIP),
        (b"\x1F\x8B", CompressionFormat.GZIP),
        (b"\x42\x5A\x68", CompressionFormat.BZIP2),
        (b"\xFD\x37\x7A\x58\x5A\x00", CompressionFormat.XZ),
        (b"\x28\xB5\x2F\xFD", CompressionFormat.ZSTD),
        (b"\x04\x22\x4D\x18", CompressionFormat.LZ4),
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
                return CompressionFormat.TAR
    else:
        pos = f.tell()
        f.seek(257)
        if f.read(5) == b"ustar":
            return CompressionFormat.TAR
        f.seek(pos)

    return CompressionFormat.UNKNOWN 

_EXTENSION_TO_FORMAT = {
    ".tar.gz": CompressionFormat.TAR_GZ,
    ".tgz": CompressionFormat.TAR_GZ,
    ".tar.bz2": CompressionFormat.TAR_BZ2,
    ".tbz2": CompressionFormat.TAR_BZ2,
    ".tar.xz": CompressionFormat.TAR_XZ,
    ".txz": CompressionFormat.TAR_XZ,
    ".tar.zstd": CompressionFormat.TAR_ZSTD,
    ".tzst": CompressionFormat.TAR_ZSTD,
    ".tar.lz4": CompressionFormat.TAR_LZ4,
    ".tlz4": CompressionFormat.TAR_LZ4,
    ".zip": CompressionFormat.ZIP,
    ".rar": CompressionFormat.RAR,
    ".7z": CompressionFormat.SEVENZIP,
    ".gz": CompressionFormat.GZIP,
    ".bz2": CompressionFormat.BZIP2,
    ".tar": CompressionFormat.TAR,
}

def detect_archive_format_by_filename(filename: str) -> CompressionFormat:
    """Detect the compression format of an archive based on its filename."""
    filename_lower = filename.lower()
    for (ext, format) in _EXTENSION_TO_FORMAT.items():
        if filename_lower.endswith(ext):
            return format
    return CompressionFormat.UNKNOWN

