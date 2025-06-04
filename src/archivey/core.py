import os
from typing import Any

from archivey.base_reader import ArchiveReader
from archivey.exceptions import ArchiveNotSupportedError
from archivey.folder_reader import FolderReader
from archivey.formats import detect_archive_format
from archivey.iso_reader import IsoReader
from archivey.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)


def _normalize_archive_path(archive_path: str | bytes | os.PathLike) -> str:
    if isinstance(archive_path, os.PathLike):
        return str(archive_path)
    elif isinstance(archive_path, bytes):
        return archive_path.decode("utf-8")
    elif isinstance(archive_path, str):
        return archive_path

    raise TypeError(f"Invalid archive path type: {type(archive_path)} {archive_path}")


def open_archive(
    archive_path: str | bytes | os.PathLike,
    *,
    use_libarchive: bool = False,
    use_rar_stream: bool = False,
    **kwargs: Any,
) -> ArchiveReader:
    """Open an archive and return the appropriate reader."""
    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"Archive file not found: {archive_path}")

    format = detect_archive_format(_normalize_archive_path(archive_path))

    pwd = kwargs.get("pwd")
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    if use_libarchive:
        raise NotImplementedError("LibArchiveReader is not implemented")

    if format == ArchiveFormat.RAR:
        if use_rar_stream:
            from archivey.rar_reader import RarStreamReader

            return RarStreamReader(archive_path, pwd=pwd)
        else:
            from archivey.rar_reader import RarReader

            return RarReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.ZIP:
        from archivey.zip_reader import ZipReader

        return ZipReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.SEVENZIP:
        from archivey.sevenzip_reader import SevenZipReader

        return SevenZipReader(archive_path, pwd=pwd)

    if format == ArchiveFormat.TAR or format in TAR_COMPRESSED_FORMATS:
        from archivey.tar_reader import TarReader

        return TarReader(archive_path, pwd=pwd, format=format)

    if format in SINGLE_FILE_COMPRESSED_FORMATS:
        from archivey.single_file_reader import SingleFileReader

        return SingleFileReader(
            archive_path,
            pwd=pwd,
            format=format,
            use_stored_metadata=kwargs.get("use_single_file_stored_metadata", False),
        )

    if format == ArchiveFormat.ISO:
        return IsoReader(archive_path, password=pwd)

    if format == ArchiveFormat.FOLDER:
        return FolderReader(archive_path)

    raise ArchiveNotSupportedError(f"Unsupported archive format: {format}")
