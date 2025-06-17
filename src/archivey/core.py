"""Core functionality for opening and interacting with archives."""

import os
from typing import Any

from archivey.base_reader import ArchiveReader, StreamingOnlyArchiveReaderWrapper
from archivey.config import ArchiveyConfig, default_config, get_default_config
from archivey.exceptions import ArchiveNotSupportedError
from archivey.folder_reader import FolderReader
from archivey.formats import detect_archive_format
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
    config: ArchiveyConfig | None = None,
    streaming_only: bool = False,
    **kwargs: Any,
) -> ArchiveReader:
    """
    Open an archive file and return an appropriate ArchiveReader instance.

    This function auto-detects the archive format and selects the correct reader.
    It is the main entry point for users of the archivey library.

    Args:
        archive_path: Path to the archive file (e.g., "my_archive.zip", "data.tar.gz").
            Can be a string, bytes, or an os.PathLike object.
        config: Optional ArchiveyConfig object to customize behavior. If None,
            default configuration is used.
        streaming_only: If True, forces the archive to be opened in a streaming-only
            mode, even if it supports random access. This can be useful for
            very large archives or when only sequential access is needed.
            Not all archive formats support this flag effectively.
        **kwargs: Additional keyword arguments, primarily `pwd` for password-protected
            archives.
            pwd (Optional[Union[str, bytes]]): Password to use for decrypting
                the archive.

    Returns:
        An ArchiveReader instance suitable for the detected archive format.

    Raises:
        FileNotFoundError: If the `archive_path` does not exist.
        ArchiveNotSupportedError: If the archive format is not supported or cannot
            be determined.
        ArchiveCorruptedError: If the archive is detected as corrupted during opening
            (some checks are format-specific).
        ArchiveEncryptedError: If the archive is encrypted and no password is provided,
            or if the provided password is incorrect.
        TypeError: If `archive_path` or `pwd` have an invalid type.

    Example:
        >>> from archivey import open_archive, ArchiveError
        >>>
        >>> try:
        ...     with open_archive("my_data.zip", pwd="secret") as archive:
        ...         for member in archive.get_members():
        ...             print(f"Found member: {member.filename}")
        ...         # Further operations with the archive
        ... except FileNotFoundError:
        ...     print("Error: Archive file not found.")
        ... except ArchiveError as e:
        ...     print(f"An archive error occurred: {e}")
    """
    archive_path = _normalize_archive_path(archive_path)

    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"Archive file not found: {archive_path}")

    format = detect_archive_format(_normalize_archive_path(archive_path))

    pwd = kwargs.get("pwd")
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    if config is None:
        config = get_default_config()

    with default_config(config):
        use_libarchive = config.use_libarchive

        reader: ArchiveReader

        if use_libarchive:
            raise NotImplementedError("LibArchiveReader is not implemented")

        if format == ArchiveFormat.RAR:
            from archivey.rar_reader import RarReader

            reader = RarReader(archive_path, pwd=kwargs.get("pwd"))

        elif format == ArchiveFormat.ZIP:
            from archivey.zip_reader import ZipReader

            reader = ZipReader(archive_path, pwd=kwargs.get("pwd"))

        elif format == ArchiveFormat.SEVENZIP:
            from archivey.sevenzip_reader import SevenZipReader

            reader = SevenZipReader(
                archive_path, pwd=kwargs.get("pwd"), streaming_only=streaming_only
            )

        elif format == ArchiveFormat.TAR or format in TAR_COMPRESSED_FORMATS:
            from archivey.tar_reader import TarReader

            reader = TarReader(
                archive_path,
                pwd=kwargs.get("pwd"),
                format=format,
                streaming_only=streaming_only,
            )

        elif format in SINGLE_FILE_COMPRESSED_FORMATS:
            from archivey.single_file_reader import SingleFileReader

            reader = SingleFileReader(
                archive_path, pwd=kwargs.get("pwd"), format=format
            )

        elif format == ArchiveFormat.ISO:
            raise NotImplementedError("ISO reader is not yet implemented")

        elif format == ArchiveFormat.FOLDER:
            reader = FolderReader(archive_path)

        else:
            raise ArchiveNotSupportedError(f"Unsupported archive format: {format}")

        if streaming_only:
            return StreamingOnlyArchiveReaderWrapper(reader)

        return reader
