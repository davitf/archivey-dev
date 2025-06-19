"""Core functionality for opening and interacting with archives."""

import os
from typing import BinaryIO

from archivey.base_reader import ArchiveReader, StreamingOnlyArchiveReaderWrapper
from archivey.compressed_streams import open_stream_fileobj
from archivey.config import ArchiveyConfig, default_config, get_default_config
from archivey.exceptions import ArchiveNotSupportedError
from archivey.folder_reader import FolderReader
from archivey.formats import (
    detect_archive_format,
    detect_archive_format_by_signature,
)
from archivey.types import (
    COMPRESSION_FORMAT_TO_TAR_FORMAT,
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)


def _normalize_archive_path(
    archive_path: BinaryIO | str | bytes | os.PathLike,
) -> BinaryIO | str:
    if hasattr(archive_path, "read"):
        return archive_path  # type: ignore[return-value]
    if isinstance(archive_path, os.PathLike):
        return str(archive_path)
    elif isinstance(archive_path, bytes):
        return archive_path.decode("utf-8")
    elif isinstance(archive_path, str):
        return archive_path

    raise TypeError(f"Invalid archive path type: {type(archive_path)} {archive_path}")


def open_archive(
    archive_path: BinaryIO | str | bytes | os.PathLike,
    *,
    config: ArchiveyConfig | None = None,
    streaming_only: bool = False,
    pwd: bytes | str | None = None,
) -> ArchiveReader:
    """
    Open an archive file and return an appropriate ArchiveReader instance.

    This function auto-detects the archive format and selects the correct reader.
    It is the main entry point for users of the archivey library.

    Args:
        archive_path: Path to the archive file (e.g., "my_archive.zip", "data.tar.gz")
            or a binary file object containing the archive data.
        config: Optional ArchiveyConfig object to customize behavior. If None,
            default configuration is used.
        streaming_only: If True, forces the archive to be opened in a streaming-only
            mode, even if it supports random access. This can be useful for
            very large archives or when only sequential access is needed.
            Not all archive formats support this flag effectively.
        pwd: Optional password (str or bytes) used to decrypt the archive if it
            is encrypted.

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
    archive_path_normalized = _normalize_archive_path(archive_path)

    if isinstance(archive_path_normalized, str):
        if not os.path.exists(archive_path_normalized):
            raise FileNotFoundError(
                f"Archive file not found: {archive_path_normalized}"
            )
        format = detect_archive_format(archive_path_normalized)
    else:
        try:
            archive_path_normalized.seek(0)
        except Exception:
            pass
        format = detect_archive_format_by_signature(archive_path_normalized)
        if format in COMPRESSION_FORMAT_TO_TAR_FORMAT:
            try:
                archive_path_normalized.seek(0)
                stream = open_stream_fileobj(
                    format, archive_path_normalized, get_default_config()
                )
                head = stream.read(262)
                if len(head) >= 262 and head[257:262].startswith(b"ustar"):
                    format = COMPRESSION_FORMAT_TO_TAR_FORMAT[format]
            finally:
                try:
                    stream.close()
                except Exception:
                    pass
        try:
            archive_path_normalized.seek(0)
        except Exception:
            pass

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

            reader = RarReader(archive_path_normalized, pwd=pwd)

        elif format == ArchiveFormat.ZIP:
            from archivey.zip_reader import ZipReader

            reader = ZipReader(archive_path_normalized, pwd=pwd)

        elif format == ArchiveFormat.SEVENZIP:
            from archivey.sevenzip_reader import SevenZipReader

            reader = SevenZipReader(
                archive_path_normalized,
                pwd=pwd,
                streaming_only=streaming_only,
            )

        elif format == ArchiveFormat.TAR or format in TAR_COMPRESSED_FORMATS:
            from archivey.tar_reader import TarReader

            reader = TarReader(
                archive_path_normalized,
                pwd=pwd,
                format=format,
                streaming_only=streaming_only,
            )

        elif format in SINGLE_FILE_COMPRESSED_FORMATS:
            from archivey.single_file_reader import SingleFileReader

            reader = SingleFileReader(
                archive_path_normalized,
                pwd=pwd,
                format=format,
            )

        elif format == ArchiveFormat.ISO:
            raise NotImplementedError("ISO reader is not yet implemented")

        elif format == ArchiveFormat.FOLDER:
            assert isinstance(archive_path_normalized, str), (
                "FolderReader only supports string paths"
            )
            reader = FolderReader(archive_path_normalized)

        else:
            raise ArchiveNotSupportedError(f"Unsupported archive format: {format}")

        if streaming_only:
            return StreamingOnlyArchiveReaderWrapper(reader)

        return reader
