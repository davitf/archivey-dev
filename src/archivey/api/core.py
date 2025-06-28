"""Core functionality for opening and interacting with archives."""

import os
from typing import BinaryIO

from archivey.api.config import ArchiveyConfig, default_config, get_default_config
from archivey.api.exceptions import ArchiveNotSupportedError
from archivey.api.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)
from archivey.formats.folder_reader import FolderReader
from archivey.formats.format_detection import detect_archive_format
from archivey.formats.compressed_streams import open_stream
from archivey.formats.rar_reader import RarReader
from archivey.formats.sevenzip_reader import SevenZipReader
from archivey.formats.single_file_reader import SingleFileReader
from archivey.formats.tar_reader import TarReader
from archivey.formats.zip_reader import ZipReader
from archivey.internal.base_reader import (
    ArchiveReader,
    StreamingOnlyArchiveReaderWrapper,
)
from archivey.internal.io_helpers import is_seekable, RewindableNonSeekableStream


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


_FORMAT_TO_READER = {
    ArchiveFormat.RAR: RarReader,
    ArchiveFormat.ZIP: ZipReader,
    ArchiveFormat.SEVENZIP: SevenZipReader,
    ArchiveFormat.TAR: TarReader,
    ArchiveFormat.FOLDER: FolderReader,
}

for format in TAR_COMPRESSED_FORMATS:
    _FORMAT_TO_READER[format] = TarReader

for format in SINGLE_FILE_COMPRESSED_FORMATS:
    _FORMAT_TO_READER[format] = SingleFileReader


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
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    archive_path_normalized = _normalize_archive_path(archive_path)

    wrapper: RewindableNonSeekableStream | None = None
    if not isinstance(archive_path_normalized, str) and not is_seekable(archive_path_normalized):
        wrapper = RewindableNonSeekableStream(archive_path_normalized)
        archive_path_normalized = wrapper

    if isinstance(archive_path_normalized, str):
        if not os.path.exists(archive_path_normalized):
            raise FileNotFoundError(
                f"Archive file not found: {archive_path_normalized}"
            )

    format = detect_archive_format(archive_path_normalized)

    if wrapper is not None:
        wrapper.rewind()
        wrapper.disable_rewind()
    if format == ArchiveFormat.UNKNOWN:
        raise ArchiveNotSupportedError(
            f"Unknown archive format for {archive_path_normalized}"
        )

    if format not in _FORMAT_TO_READER:
        raise ArchiveNotSupportedError(
            f"Unsupported archive format: {format} (for {archive_path_normalized})"
        )

    reader_class = _FORMAT_TO_READER.get(format)

    if config is None:
        config = get_default_config()

    with default_config(config):
        if format == ArchiveFormat.FOLDER:
            assert isinstance(archive_path_normalized, str), (
                "FolderReader only supports string paths"
            )
            reader = FolderReader(archive_path_normalized)
        else:
            assert reader_class is not None
            reader = reader_class(
                format=format,
                archive_path=archive_path_normalized,
                pwd=pwd,
                streaming_only=streaming_only,
            )

        if streaming_only:
            return StreamingOnlyArchiveReaderWrapper(reader)
        else:
            return reader


def open_compressed_stream(
    archive_path: BinaryIO | str | bytes | os.PathLike,
    *,
    config: ArchiveyConfig | None = None,
) -> BinaryIO:
    """Open a single-file compressed stream and return the uncompressed stream."""

    archive_path_normalized = _normalize_archive_path(archive_path)

    wrapper: RewindableNonSeekableStream | None = None
    if not isinstance(archive_path_normalized, str) and not is_seekable(archive_path_normalized):
        wrapper = RewindableNonSeekableStream(archive_path_normalized)
        archive_path_normalized = wrapper

    if isinstance(archive_path_normalized, str) and not os.path.exists(
        archive_path_normalized
    ):
        raise FileNotFoundError(
            f"Archive file not found: {archive_path_normalized}"
        )

    format = detect_archive_format(archive_path_normalized)

    if wrapper is not None:
        wrapper.rewind()
        wrapper.disable_rewind()

    if format not in SINGLE_FILE_COMPRESSED_FORMATS:
        raise ArchiveNotSupportedError(
            f"Unsupported single-file compressed format: {format}"
        )

    if config is None:
        config = get_default_config()

    with default_config(config):
        return open_stream(format, archive_path_normalized, config)
