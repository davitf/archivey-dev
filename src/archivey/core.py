"""Core functionality for opening and interacting with archives."""

import os
from typing import BinaryIO, Callable

from archivey.archive_reader import ArchiveReader
from archivey.config import ArchiveyConfig, archivey_config, get_archivey_config
from archivey.exceptions import ArchiveNotSupportedError
from archivey.formats.compressed_streams import open_stream
from archivey.formats.folder_reader import FolderReader
from archivey.formats.format_detection import detect_archive_format
from archivey.formats.rar_reader import RarReader
from archivey.formats.sevenzip_reader import SevenZipReader
from archivey.formats.single_file_reader import SingleFileReader
from archivey.formats.tar_reader import TarReader
from archivey.formats.zip_reader import ZipReader
from archivey.internal.io_helpers import (
    ReadableBinaryStream,
    RewindableStreamWrapper,
    ensure_binaryio,
    ensure_bufferedio,
    fix_stream_start_position,
    is_seekable,
    is_stream,
)
from archivey.internal.utils import ensure_not_none
from archivey.types import ArchiveFormat, ContainerFormat, StreamFormat


def _normalize_path_or_stream(
    archive_path: ReadableBinaryStream | str | bytes | os.PathLike,
) -> tuple[BinaryIO | None, str | None]:
    if is_stream(archive_path):
        return ensure_binaryio(archive_path), None
    if isinstance(archive_path, os.PathLike):
        return None, str(archive_path)
    if isinstance(archive_path, bytes):
        return None, archive_path.decode("utf-8")
    if isinstance(archive_path, str):
        return None, archive_path

    raise TypeError(f"Invalid archive path type: {type(archive_path)} {archive_path}")


_FORMAT_TO_READER: dict[ContainerFormat, Callable[..., ArchiveReader]] = {
    ContainerFormat.RAR: RarReader,
    ContainerFormat.ZIP: ZipReader,
    ContainerFormat.SEVENZIP: SevenZipReader,
    ContainerFormat.TAR: TarReader,
    ContainerFormat.FOLDER: FolderReader,
    ContainerFormat.RAW_STREAM: SingleFileReader,
}


def open_archive(
    path_or_stream: str | bytes | os.PathLike | ReadableBinaryStream,
    *,
    config: ArchiveyConfig | None = None,
    streaming_only: bool = False,
    pwd: bytes | str | None = None,
    format: ArchiveFormat | ContainerFormat | StreamFormat | None = None,
) -> ArchiveReader:
    """
    Open an archive file and return an [ArchiveReader][archivey.ArchiveReader] instance.

    Args:
        path_or_stream: Path to the archive file (e.g., "my_archive.zip", "data.tar.gz")
            or a binary file-like object containing the archive data.
        config: Optional [ArchiveyConfig][archivey.ArchiveyConfig] object to customize
            behavior. If `None`, the default configuration (which may have been
            customized with [set_archivey_config][archivey.set_archivey_config]) is
            used.
        streaming_only: If `True`, forces the archive to be opened in a streaming-only
            mode, even if it supports random access. This can be more efficient if you
            only need to extract the archive or iterate over its members once.

            If set to `True`, disables random access methods like `open()` and
            `extract()` to avoid expensive seeks or rewinds. Calls to those methods will
            raise a `ValueError`.
        pwd: Optional password used to decrypt the archive if it is encrypted.
        format: Optional archive format to use. If `None`, the format is auto-detected.

    Returns:
        An ArchiveReader instance for working with the archive.

    Raises:
        FileNotFoundError: If `path_or_stream` points to a non-existent file.
        ArchiveNotSupportedError: If the archive format is not supported or cannot
            be determined.
        ArchiveCorruptedError: If the archive is detected as corrupted during opening.
        ArchiveEncryptedError: If the archive is encrypted and no password is provided,
            or if the provided password is incorrect. This will only be raised here
            if the archive header is encrypted; otherwise, the incorrect password
            may only be detected when attempting to read an encrypted member.
        TypeError: If `path_or_stream` or `pwd` have an invalid type.

    Example:
        ```python
        from archivey import open_archive, ArchiveError

        try:
            with open_archive("my_data.zip", pwd="secret") as archive:
                print(f"Members: {archive.get_members()}")
                # Further operations with the archive
        except FileNotFoundError:
            print("Error: Archive file not found.")
        except ArchiveError as e:
            print(f"An archive error occurred: {e}")
        ```
    """
    if pwd is not None and not isinstance(pwd, (str, bytes)):
        raise TypeError("Password must be a string or bytes")

    stream: BinaryIO | None
    path: str | None
    stream, path = _normalize_path_or_stream(path_or_stream)

    rewindable_wrapper: RewindableStreamWrapper | None = None
    if stream is not None:
        assert not stream.closed
        if is_seekable(stream):
            stream.seek(0)

        # Many reader libraries expect the stream's read() method to return the
        # full data, so we need to ensure the stream is buffered.
        rewindable_wrapper = RewindableStreamWrapper(ensure_bufferedio(stream))
        stream = rewindable_wrapper.get_stream()

    else:
        assert path is not None
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archive file not found: {path}")

    if format is None:
        with archivey_config(config):
            format = detect_archive_format(ensure_not_none(stream or path))

    if isinstance(format, ContainerFormat):
        format = ArchiveFormat(format, StreamFormat.UNCOMPRESSED)
    elif isinstance(format, StreamFormat):
        format = ArchiveFormat(ContainerFormat.RAW_STREAM, format)

    if rewindable_wrapper is not None:
        stream = rewindable_wrapper.get_rewinded_stream()
        assert not stream.closed

    if format == ArchiveFormat.UNKNOWN:
        raise ArchiveNotSupportedError(
            f"Unknown archive format for {ensure_not_none(stream or path)}"
        )

    if format.container not in _FORMAT_TO_READER:
        raise ArchiveNotSupportedError(
            f"Unsupported archive format: {format} (for {ensure_not_none(stream or path)})"
        )

    reader_class = _FORMAT_TO_READER.get(format.container)

    if config is None:
        config = get_archivey_config()

    if stream is not None:
        assert not stream.closed

    with archivey_config(config):
        assert reader_class is not None
        return reader_class(
            format=format,
            archive_path=ensure_not_none(stream or path),
            pwd=pwd,
            streaming_only=streaming_only,
        )


def open_compressed_stream(
    path_or_stream: BinaryIO | str | bytes | os.PathLike,
    *,
    config: ArchiveyConfig | None = None,
    format: ArchiveFormat | StreamFormat | None = None,
) -> BinaryIO:
    """Open a single-file compressed stream and return the uncompressed stream.

    This function ensures that if a stream is passed, reading starts from the
    stream's current position at the time of the call, after any internal
    operations like format detection (which might require reading from the
    beginning of the stream).

    Args:
        path_or_stream: Path to the compressed file (e.g., "my_data.gz", "data.bz2")
            or a binary file-like object containing the compressed data.
        config: Optional [ArchiveyConfig][archivey.ArchiveyConfig] object to customize
            behavior. If `None`, the default configuration (which may have been
            customized with [set_archivey_config][archivey.set_archivey_config]) is
            used.
        format: Optional archive format to use. If `None`, the format is auto-detected.

    Returns:
        A binary file-like object containing the uncompressed data.

    Raises:
        FileNotFoundError: If `path_or_stream` points to a non-existent file.
        ArchiveNotSupportedError: If the archive format is not supported or cannot
            be determined.
        ArchiveCorruptedError: If the archive is detected as corrupted during opening.
        TypeError: If `path_or_stream` has an invalid type.
    """
    stream: BinaryIO | None
    path: str | None

    stream, path = _normalize_path_or_stream(path_or_stream)

    rewindable_wrapper: RewindableStreamWrapper | None = None
    if stream is not None:
        assert not stream.closed

        # If the stream is not at the start, get a wrapper streams that start at the
        # current position, so format detection and the stream readers can seek to 0
        # and read where the compressed data starts.
        if stream is not None:
            stream = fix_stream_start_position(stream)

        # Many reader libraries expect the stream's read() method to return the
        # full data, so we need to ensure the stream is buffered.
        rewindable_wrapper = RewindableStreamWrapper(ensure_bufferedio(stream))
        stream = rewindable_wrapper.get_stream()

    else:
        assert path is not None
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archive file not found: {path}")

    if format is None:
        format = detect_archive_format(
            ensure_not_none(stream or path), detect_compressed_tar=False
        )

    if rewindable_wrapper is not None:
        stream = rewindable_wrapper.get_rewinded_stream()

    if isinstance(format, ArchiveFormat):
        if format.container != ContainerFormat.RAW_STREAM:
            raise ArchiveNotSupportedError(
                f"Unsupported single-file compressed format: {format}"
            )
        format = format.stream

    if config is None:
        config = get_archivey_config()

    return open_stream(format, ensure_not_none(stream or path), config)
