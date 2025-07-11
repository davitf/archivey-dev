"""Core functionality for opening and interacting with archives."""

import io
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
    ConcatenationStream,
    ReadableBinaryStream,
    RecordableStream,
    ensure_binaryio,
    ensure_bufferedio,
    is_seekable,
    is_stream,
)
from archivey.internal.utils import ensure_not_none
from archivey.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    TAR_COMPRESSED_FORMATS,
    ArchiveFormat,
)


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


_FORMAT_TO_READER: dict[ArchiveFormat, Callable[..., ArchiveReader]] = {
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
    path_or_stream: ReadableBinaryStream | str | bytes | os.PathLike,
    *,
    config: ArchiveyConfig | None = None,
    streaming_only: bool = False,
    pwd: bytes | str | None = None,
    format: ArchiveFormat | None = None,
) -> ArchiveReader:
    """
    Open an archive file and return an appropriate ArchiveReader instance.

    This function auto-detects the archive format and selects the correct reader.
    It is the main entry point for users of the archivey library.

    Args:
        path_or_stream: Path to the archive file (e.g., "my_archive.zip", "data.tar.gz")
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

    stream, path = _normalize_path_or_stream(path_or_stream)

    recordable_stream: RecordableStream | None = None
    if stream is not None:
        assert not stream.closed
        # Many reader libraries expect the stream's read() method to return the
        # full data, so we need to ensure the stream is buffered.
        stream = ensure_bufferedio(stream)

        if not is_seekable(stream):
            recordable_stream = RecordableStream(stream)
            stream = recordable_stream

    if stream is None:
        assert path is not None
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archive file not found: {path}")

    format = detect_archive_format(ensure_not_none(stream or path))

    if stream is not None:
        assert not stream.closed

    if recordable_stream is not None:
        stream = recordable_stream.get_complete_stream()

    if format == ArchiveFormat.UNKNOWN:
        raise ArchiveNotSupportedError(
            f"Unknown archive format for {ensure_not_none(stream or path)}"
        )

    if format not in _FORMAT_TO_READER:
        raise ArchiveNotSupportedError(
            f"Unsupported archive format: {format} (for {ensure_not_none(stream or path)})"
        )

    reader_class = _FORMAT_TO_READER.get(format)

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
) -> BinaryIO:
    """Open a single-file compressed stream and return the uncompressed stream."""

    stream, path = _normalize_path_or_stream(path_or_stream)

    wrapper: RecordableStream | None = None
    if stream is not None:
        assert not stream.closed
        if not is_seekable(stream):
            original_stream = stream
            wrapper = RecordableStream(stream)
            stream = wrapper

    if stream is None:
        assert path is not None
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archive file not found: {path}")

    format = detect_archive_format(ensure_not_none(stream or path))

    if wrapper is not None:
        recorded_data = wrapper.get_all_data()
        stream = ConcatenationStream([io.BytesIO(recorded_data), original_stream])

    if format not in SINGLE_FILE_COMPRESSED_FORMATS:
        raise ArchiveNotSupportedError(
            f"Unsupported single-file compressed format: {format}"
        )

    if config is None:
        config = get_archivey_config()

    with archivey_config(config):
        return open_stream(format, ensure_not_none(stream or path), config)
