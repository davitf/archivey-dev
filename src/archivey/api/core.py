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
from archivey.formats.rar_reader import RarReader
from archivey.formats.sevenzip_reader import SevenZipReader
from archivey.formats.single_file_reader import SingleFileReader
from archivey.formats.tar_reader import TarReader
from archivey.formats.zip_reader import ZipReader
from archivey.internal.base_reader import (
    ArchiveReader,
    StreamingOnlyArchiveReaderWrapper,
)
from archivey.internal.io_helpers import LimitedSeekStreamWrapper, is_seekable


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

    archive_path_input = _normalize_archive_path(archive_path)
    stream_for_detection: BinaryIO | str = archive_path_input
    stream_for_reader: BinaryIO | str = archive_path_input # This will be passed to the reader

    if isinstance(archive_path_input, str):
        if not os.path.exists(archive_path_input):
            raise FileNotFoundError(
                f"Archive file not found: {archive_path_input}"
            )
    elif hasattr(archive_path_input, "read"): # It's a stream object
        if streaming_only and not is_seekable(archive_path_input):
            # If streaming_only and original stream is not seekable,
            # wrap it for detection AND for the reader.
            # The LimitedSeekStreamWrapper needs to be used consistently.
            # Its internal buffer will be consumed by detection, so the reader needs the same instance.
            wrapped_stream = LimitedSeekStreamWrapper(archive_path_input, buffer_size=65536)
            stream_for_detection = wrapped_stream
            stream_for_reader = wrapped_stream # Reader gets the wrapped stream
        # If not (streaming_only and not is_seekable), detection and reader use original stream_for_detection
    # else: it's a path string, detection and reader use it directly

    detected_format = detect_archive_format(stream_for_detection)

    if detected_format == ArchiveFormat.UNKNOWN:
        # If detection failed with a wrapped stream, it implies the format might not
        # be detectable even with limited seeking, or the wrapper interfered.
        # There isn't a simple fallback to the original stream here if it was non-seekable,
        # as detection would have failed there too.
        raise ArchiveNotSupportedError(
            f"Unknown archive format for {archive_path_input}"
        )

    if detected_format not in _FORMAT_TO_READER:
        raise ArchiveNotSupportedError(
            f"Unsupported archive format: {detected_format} (for {archive_path_input})"
        )

    reader_class = _FORMAT_TO_READER.get(detected_format)

    if config is None:
        config = get_default_config()

    with default_config(config):
        if detected_format == ArchiveFormat.FOLDER:
            assert isinstance(stream_for_reader, str), ( # Should be archive_path_input if FOLDER
                "FolderReader only supports string paths"
            )
            reader = FolderReader(stream_for_reader)
        else:
            assert reader_class is not None
            reader = reader_class(
                format=detected_format,
                archive_path=stream_for_reader, # Pass the (potentially wrapped) stream
                pwd=pwd,
                streaming_only=streaming_only,
            )

        if streaming_only:
            # If the reader itself is already a streaming type due to stream_for_reader
            # being a LimitedSeekStreamWrapper, the StreamingOnlyArchiveReaderWrapper might be redundant
            # in some aspects but still enforces the streaming API contract.
            # If stream_for_reader was the original seekable stream, or a path,
            # and the reader chose a random-access backend, this wrapper makes it streaming.
            if isinstance(reader, StreamingOnlyArchiveReaderWrapper): # Should not happen with current logic
                return reader
            return StreamingOnlyArchiveReaderWrapper(reader)
        else:
            return reader
