import abc
from typing import IO, ContextManager

from archivey.formats import detect_archive_format_by_filename
from archivey.types import ArchiveMember, MemberType


class ArchiveWriter(abc.ABC):
    def __init__(self, archive_path: str):
        self.archive_path = archive_path

    @abc.abstractmethod
    def add_member(self, member: ArchiveMember) -> IO[bytes] | None:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass

    def __enter__(self) -> "ArchiveWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def open(self, filename: str) -> ContextManager[IO[bytes]]:
        member = ArchiveMember(
            filename=filename,
            type=MemberType.FILE,
            file_size=None,  # Will be determined by the amount of data written
            mtime=None,  # Should be set by the writer implementation
        )
        # TODO: The issue asks for `open` to return a stream, but `add_member` also returns a stream.
        # This means we'd have a stream that, when written to, writes to another stream.
        # This seems overly complicated.
        # For now, let's assume `add_member` handles the file creation and returns the stream,
        # and `open` is a convenience wrapper around it.
        # The returned stream from `add_member` for files should be manageable by a context manager.
        stream = self.add_member(member)
        if stream is None:
            # This should not happen if MemberType is FILE
            raise Exception(f"Could not open file {filename} for writing.")

        # Ensure the stream is a context manager
        if not (hasattr(stream, "__enter__") and hasattr(stream, "__exit__")):
            # Wrap it if it's not a context manager (e.g. io.BytesIO)
            # This is a simplified wrapper. A more robust one might be needed.
            class StreamContextWrapper:
                def __init__(self, stream_to_wrap: IO[bytes]):
                    self._stream = stream_to_wrap

                def __enter__(self) -> IO[bytes]:
                    return self._stream

                def __exit__(self, exc_type, exc_val, exc_tb):
                    # The stream should be closed by the ArchiveWriter's close method
                    # or by the specific writer's add_member implementation details.
                    # If direct closure here is needed, it needs careful consideration
                    # to not interfere with the archive finalization process.
                    pass # self._stream.close() if hasattr(self._stream, "close") else None
            return StreamContextWrapper(stream)
        return stream


    def add(self, name: str, type: MemberType, link_target: str | None = None) -> None:
        member = ArchiveMember(
            filename=name,
            type=type,
            link_target=link_target,
            file_size=None,
            mtime=None, # Should be set by the writer implementation
        )
        self.add_member(member)


def open_archive_writer(archive_path: str) -> ArchiveWriter:
    archive_format = detect_archive_format_by_filename(archive_path)
    # Importing writer classes here to avoid circular dependencies
    from archivey.zip_writer import ZipArchiveWriter  # Placeholder
    from archivey.tar_writer import TarArchiveWriter  # Placeholder

    if archive_format == archive_format.ZIP:
        return ZipArchiveWriter(archive_path)
    elif archive_format in [
        archive_format.TAR,
        archive_format.TAR_GZ,
        archive_format.TAR_BZ2,
        archive_format.TAR_XZ,
        archive_format.TAR_ZSTD,
        archive_format.TAR_LZ4,
    ]:
        return TarArchiveWriter(archive_path, archive_format) # Pass format for tar
    else:
        raise NotImplementedError(
            f"Archive format {archive_format} is not supported for writing."
        )
