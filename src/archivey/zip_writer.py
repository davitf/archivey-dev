import zipfile
import io
from datetime import datetime
import stat

from archivey.base_writer import ArchiveWriter
from archivey.types import ArchiveMember, MemberType, CreateSystem


class ZipArchiveWriter(ArchiveWriter):
    def __init__(self, archive_path: str):
        super().__init__(archive_path)
        self._zipfile = zipfile.ZipFile(self.archive_path, "w", zipfile.ZIP_DEFLATED)
        self._open_streams = [] # To keep track of streams that need to be closed explicitly

    def add_member(self, member: ArchiveMember) -> io.IO[bytes] | None:
        # Default mtime to now if not provided
        mtime = member.mtime or datetime.now()
        date_time_tuple = mtime.timetuple()[:6]

        # Create ZipInfo object
        # For directories, ensure the name ends with a slash
        filename = member.filename
        if member.type == MemberType.DIR and not filename.endswith("/"):
            filename += "/"

        zip_info = zipfile.ZipInfo(filename, date_time=date_time_tuple)

        # Set permissions and type
        if member.type == MemberType.FILE:
            zip_info.external_attr = (stat.S_IFREG | (member.mode or 0o644)) << 16
        elif member.type == MemberType.DIR:
            zip_info.external_attr = (stat.S_IFDIR | (member.mode or 0o755)) << 16
        elif member.type == MemberType.SYMLINK:
            zip_info.external_attr = (stat.S_IFLNK | (member.mode or 0o777)) << 16
        else:
            # For OTHER or unhandled types, default to regular file permissions
            zip_info.external_attr = (stat.S_IFREG | 0o644) << 16

        # Set compression method if specified, otherwise use default from constructor
        if member.compression_method:
            if member.compression_method.lower() == "store":
                zip_info.compress_type = zipfile.ZIP_STORED
            elif member.compression_method.lower() == "deflate":
                zip_info.compress_type = zipfile.ZIP_DEFLATED
            elif member.compression_method.lower() == "bzip2":
                zip_info.compress_type = zipfile.ZIP_BZIP2
            elif member.compression_method.lower() == "lzma":
                zip_info.compress_type = zipfile.ZIP_LZMA
            else:
                # Default or warn for unsupported methods for zip
                zip_info.compress_type = self._zipfile.compression
        else:
            zip_info.compress_type = self._zipfile.compression

        if member.comment:
            zip_info.comment = member.comment.encode('utf-8') # zipfile expects bytes

        # Handle create_system (optional, zipfile sets it based on platform)
        # Example: zip_info.create_system = CreateSystem.UNIX.value

        if member.type == MemberType.FILE:
            # For files, return a stream that can be written to.
            # The data will be written to the archive when the stream is closed.
            # `zipfile.ZipFile.open()` in write mode returns a ZipExtFile which is an IO[bytes]
            # and handles writing to the archive upon its close().
            # No, zipfile.open() in write mode is for writing *into* the zip file.
            # We need to use writestr for symlinks and directories, and provide a stream for files.

            # The issue with `self._zipfile.open(zip_info, "w")` is that it expects all data
            # to be written *before* the next member is added or the archive is closed.
            # This doesn't align well with the `open("file.txt") as outfile: outfile.write(...)` API
            # where writes can be interleaved.
            # A workaround is to buffer the content in memory and write it on close,
            # or write to a temporary file. For simplicity, let's use an in-memory buffer.

            class ZipFileStream(io.BytesIO):
                def __init__(self, zip_file_obj: zipfile.ZipFile, zip_info_obj: zipfile.ZipInfo):
                    super().__init__()
                    self._zip_file_obj = zip_file_obj
                    self._zip_info_obj = zip_info_obj
                    self._closed = False

                def close(self) -> None:
                    if not self._closed:
                        self.seek(0)
                        self._zip_file_obj.writestr(self._zip_info_obj, self.read())
                        self._closed = True
                    super().close()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self.close()

            stream = ZipFileStream(self._zipfile, zip_info)
            self._open_streams.append(stream) # Keep track to ensure it's closed
            return stream

        elif member.type == MemberType.DIR:
            # writestr with empty bytes for a directory. Ensure name ends with /.
            self._zipfile.writestr(zip_info, b"")
            return None
        elif member.type == MemberType.SYMLINK:
            if member.link_target is None:
                raise ValueError("Link target is required for symlinks.")
            # Store the link target as the content of the file for symlinks.
            self._zipfile.writestr(zip_info, member.link_target.encode("utf-8"))
            return None
        else:
            # For other types, or if direct stream writing isn't applicable
            # e.g. if it's a type that doesn't have content or is handled differently.
            # This might need specific handling based on the member type.
            # For now, not returning a stream means no direct writing to it.
            # Consider if specific types need writestr or similar.
            return None

    def close(self) -> None:
        for stream in self._open_streams:
            if hasattr(stream, 'closed') and not stream.closed:
                stream.close()
        self._open_streams.clear()

        if self._zipfile:
            self._zipfile.close()
            self._zipfile = None # type: ignore
