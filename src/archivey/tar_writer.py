import tarfile
import io
from datetime import datetime
import time

from archivey.base_writer import ArchiveWriter
from archivey.types import ArchiveMember, MemberType, ArchiveFormat, TAR_FORMAT_TO_COMPRESSION_FORMAT

# Helper to get the tar mode string
def get_tar_mode(archive_format: ArchiveFormat) -> str:
    if archive_format == ArchiveFormat.TAR:
        return "w"
    elif archive_format == ArchiveFormat.TAR_GZ:
        return "w:gz"
    elif archive_format == ArchiveFormat.TAR_BZ2:
        return "w:bz2"
    elif archive_format == ArchiveFormat.TAR_XZ:
        return "w:xz"
    # For ZSTD and LZ4, tarfile might not support them directly with "w:zst" or "w:lz4"
    # depending on the Python version and linked libraries.
    # However, modern tarfile versions are increasingly supporting these.
    elif archive_format == ArchiveFormat.TAR_ZSTD:
        return "w:zst" # Requires tarfile support for zstd
    elif archive_format == ArchiveFormat.TAR_LZ4:
        return "w:lz4" # Requires tarfile support for lz4
    else:
        # Fallback to plain tar if format is unknown or not a tar variant
        raise ValueError(f"Unsupported TAR format for writing: {archive_format}")

class TarArchiveWriter(ArchiveWriter):
    def __init__(self, archive_path: str, archive_format: ArchiveFormat):
        super().__init__(archive_path)
        self.archive_format = archive_format
        mode = get_tar_mode(self.archive_format)
        # Placeholder for potential external compression stream handling
        self._comp_stream = None
        self._tarfile = tarfile.open(self.archive_path, mode)
        # No _open_streams_info needed if streams handle their own writing on close

    def add_member(self, member: ArchiveMember) -> io.IO[bytes] | None:
        tarinfo = tarfile.TarInfo(name=member.filename)

        mtime = member.mtime or datetime.now()
        # tarinfo.mtime expects a numeric timestamp
        tarinfo.mtime = int(time.mktime(mtime.timetuple()))


        if member.type == MemberType.FILE:
            tarinfo.type = tarfile.REGTYPE
            tarinfo.mode = member.mode or 0o644
        elif member.type == MemberType.DIR:
            tarinfo.type = tarfile.DIRTYPE
            tarinfo.mode = member.mode or 0o755
            tarinfo.name = member.filename.rstrip("/") # Tar expects dir names without trailing slash
        elif member.type == MemberType.SYMLINK:
            tarinfo.type = tarfile.SYMTYPE
            tarinfo.mode = member.mode or 0o777 # Permissions for symlinks themselves
            if member.link_target is None:
                raise ValueError("Link target is required for symlinks.")
            tarinfo.linkname = member.link_target
        elif member.type == MemberType.HARDLINK:
            tarinfo.type = tarfile.LNKTYPE # Represents a hard link
            tarinfo.mode = member.mode or 0o644
            if member.link_target is None:
                # In tar, a hard link is a link to a file already in the archive.
                # The link_target should be the path of the original file in the archive.
                raise ValueError("Link target (path of existing archive member) is required for hardlinks.")
            tarinfo.linkname = member.link_target
        else:
            # Default to regular file for OTHER types, may need adjustment
            tarinfo.type = tarfile.REGTYPE
            tarinfo.mode = 0o644

        if member.type == MemberType.FILE:
            class TarFileStream(io.BytesIO):
                def __init__(self, tar_file_obj: tarfile.TarFile, tar_info_obj: tarfile.TarInfo):
                    super().__init__()
                    self._tar_file_obj = tar_file_obj
                    self._tar_info_obj = tar_info_obj
                    self._closed = False

                def close(self) -> None:
                    if not self._closed:
                        self.seek(0)
                        self._tar_info_obj.size = len(self.getvalue())
                        self.seek(0)
                        self._tar_file_obj.addfile(self._tar_info_obj, self)
                        self._closed = True
                    super().close()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    self.close()

            # The stream is returned; the ArchiveWriter.open() method should ensure it's context-managed
            # so that its close() method (which calls addfile) is invoked.
            return TarFileStream(self._tarfile, tarinfo)
        else:
            # For DIR, SYMLINK, HARDLINK, add them directly as they don't have separate file content stream
            self._tarfile.addfile(tarinfo)
            return None

    def close(self) -> None:
        if self._tarfile:
            self._tarfile.close()
            self._tarfile = None

        if self._comp_stream: # If an external compression stream was used
            self._comp_stream.close()
            self._comp_stream = None
