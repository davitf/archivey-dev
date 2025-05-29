import tarfile
import io
import os
from datetime import datetime
from typing import IO, Any, Union, Optional, cast

from archivey.base_writer import ArchiveWriter
from archivey.types import ArchiveMember, MemberType # Assuming MemberType might be useful
from archivey.exceptions import ArchiveError # Assuming a generic ArchiveError exists
from archivey.formats import ArchiveFormat # For determining compression from format


class TarIOWrapper(io.BufferedIOBase):
    """A wrapper for tarfile.extractfile-like stream, but for adding members."""
    def __init__(self, tar_file: tarfile.TarFile, tar_info: tarfile.TarInfo, data_stream: io.BytesIO):
        super().__init__()
        self._tar_file = tar_file
        self._tar_info = tar_info
        self._data_stream = data_stream # This stream holds the data to be added
        self._closed = False

    def write(self, b: bytes) -> int: # type: ignore[override]
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._data_stream.write(b)

    def readable(self) -> bool:
        return False # Not readable in this context

    def writable(self) -> bool:
        return not self.closed

    def seekable(self) -> bool:
        return self._data_stream.seekable()

    def tell(self) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._data_stream.tell()

    def close(self) -> None:
        if not self.closed:
            self._closed = True
            # The actual adding to tarfile happens here, using the collected data
            self._data_stream.seek(0)
            self._tar_info.size = self._data_stream.getbuffer().nbytes
            self._tar_file.addfile(self._tar_info, self._data_stream)
            self._data_stream.close() # Close the BytesIO stream
        # Do not call super().close() if it sets self.closed because we manage it

    @property
    def closed(self) -> bool:
        return self._closed


class TarWriter(ArchiveWriter):
    """Writer for TAR archives (including compressed variants like .tar.gz)."""

    def __init__(
        self,
        archive: Union[str, IO[bytes]],
        mode: str = "w", # e.g., "w", "w:gz", "w:bz2", "x:xz", etc. or use format
        *,
        format: Optional[ArchiveFormat] = None, # To explicitly set tar variant
        encoding: Optional[str] = "utf-8", # tarfile default
        errors: Optional[str] = "strict",   # tarfile default
        **kwargs: Any,
    ):
        """Initialize the TarWriter.

        Args:
            archive: Path to the TAR file or a file-like object.
            mode: Mode to open the archive. Examples: "w" (uncompressed),
                  "w:gz" (gzipped), "w:bz2" (bzipped2), "w:xz" (lzma/xz).
                  Can also be "x" for exclusive creation variants.
                  If `format` is provided, `mode` might be derived or validated against it.
            format: The specific TAR format (e.g., TAR_GZ, TAR_BZ2). If provided,
                    it can determine the compression mode.
            encoding: Encoding for member names.
            errors: Error handling for encoding/decoding member names.
            **kwargs: Additional arguments for tarfile.open().
        """
        # Determine tarfile mode from format if mode is simple "w" or "x"
        derived_mode = mode
        if format:
            if format == ArchiveFormat.TAR:
                derived_mode = mode.replace(":", "") # "w" or "x"
            elif format == ArchiveFormat.TAR_GZ:
                derived_mode = f"{mode.split(':')[0]}:gz"
            elif format == ArchiveFormat.TAR_BZ2:
                derived_mode = f"{mode.split(':')[0]}:bz2"
            elif format == ArchiveFormat.TAR_XZ:
                derived_mode = f"{mode.split(':')[0]}:xz"
            # Add other TAR formats (zstd, lz4) if tarfile/Python supports them directly
            # or if external libraries are used via tarfile filters.
        
        super().__init__(archive, derived_mode, encoding=encoding, **kwargs) # Pass derived_mode
        self._tarfile_kwargs = kwargs
        self._tarfile_kwargs.setdefault('encoding', encoding)
        self._tarfile_kwargs.setdefault('errors', errors)

        self._tarfile: Optional[tarfile.TarFile] = None


    def _ensure_tarfile_open(self) -> tarfile.TarFile:
        if self._tarfile is None:
            try:
                # self._mode already contains compression, e.g. "w:gz"
                self._tarfile = tarfile.open(
                    fileobj=self._archive if isinstance(self._archive, io.IOBase) else None,
                    name=self._archive if isinstance(self._archive, str) else None,
                    mode=self._mode,
                    **self._tarfile_kwargs
                )
            except tarfile.TarError as e:
                raise ArchiveError(f"Invalid TAR archive or parameters: {e}") from e
            except Exception as e:
                raise ArchiveError(f"Failed to open TAR archive '{self._archive}': {e}") from e
        return self._tarfile

    def open(
        self,
        member_info: Union[str, ArchiveMember, tarfile.TarInfo],
        mode: str = "w",
        # TAR specific options for adding members are usually part of TarInfo
    ) -> IO[bytes]:
        """Prepare a buffer for adding a new member to the TAR archive.

        The actual writing to the archive is deferred until the returned stream is closed.

        Args:
            member_info: Name of the member (str), an ArchiveMember instance,
                         or a pre-configured tarfile.TarInfo object.
            mode: Must be 'w'.

        Returns:
            A file-like object (buffer) to write the member's content.
            Closing this stream finalizes adding the member to the archive.
        """
        super().open(member_info, mode) # Basic checks

        tf = self._ensure_tarfile_open()
        
        tar_info: tarfile.TarInfo

        if isinstance(member_info, tarfile.TarInfo):
            tar_info = member_info
        else:
            name: str
            if isinstance(member_info, ArchiveMember):
                name = member_info.filename
                # Create TarInfo from ArchiveMember
                tar_info = tarfile.TarInfo(name=name)
                if member_info.mtime is not None:
                    if isinstance(member_info.mtime, (int, float)):
                        tar_info.mtime = int(member_info.mtime)
                    elif isinstance(member_info.mtime, datetime):
                        tar_info.mtime = int(member_info.mtime.timestamp())
                else:
                    tar_info.mtime = int(datetime.now().timestamp())

                if member_info.type == MemberType.DIR:
                    tar_info.type = tarfile.DIRTYPE
                    tar_info.size = 0 # Directories have 0 size
                elif member_info.type == MemberType.LINK:
                    tar_info.type = tarfile.SYMTYPE
                    tar_info.linkname = member_info.link_target or ""
                    tar_info.size = 0 # Symlinks have 0 size in tar header
                else: # FILE or unknown
                    tar_info.type = tarfile.REGTYPE
                
                if member_info.permissions is not None:
                    tar_info.mode = member_info.permissions
                # uid, gid, uname, gname could also be set here if available and desired
            else: # str
                name = member_info
                tar_info = tarfile.TarInfo(name=name)
                tar_info.mtime = int(datetime.now().timestamp())
                # Assume REGTYPE if just a string is given, could be configurable
                tar_info.type = tarfile.REGTYPE

        # For regular files, size will be set by TarIOWrapper.close()
        # For dirs and links, size is 0.
        if tar_info.type != tarfile.REGTYPE:
            tar_info.size = 0

        # Data for the member will be written to this BytesIO buffer first.
        # When TarIOWrapper.close() is called, this buffer's content is added
        # to the tarfile using tarfile.addfile().
        member_data_stream = io.BytesIO()
        return cast(IO[bytes], TarIOWrapper(tf, tar_info, member_data_stream))

    def close(self) -> None:
        """Close the TAR archive, finalizing its contents."""
        if not self._closed:
            if self._tarfile:
                self._tarfile.close()
                self._tarfile = None
            super().close() # Sets self._closed = True

    # writestr and write are inherited from ArchiveWriter
    # They will use the TarWriter.open() method.
