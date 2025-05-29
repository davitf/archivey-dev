import zipfile
import io
from datetime import datetime
from typing import IO, Any, Union, Optional, cast

from archivey.base_writer import ArchiveWriter
from archivey.types import ArchiveMember, MemberType # Assuming MemberType might be useful
from archivey.exceptions import ArchiveError # Assuming a generic ArchiveError exists

# Default compression, can be overridden in constructor or open method
DEFAULT_ZIP_COMPRESSION = zipfile.ZIP_DEFLATED

class ZipIOWrapper(io.BufferedIOBase):
    """A wrapper for ZipFile.open() to provide a file-like object that can be closed."""
    def __init__(self, zip_file: zipfile.ZipFile, name: str, mode: str = 'w', **kwargs: Any):
        super().__init__()
        self._zip_file = zip_file
        self._name = name
        self._mode = mode
        self._kwargs = kwargs
        self._io: Optional[IO[bytes]] = None
        self._ensure_open()

    def _ensure_open(self) -> IO[bytes]:
        if self._io is None:
            # The actual open call to zipfile happens here
            self._io = self._zip_file.open(self._name, self._mode, **self._kwargs)
        return self._io

    def write(self, b: bytes) -> int: # type: ignore[override]
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._ensure_open().write(b)

    def read(self, size: Optional[int] = -1) -> bytes: # type: ignore[override]
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._ensure_open().read(size if size is not None else -1) # Ensure -1 if None for readall

    def readable(self) -> bool:
        return self._mode == 'r' and not self.closed

    def writable(self) -> bool:
        return self._mode == 'w' and not self.closed

    def seekable(self) -> bool:
        # zipfile streams are generally not seekable
        if self._io:
            return self._io.seekable()
        return False

    def tell(self) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._ensure_open().tell()

    def close(self) -> None:
        if self._io and not self.closed:
            self._io.close()
            self._io = None
        super().close() # Sets self.closed = True

    @property
    def closed(self) -> bool:
        # Consistently use super().closed if BufferedIOBase provides it,
        # otherwise manage _closed attribute.
        return super().closed


class ZipWriter(ArchiveWriter):
    """Writer for ZIP archives."""

    def __init__(
        self,
        archive: Union[str, IO[bytes]],
        mode: str = "w",
        *,
        compression: int = DEFAULT_ZIP_COMPRESSION,
        compresslevel: Optional[int] = None,
        encoding: Optional[str] = None,
        **kwargs: Any,
    ):
        """Initialize the ZipWriter.

        Args:
            archive: Path to the ZIP file or a file-like object.
            mode: Mode to open the archive ('w', 'x', 'a').
            compression: Default compression method for new members (e.g., zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED).
            compresslevel: Compression level (if applicable to the compression method).
            encoding: Default encoding for filenames. UTF-8 is recommended by ZIP specification.
            **kwargs: Additional arguments for zipfile.ZipFile.
        """
        super().__init__(archive, mode, encoding=encoding, **kwargs)
        self._compression = compression
        self._compresslevel = compresslevel
        self._zipfile_kwargs = kwargs

        # Defer ZipFile creation until first use or explicit open to better handle modes
        self._zipfile: Optional[zipfile.ZipFile] = None


    def _ensure_zipfile_open(self) -> zipfile.ZipFile:
        if self._zipfile is None:
            try:
                # Pass encoding to zipfile.ZipFile if it supports it (Python 3.11+)
                # For now, filenames are expected to be handled by the user or default to cp437/utf-8 by zipfile
                self._zipfile = zipfile.ZipFile(
                    self._archive,
                    self._mode,
                    compression=self._compression,
                    compresslevel=self._compresslevel,
                    **self._zipfile_kwargs
                )
            except zipfile.BadZipFile as e:
                raise ArchiveError(f"Invalid ZIP archive or parameters: {e}") from e
            except Exception as e: # Catch other potential errors like file not found, permissions
                raise ArchiveError(f"Failed to open ZIP archive '{self._archive}': {e}") from e
        return self._zipfile

    def open(
        self,
        member_info: Union[str, ArchiveMember],
        mode: str = "w",
        *,
        pwd: Optional[bytes] = None,
        compress_type: Optional[int] = None,
        compresslevel: Optional[int] = None,
        # Add other zipfile.ZipInfo or ZipFile.open parameters as needed
    ) -> IO[bytes]:
        """Open a member for writing within the ZIP archive.

        Args:
            member_info: Name of the member (str) or an ArchiveMember instance.
                         If ArchiveMember, its 'filename' and 'mtime' are used.
            mode: Must be 'w'.
            pwd: Password for encrypting the member.
            compress_type: Specific compression method for this member. Overrides archive default.
            compresslevel: Specific compression level for this member.

        Returns:
            A file-like object for writing to the member.
        """
        super().open(member_info, mode) # Basic checks from base class

        zf = self._ensure_zipfile_open()
        name: str
        date_time: Optional[tuple[int, int, int, int, int, int]] = None

        if isinstance(member_info, ArchiveMember):
            name = member_info.filename
            if member_info.mtime:
                # zipfile expects a 6-tuple for date_time
                if isinstance(member_info.mtime, (int, float)):
                     dt_obj = datetime.fromtimestamp(member_info.mtime)
                elif isinstance(member_info.mtime, datetime):
                     dt_obj = member_info.mtime
                else: # E.g. string, try to parse, or ignore
                    dt_obj = datetime.now() # Fallback, or raise error

                date_time = (
                    dt_obj.year, dt_obj.month, dt_obj.day,
                    dt_obj.hour, dt_obj.minute, dt_obj.second
                )
        else: # str
            name = member_info
            # Could default date_time to now() if not ArchiveMember
            # date_time = datetime.now().timetuple()[:6]


        # Prepare ZipInfo if more control is needed (e.g. permissions, comments)
        # For now, directly use zf.open() which creates ZipInfo internally
        # zinfo = zipfile.ZipInfo(name, date_time=date_time)
        # zinfo.compress_type = compress_type if compress_type is not None else self._compression
        # if isinstance(member_info, ArchiveMember) and member_info.permissions is not None:
        #    zinfo.external_attr = (member_info.permissions & 0xFFFF) << 16 # Basic Unix permissions

        open_kwargs = {}
        if pwd:
            open_kwargs['pwd'] = pwd
        if compress_type is not None:
            open_kwargs['force_zip64'] = True # Recommended if sizes are unknown or large
            # For zipfile.open(), we pass compress_type via ZipInfo or rely on archive default
            # The 'compress_type' param to ZipFile.open() is not standard.
            # Instead, one creates a ZipInfo object, sets its compress_type, and passes it.
            # However, for simplicity, we might need to create ZipInfo explicitly here.
            # Let's assume for now that the archive-level compression is used,
            # or we require creating ZipInfo for per-file compression type.

        # If per-file compression is desired beyond the archive's default,
        # we must create a ZipInfo object.
        if compress_type is not None or date_time is not None:
            zinfo = zipfile.ZipInfo(name, date_time if date_time else datetime.now().timetuple()[:6])
            zinfo.compress_type = compress_type if compress_type is not None else self._compression
            if compresslevel is not None and hasattr(zinfo, 'compresslevel'): # Python 3.7+ for ZipInfo
                 # This attribute is not directly on ZipInfo in older Pythons for open()
                 # It's usually passed to ZipFile constructor.
                 # For per-file compresslevel with zf.open(), it's tricky.
                 # The `compresslevel` parameter is for the `ZipFile` constructor.
                 # `ZipFile.open` does not take `compresslevel`.
                 # To set per-file `compress_type` and `compresslevel` correctly with `zf.open`,
                 # you often pass a fully configured `ZipInfo` object.
                 # However, `zf.open` also takes `compress_type` directly in some Python versions,
                 # but not `compresslevel`.
                 # For broader compatibility and control, writing via `zf.writestr(zinfo, data, compresslevel=...)`
                 # or `zf.write(filename, arcname, compress_type, compresslevel)` is more direct for compresslevel.
                 # Since we return a stream, we must use `zf.open(zinfo, ...)`
                 pass # `compresslevel` for `zf.open` is not directly supported.
                      # It's part of the `ZipFile` constructor or `ZipFile.write/writestr`.
                      # This implies that for streaming `open`, per-file `compresslevel` is hard.

            # This will use the zinfo's compress_type.
            # pwd needs to be passed to open method.
            return cast(IO[bytes], ZipIOWrapper(zf, zinfo, mode=mode, pwd=pwd, **open_kwargs)) # type: ignore
        else:
            # Simpler path if no custom date_time or compress_type for this specific file.
            # Relies on archive-level compression settings.
            # pwd can be passed directly to zf.open()
            return cast(IO[bytes], ZipIOWrapper(zf, name, mode=mode, pwd=pwd, **open_kwargs))


    def close(self) -> None:
        """Close the ZIP archive, finalizing its contents."""
        if not self._closed:
            if self._zipfile:
                self._zipfile.close()
                self_zipfile = None # type: ignore
            super().close() # Sets self._closed = True

    # writestr and write are inherited from ArchiveWriter
    # They will use the above `open` method.
