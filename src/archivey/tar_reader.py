import gzip
import io
import lzma
import stat
import tarfile
from datetime import datetime
from typing import Iterator, List, Union

from archivey.base_reader import (
    ArchiveInfo,
    ArchiveMember,
    ArchiveReader,
)
from archivey.exceptions import ArchiveCorruptedError, ArchiveMemberNotFoundError
from archivey.types import ArchiveFormat, MemberType


class TarReader(ArchiveReader):
    """Reader for TAR archives and compressed TAR archives (e.g., .tar.gz, .tar.bz2).

    This class uses the standard library's `tarfile` module to interact with
    TAR archives. It supports reading plain TAR files as well as those compressed
    with GZIP, BZIP2, LZMA (XZ), Zstandard, or LZ4 (if the respective Python
    bindings are available for Zstd/LZ4 and supported by `tarfile`).

    Args:
        archive_path: Path to the TAR archive file.
        format: The `ArchiveFormat` of the file (e.g., TAR, TAR_GZ).
        pwd: Password for decryption. Not supported for TAR archives;
             a `ValueError` will be raised if provided.

    Attributes:
        archive_path (str): Path to the archive file.
        _archive (Optional[tarfile.TarFile]): The underlying `tarfile.TarFile` object.
        _members (Optional[List[ArchiveMember]]): Cached list of archive members.
        _format_info (Optional[ArchiveInfo]): Cached archive information.

    Raises:
        ValueError: If `pwd` is provided.
        ArchiveCorruptedError: If the archive is invalid, corrupted, or if
                               a compressed TAR stream is malformed.
        ImportError: If a compression library required for a specific format
                     (e.g., `zstandard` for `.tar.zst`) is not found by `tarfile`.
    """

    def __init__(
        self,
        archive_path: str,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None, # Kept for API consistency
    ):
        """Initializes TarReader.

        Args:
            archive_path: Path to the TAR archive.
            format: The `ArchiveFormat` enum, indicating if it's plain TAR
                    or a compressed variant like TAR_GZ.
            pwd: Password (not supported for TAR, will raise ValueError if set).
        """
        super().__init__(archive_path, format, pwd=pwd)
        if pwd is not None: # pragma: no cover
            raise ValueError("TAR format does not support password protection.")

        # self.archive_path is set by super()
        self._members: List[ArchiveMember] | None = None
        self._format_info: ArchiveInfo | None = None

        # Determine tarfile mode (e.g., "r", "r:gz", "r:bz2")
        mode_map = {
            ArchiveFormat.TAR: "r",
            ArchiveFormat.TAR_GZ: "r:gz",
            ArchiveFormat.TAR_BZ2: "r:bz2",
            ArchiveFormat.TAR_XZ: "r:xz",
            ArchiveFormat.TAR_ZSTD: "r:zst", # Requires Python 3.9+ tarfile and zstandard
            ArchiveFormat.TAR_LZ4: "r:lz4",  # Requires Python 3.9+ tarfile and lz4
        }
        tar_mode = mode_map.get(format)

        if tar_mode is None: # pragma: no cover
            # Should not happen if format detection is correct and comprehensive
            raise ArchiveFormatError(f"Unsupported TAR format for tarfile mode: {format.value}")

        try:
            # The `tarfile.open` type hint might be overly strict in some contexts.
            # We use tar_mode which is one of the valid string modes.
            self._archive = tarfile.open(self.archive_path, mode=tar_mode) # type: ignore
        except tarfile.ReadError as e:
            # This can be due to file corruption, not a tar file, or sometimes
            # wrong compression format (e.g. trying to read a .gz as plain .tar)
            raise ArchiveCorruptedError(f"Invalid or corrupted TAR archive '{self.archive_path}': {e}") from e
        except (gzip.BadGzipFile, bz2.BZ2Error, lzma.LZMAError) as e: # bz2.BZ2Error for completeness
            # These occur if the compression layer is corrupted for compressed TARs
            raise ArchiveCorruptedError(
                f"Corrupted compressed TAR archive '{self.archive_path}' (format: {format.value}): {e}"
            ) from e
        except FileNotFoundError: # pragma: no cover
            raise
        except ImportError as e: # For missing zstandard/lz4
            raise ImportError(
                f"Missing compression library for format {format.value} (e.g., zstandard, lz4): {e}"
            ) from e
        except Exception as e: # Catch other tarfile.open issues
            raise ArchiveError(f"Error opening TAR archive '{self.archive_path}': {e}") from e


    def close(self) -> None:
        """Closes the TAR archive and releases resources.

        Safe to call multiple times.
        """
        if hasattr(self, "_archive") and self._archive:
            try:
                self._archive.close()
            except Exception as e: # pragma: no cover
                logger.warning(f"Error closing TarFile for {self.archive_path}: {e}")
            self._archive = None
        self._members = None
        self._format_info = None

    def get_members(self) -> List[ArchiveMember]:
        """Retrieves a list of all members in the TAR archive.

        Member information is cached after the first call.

        Returns:
            A list of `ArchiveMember` objects.

        Raises:
            ValueError: If the archive is closed.
            ArchiveCorruptedError: If there's an error reading member info from the TAR file.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []
            try:
                tar_infos = self._archive.getmembers()
            except Exception as e: # pragma: no cover
                # tarfile can raise various errors here if archive is bad
                raise ArchiveCorruptedError(f"Failed to read member information from TAR archive '{self.archive_path}': {e}") from e

            for info in tar_infos:
                # Determine overall compression method of the TAR archive itself
                outer_compression_method = None
                current_format = self.get_format()
                if current_format != ArchiveFormat.TAR:
                    # Map TAR format to its compression type (GZIP, BZIP2, etc.)
                    compression_map = {
                        ArchiveFormat.TAR_GZ: ArchiveFormat.GZIP.value,
                        ArchiveFormat.TAR_BZ2: ArchiveFormat.BZIP2.value,
                        ArchiveFormat.TAR_XZ: ArchiveFormat.XZ.value,
                        ArchiveFormat.TAR_ZSTD: ArchiveFormat.ZSTD.value,
                        ArchiveFormat.TAR_LZ4: ArchiveFormat.LZ4.value,
                    }
                    outer_compression_method = compression_map.get(current_format)

                # Ensure directory names end with a slash for consistency
                filename = info.name
                if info.isdir() and not filename.endswith("/"): # pragma: no cover
                    filename += "/"

                member_type = MemberType.OTHER # Default
                if info.isfile():
                    member_type = MemberType.FILE
                elif info.isdir():
                    member_type = MemberType.DIR
                elif info.issym() or info.islnk(): # islnk for hard links, issym for symbolic
                    member_type = MemberType.LINK
                # tarfile also has isfifo, ischr, isblk, isdev for special files

                member = ArchiveMember(
                    filename=filename,
                    size=info.size,
                    mtime=datetime.fromtimestamp(info.mtime) if info.mtime else None,
                    type=member_type,
                    permissions=stat.S_IMODE(info.mode) if hasattr(info, "mode") else None,
                    link_target=info.linkname if info.issym() or info.islnk() else None,
                    crc32=None,  # TAR format itself does not store CRCs per member
                    compression_method=None, # Individual files within TAR are not separately compressed by TAR spec
                                             # `outer_compression_method` applies to the whole archive stream
                    extra={
                        "tar_type": info.type, # tarfile specific type flag (e.g. REGTYPE, DIRTYPE)
                        "mode": info.mode,
                        "uid": info.uid,
                        "gid": info.gid,
                        "uname": info.uname,
                        "gname": info.gname,
                        "pax_headers": info.pax_headers, # PAX headers if present
                    },
                    raw_info=info, # Store the tarfile.TarInfo object
                )
                self._members.append(member)
        return self._members

    def open(
        self, member: Union[str, ArchiveMember], *, pwd: bytes | str | None = None
    ) -> io.IOBase:
        """Opens a member within the TAR archive for reading.

        Args:
            member: Either the name of the member (str) or an `ArchiveMember`
                    object.
            pwd: Password for decryption. Not supported for TAR archives;
                 a `ValueError` will be raised if provided.

        Returns:
            A file-like object (binary I/O stream) for reading the member's content.

        Raises:
            ValueError: If the archive is closed or `pwd` is provided.
            ArchiveMemberNotFoundError: If the specified member is not found.
            ArchiveCorruptedError: If there's an error extracting the member.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")
        if pwd is not None: # pragma: no cover
            raise ValueError("TAR format does not support password protection.")

        tar_info_obj: tarfile.TarInfo
        member_name_to_find: str

        if isinstance(member, str):
            member_name_to_find = member
        elif isinstance(member, ArchiveMember):
            member_name_to_find = member.filename
        else: # pragma: no cover
            raise TypeError("Member must be a string name or an ArchiveMember object.")

        try:
            # Use raw_info if available and it's a TarInfo object, otherwise lookup by name
            if isinstance(member, ArchiveMember) and isinstance(member.raw_info, tarfile.TarInfo):
                tar_info_obj = member.raw_info
            else:
                tar_info_obj = self._archive.getmember(member_name_to_find)
        except KeyError:
            raise ArchiveMemberNotFoundError(
                f"Member '{member_name_to_find}' not found in archive '{self.archive_path}'"
            ) from None # No need to chain the KeyError

        try:
            # extractfile returns a BufferedReader-like object
            extracted_file = self._archive.extractfile(tar_info_obj)
            if extracted_file is None: # Should not happen if getmember succeeded and it's a file/link
                 # This case usually applies to non-regular files like directories for which extractfile returns None
                raise ArchiveError(f"Cannot extract member '{tar_info_obj.name}' - it might be a directory or special file type not supporting direct extraction to a file stream.")
            return extracted_file
        except (tarfile.ReadError, KeyError, EOFError) as e: # KeyError if member somehow disappeared or internal issue
            raise ArchiveCorruptedError(f"Error reading member '{tar_info_obj.name}' from '{self.archive_path}': {e}") from e
        except Exception as e: # Catch other unexpected errors from extractfile
            raise ArchiveError(f"Unexpected error extracting member '{tar_info_obj.name}': {e}") from e


    def iter_members(self) -> Iterator[ArchiveMember]:
        """Returns an iterator over `ArchiveMember` objects in the archive.

        Yields:
            `ArchiveMember` objects.
        """
        return iter(self.get_members())

    def get_archive_info(self) -> ArchiveInfo:
        """Retrieves detailed information about the TAR archive.

        Information is cached after the first call.

        Returns:
            An `ArchiveInfo` object.

        Raises:
            ValueError: If the archive is closed.
        """
        if self._archive is None: # pragma: no cover
            raise ValueError("Archive is closed")

        if self._format_info is None:
            current_format = self.get_format()
            # Compressed TAR archives are "solid" because the entire TAR archive
            # is a single compressed stream.
            is_solid = current_format != ArchiveFormat.TAR

            # Try to get tarfile-specific format (e.g., USTAR, PAX) and encoding
            tarfile_format_version = None
            if hasattr(self._archive, "format"): # POSIX, GNU, USTAR
                tarfile_format_version = str(self._archive.format) # tarfile.format is an int enum
            
            encoding = None
            if hasattr(self._archive, "encoding"):
                encoding = self._archive.encoding


            self._format_info = ArchiveInfo(
                format=current_format,
                is_solid=is_solid,
                comment=None, # TAR format does not have a global archive comment standard
                extra={
                    "tarfile_type": tarfile_format_version, # e.g. USTAR_FORMAT, GNU_FORMAT
                    "encoding": encoding, # Encoding for filenames/metadata
                },
            )
        return self._format_info

    def is_solid(self) -> bool: # pragma: no cover
        """Checks if the archive is effectively solid.

        For TAR archives, "solid" means the entire TAR structure is compressed
        as a single stream (e.g., .tar.gz). Plain .tar files are not solid
        in this context.

        Returns:
            True if the TAR archive is compressed (e.g., .tar.gz, .tar.bz2),
            False for plain .tar archives.
        """
        # This method might be redundant if get_archive_info().is_solid is used.
        # Kept for potential direct use or clarity.
        current_format = self.get_format()
        return current_format != ArchiveFormat.TAR
