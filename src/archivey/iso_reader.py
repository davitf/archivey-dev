import io
import os
from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Iterator, List, Optional

if TYPE_CHECKING:
    import pycdlib
else:
    try:
        import pycdlib
    except ImportError:
        pycdlib = None  # type: ignore[assignment]

from archivey.base_reader import ArchiveReader, PathType
from archivey.exceptions import (
    ArchiveyError,
    CorruptedArchiveError,
)
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType


class IsoReader(ArchiveReader):
    """
    Reads ISO 9660 archives using the pycdlib library.
    """

    format = ArchiveFormat.ISO
    magic = b"CD001"  # Standard ISO 9660 identifier
    magic_offset = 0x8001

    def __init__(
        self,
        archive: PathType,
        password: Optional[str | bytes] = None,
        encoding: Optional[
            str
        ] = None,  # ISO 9660 typically uses its own encoding system.
    ):
        super().__init__(archive, password=password, encoding=encoding)
        self.iso: Optional[pycdlib.PyCdlib] = None
        self.archive_path_obj: Optional[Path] = None

        if isinstance(archive, (str, bytes, os.PathLike)):
            self.archive_path_obj = Path(archive)
            try:
                self.iso = pycdlib.PyCdlib()
                self.iso.open(archive)
            except Exception as e:  # pycdlib can raise various errors
                raise CorruptedArchiveError(f"Failed to open ISO file: {e}") from e
        elif hasattr(archive, "read") and hasattr(archive, "seek"):
            # pycdlib requires a filepath, it cannot work directly with streams.
            # We could save the stream to a temporary file, but that adds complexity.
            # For now, let's state that IsoReader expects a path.
            raise ArchiveyError("IsoReader requires a file path, not a stream.")
        else:
            raise TypeError("archive must be a path-like object or a file-like object")

        if password:
            # ISO 9660 does not natively support encryption in a way pycdlib handles.
            # If password-protected ISOs exist, they are likely handled by proprietary means.
            # For standard ISOs, passwords are not applicable.
            # We could raise EncryptedArchiveError if a password is provided,
            # or simply ignore it. Ignoring seems more user-friendly for now.
            pass  # Ignoring password as ISOs are not typically encrypted

    def _is_pycdlib_dir(
        self, record: pycdlib.backends.pycdlibstructures.DirectoryRecord
    ) -> bool:
        """Checks if a pycdlib DirectoryRecord is a directory."""
        return record.is_dir()

    def _is_pycdlib_symlink(
        self, record: pycdlib.backends.pycdlibstructures.DirectoryRecord
    ) -> bool:
        """Checks if a pycdlib DirectoryRecord is a symbolic link (using Rock Ridge)."""
        # pycdlib doesn't directly expose a simple is_symlink().
        # We need to check for Rock Ridge 'SL' extension.
        if record.has_rock_ridge():
            rr_extensions = record.rock_ridge_extensions()
            for ext in rr_extensions:
                if isinstance(ext, pycdlib.rockridge.SLExtension):
                    return True
        return False

    def _convert_direntry_to_member(
        self, path: str, record: pycdlib.backends.pycdlibstructures.DirectoryRecord
    ) -> ArchiveMember:
        """Converts a pycdlib DirectoryRecord to an ArchiveMember."""
        member_type = MemberType.OTHER
        if self._is_pycdlib_dir(record):
            member_type = MemberType.DIR
        elif self._is_pycdlib_symlink(record):  # Check for symlink before regular file
            member_type = MemberType.LINK
        elif record.is_file():
            member_type = MemberType.FILE

        mtime_dt: Optional[datetime] = None
        if record.date_time:
            try:
                # pycdlib's datetime is timezone-naive. Assume UTC or local as per ISO standard context.
                # Python's datetime.fromtimestamp will use local timezone.
                # For consistency, one might want to specify UTC if the ISO timestamps are UTC.
                # However, pycdlib already gives a datetime object.
                mtime_dt = record.date_time
            except ValueError:  # Should not happen if record.date_time is valid
                mtime_dt = None

        link_target = None
        if member_type == MemberType.LINK and record.has_rock_ridge():
            rr_extensions = record.rock_ridge_extensions()
            for ext in rr_extensions:
                if isinstance(ext, pycdlib.rockridge.SLExtension):
                    # SLExtension stores components of the link target.
                    # We need to join them. pycdlib doesn't seem to have a direct
                    # method to get the full link target path string easily.
                    # This is a simplified assumption; complex SL links might exist.
                    link_target = "/".join(
                        c.data.decode("utf-8", "replace") for c in ext.sl_components
                    )
                    break

        # pycdlib does not directly expose CRC32 or compression method for individual files
        # as ISO9660 is not a compressed format in the same way as zip/rar.
        return ArchiveMember(
            filename=path,
            file_size=record.data_length
            if record.is_file() or self._is_pycdlib_symlink(record)
            else 0,  # Symlinks also have a size for their target path
            compress_size=record.data_length
            if record.is_file() or self._is_pycdlib_symlink(record)
            else 0,  # No compression in ISO
            mtime=mtime_dt,
            type=member_type,
            mode=record.posix_mode()
            if record.has_rock_ridge() and hasattr(record, "posix_mode")
            else None,
            link_target=link_target,
            raw_info=record,
        )

    def _walk_iso(
        self, current_path: str = "/", current_iso_path: str = "/"
    ) -> Iterator[ArchiveMember]:
        """Helper to recursively walk the ISO directory structure."""
        if not self.iso:
            raise ArchiveyError("ISO not opened")

        try:
            for child_name_bytes, record in self.iso.list_dir(
                iso_path=current_iso_path
            ):
                child_name = child_name_bytes.decode(
                    self.iso.joliet_output_encoding
                    if self.iso.has_joliet()
                    else self.iso.iso_output_encoding,
                    "replace",
                )

                # Skip '.' and '..' entries
                if child_name == "." or child_name == "..":
                    continue

                # Construct the full path for the member
                # Ensure no double slashes if current_path is '/'
                member_path = os.path.join(current_path.strip("/"), child_name).replace(
                    "\\", "/"
                )
                if (
                    not member_path.startswith("/") and current_path == "/"
                ):  # Handle root children
                    member_path = f"/{member_path}"
                if current_path != "/" and not member_path.startswith(
                    current_path
                ):  # ensure children of /foo are /foo/bar
                    member_path = f"{current_path.rstrip('/')}/{child_name}".replace(
                        "\\", "/"
                    )

                # Clean path: remove leading slash for internal consistency if desired,
                # but usually archive members are listed with full paths from root.
                # For now, keep leading slash.
                # member_path = member_path.lstrip('/') # Example if no leading slash is wanted

                yield self._convert_direntry_to_member(member_path, record)

                if self._is_pycdlib_dir(record):
                    # Construct the iso_path for pycdlib (needs trailing slash for dirs)
                    next_iso_path = (
                        os.path.join(current_iso_path, child_name).replace("\\", "/")
                        + "/"
                    )
                    yield from self._walk_iso(member_path, next_iso_path)
        except Exception as e:
            raise CorruptedArchiveError(
                f"Error walking ISO directory {current_iso_path}: {e}"
            ) from e

    def get_members(self) -> List[ArchiveMember]:
        if not self.iso:
            raise ArchiveyError("ISO not opened")
        return list(self.iter_members())

    def iter_members(self) -> Iterator[ArchiveMember]:
        if not self.iso:
            raise ArchiveyError("ISO not opened")

        # Yield the root directory itself, as it's a common convention
        # pycdlib doesn't explicitly list the root dir record in list_dir('/')
        # We can try to get its record specifically.
        try:
            root_record = self.iso.get_record(iso_path="/.")
            if root_record:
                yield self._convert_direntry_to_member("/", root_record)
        except Exception:
            # This might fail if the ISO is very strange or empty.
            # Log or handle as appropriate. For now, proceed to walk.
            pass

        yield from self._walk_iso(current_path="/", current_iso_path="/")

    def open(
        self, member: ArchiveMember | str, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        if not self.iso:
            raise ArchiveyError("ISO not opened")

        member_name: str
        if isinstance(member, ArchiveMember):
            member_name = member.filename
        elif isinstance(member, str):
            member_name = member
        else:
            raise TypeError("member must be an ArchiveMember or a string path")

        # pycdlib needs the exact path as it's stored in the ISO
        # Our member.filename should correspond to this.
        # Ensure it's in the format pycdlib expects (e.g. /FOO/BAR.TXT;1)
        # The _walk_iso method should produce these correct paths.
        # If the member name includes a version (';1'), pycdlib handles it.
        # If it doesn't, pycdlib usually defaults to the first version.

        # Convert our potentially clean path to the ISO path format pycdlib expects
        # This means it must start with '/' and use '/' as separator.
        iso_path = member_name
        if not iso_path.startswith("/"):
            iso_path = "/" + iso_path

        # Ensure the path uses '/' as separator, which pycdlib expects.
        iso_path = iso_path.replace("\\", "/")

        try:
            # Check if it's a directory, pycdlib can't extract directories as a stream
            record = self.iso.get_record(iso_path=iso_path)
            if record and self._is_pycdlib_dir(record):
                raise IsADirectoryError(
                    f"Cannot open directory '{member_name}' as a file stream."
                )

            file_data = self.iso.get_file_from_iso(iso_path=iso_path)
            return io.BytesIO(file_data)
        except pycdlib.backends.pycdlibexceptions.PyCdlibInvalidInput:
            # This exception is often raised for "file not found"
            raise FileNotFoundError(f"File not found in ISO: {member_name}") from None
        except Exception as e:
            raise ArchiveyError(
                f"Failed to open member '{member_name}' from ISO: {e}"
            ) from e

    def get_archive_info(self) -> ArchiveInfo:
        if not self.iso:
            raise ArchiveyError("ISO not opened")

        # Basic info
        info = ArchiveInfo(format=self.format.value)

        # Try to get volume label
        # pycdlib might have this on the primary volume descriptor
        try:
            pvd = self.iso.pvd
            if pvd:
                info.comment = pvd.volume_identifier.decode(
                    self.iso.iso_output_encoding, "replace"
                ).strip()
                # Other PVD fields like system_identifier, volume_set_identifier,
                # publisher_identifier, data_preparer_identifier, application_identifier
                # could be added to extra_info if needed.
                extra = {}
                if pvd.system_identifier:
                    extra["system_identifier"] = pvd.system_identifier.decode(
                        self.iso.iso_output_encoding, "replace"
                    ).strip()
                if pvd.publisher_identifier:
                    extra["publisher_identifier"] = pvd.publisher_identifier.decode(
                        self.iso.iso_output_encoding, "replace"
                    ).strip()
                if pvd.application_identifier:
                    extra["application_identifier"] = pvd.application_identifier.decode(
                        self.iso.iso_output_encoding, "replace"
                    ).strip()
                if extra:
                    info.extra = extra
        except Exception:
            # Silently ignore if PVD info is not available or fails to parse
            pass

        # ISOs are not inherently solid, encrypted (standardly), or versioned like RAR/7z
        info.is_solid = False
        # info.version could perhaps indicate ISO9660 version (e.g., "1"), Joliet, Rock Ridge presence
        # For now, keeping it simple.

        return info

    def close(self) -> None:
        if self.iso:
            try:
                self.iso.close()
            except Exception:
                # Log error or handle as needed, but don't let it prevent closing.
                # print(f"Error closing ISO: {e}")
                pass
            self.iso = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def check_format_by_signature(cls, path_or_file: PathType) -> bool:
        """
        Checks if the given file matches the ISO 9660 signature.
        """
        close_after = False
        f: IO[bytes]
        if isinstance(path_or_file, (str, bytes, os.PathLike)):
            try:
                f = open(path_or_file, "rb")
                close_after = True
            except FileNotFoundError:
                return False
        elif hasattr(path_or_file, "read") and hasattr(path_or_file, "seek"):
            f = path_or_file  # type: ignore
        else:
            return False  # Not a path or stream

        original_pos = -1
        if f.seekable():
            original_pos = f.tell()
            f.seek(cls.magic_offset)
        else:  # Non-seekable stream, cannot check at specific offset
            if (
                cls.magic_offset > 0
            ):  # If magic is not at the beginning for non-seekable stream
                return False

        try:
            sig = f.read(len(cls.magic))
            return sig == cls.magic
        except Exception:
            return False
        finally:
            if f.seekable() and original_pos != -1:
                f.seek(original_pos)
            if close_after:
                f.close()

    @classmethod
    def check_format_by_path(cls, path: PathType) -> bool:
        """
        Checks if the given path has a common ISO extension.
        """
        if isinstance(path, (str, bytes, os.PathLike)):
            return Path(path).suffix.lower() == ".iso"
        return False

    @classmethod
    def get_extra_extensions(cls) -> list[str]:
        return [".iso"]
