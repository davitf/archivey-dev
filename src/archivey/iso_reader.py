import io
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Callable, Iterator, List, Optional

from archivey.exceptions import ArchiveError, PackageNotInstalledError

if TYPE_CHECKING:
    import pycdlib
    from pycdlib.pycdlibexception import PyCdlibException
else:
    try:
        import pycdlib
        from pycdlib.pycdlibexception import PyCdlibException
    except ImportError:
        pycdlib = None  # type: ignore[assignment]
        PyCdlibException = Exception  # type: ignore[misc,assignment]

from archivey.base_reader import BaseArchiveReaderRandomAccess
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

logger = logging.getLogger(__name__)


class IsoReader(BaseArchiveReaderRandomAccess):
    """
    Reads ISO 9660 archives using the pycdlib library.
    """

    format = ArchiveFormat.ISO
    magic = b"CD001"  # Standard ISO 9660 identifier
    magic_offset = 0x8001

    def __init__(
        self,
        archive_path: str | bytes,
        password: Optional[str | bytes] = None,
        encoding: Optional[
            str
        ] = None,  # ISO 9660 typically uses its own encoding system.
    ):
        super().__init__(ArchiveFormat.ISO, archive_path)
        self.iso: Optional[pycdlib.pycdlib.PyCdlib] = None
        self.archive_path_obj: Optional[Path] = None

        if pycdlib is None:
            raise PackageNotInstalledError(
                "pycdlib package is not installed. Please install it to work with ISO archives."
            )

        self.iso = pycdlib.pycdlib.PyCdlib()
        self.iso.open(self.archive_path)

        if password:
            # ISO 9660 does not natively support encryption in a way pycdlib handles.
            # If password-protected ISOs exist, they are likely handled by proprietary means.
            # For standard ISOs, passwords are not applicable.
            # We could raise EncryptedArchiveError if a password is provided,
            # or simply ignore it. Ignoring seems more user-friendly for now.
            pass  # Ignoring password as ISOs are not typically encrypted

    def _path_variants(self, path: str) -> list[dict[str, str]]:
        """Return possible path keyword arguments for pycdlib functions."""
        assert self.iso is not None
        variants: list[dict[str, str]] = []
        if getattr(self.iso, "rock_ridge", None) is not None:
            variants.extend(
                [
                    {"rr_path": path},
                    {"rr_pathname": path},
                    {"rr_name": path},
                ]
            )
        if getattr(self.iso, "joliet", None) is not None:
            variants.extend(
                [
                    {"joliet_path": path},
                    {"joliet_pathname": path},
                    {"joliet_name": path},
                ]
            )
        variants.extend([{"iso_path": path.upper()}, {"path": path.upper()}])
        return variants

    def _call_with_path(self, func, path: str):
        """Call a pycdlib function with the best available path argument."""
        import inspect

        last_err: Exception | None = None
        params = set(inspect.signature(func).parameters)
        for kwargs in self._path_variants(path):
            key = next(iter(kwargs))
            if key not in params:
                continue
            try:
                return func(**kwargs)
            except TypeError as e:
                last_err = e
                continue
        if last_err is not None:
            raise last_err
        raise ArchiveError(f"Unable to call {func} with path {path}")

    def _is_pycdlib_dir(
        self,
        record,  #: pycdlib.DirectoryRecord
    ) -> bool:
        """Checks if a pycdlib DirectoryRecord is a directory."""
        return record.is_dir()

    def _is_pycdlib_symlink(
        self,
        record,  #: pycdlib.pycdlib.backends.pycdlibstructures.DirectoryRecord
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
        self,
        path: str,
        record,  #: pycdlib.backends.pycdlibstructures.DirectoryRecord
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
            raise ArchiveError("ISO not opened")

        try:
            list_dir_fn = getattr(self.iso, "list_dir", None)
            list_children_fn = getattr(self.iso, "list_children", None)

            if list_dir_fn is not None:
                entries = self._call_with_path(list_dir_fn, current_iso_path)
                iterator = ((n, r) for n, r in entries)
            elif list_children_fn is not None:
                records = self._call_with_path(list_children_fn, current_iso_path)

                def child_iter():
                    for rec in records:
                        name_bytes: bytes
                        if getattr(
                            self.iso, "rock_ridge", None
                        ) is not None and hasattr(rec, "rock_ridge_name"):
                            name = rec.rock_ridge_name()
                            name_bytes = (
                                name.encode("utf-8") if isinstance(name, str) else name
                            )
                        elif getattr(self.iso, "joliet", None) is not None and hasattr(
                            rec, "joliet_name"
                        ):
                            name = rec.joliet_name()
                            name_bytes = (
                                name.encode("utf-8") if isinstance(name, str) else name
                            )
                        else:
                            name = rec.file_identifier()
                            name_bytes = (
                                name.encode("utf-8") if isinstance(name, str) else name
                            )
                        yield name_bytes, rec

                iterator = child_iter()
            else:
                raise ArchiveError("No directory listing function available")

            for child_name_bytes, record in iterator:
                if (
                    getattr(self.iso, "rock_ridge", None) is not None
                    or getattr(self.iso, "joliet", None) is not None
                ):
                    child_name = child_name_bytes.decode("utf-8", "replace")
                else:
                    child_name = child_name_bytes.decode(
                        self.iso.iso_output_encoding, "replace"
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
                    # Construct path for recursion; keep same case as returned
                    next_iso_path = (
                        os.path.join(current_iso_path, child_name).replace("\\", "/")
                        + "/"
                    )
                    yield from self._walk_iso(member_path, next_iso_path)
        except PyCdlibException as e:
            raise ArchiveError(
                f"Error walking ISO directory {current_iso_path}: {e}"
            ) from e

    def get_members(self) -> List[ArchiveMember]:
        if not self.iso:
            raise ArchiveError("ISO not opened")
        return list(self.iter_members_with_io())

    def iter_members_with_io(
        self,
        filter: Callable[[ArchiveMember], bool] | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, IO[bytes] | None]]:
        if not self.iso:
            raise ArchiveError("ISO not opened")

        if pwd is not None:
            raise ArchiveError("Password is not supported for ISOReader")

        # Yield the root directory itself, as it's a common convention
        # pycdlib doesn't explicitly list the root dir record in list_dir('/')
        # We can try to get its record specifically.
        try:
            root_record = self._call_with_path(self.iso.get_record, "/")
            if root_record:
                yield self._convert_direntry_to_member("/", root_record)
        except PyCdlibException:
            # This might fail if the ISO is very strange or empty.
            # Log or handle as appropriate. For now, proceed to walk.
            pass

        for member in self._walk_iso(current_path="/", current_iso_path="/"):
            if filter is None or filter(member):
                try:
                    stream = self.open(member)
                    yield member, stream
                    stream.close()
                except (ArchiveError, OSError) as e:
                    logger.info(f"Error opening member {member.filename}: {e}")

    def open(
        self, member: ArchiveMember | str, *, pwd: Optional[str | bytes] = None
    ) -> IO[bytes]:
        if not self.iso:
            raise ArchiveError("ISO not opened")

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
            record = self._call_with_path(self.iso.get_record, iso_path)
            if record and self._is_pycdlib_dir(record):
                raise IsADirectoryError(
                    f"Cannot open directory '{member_name}' as a file stream."
                )

            file_data = self._call_with_path(self.iso.get_file_from_iso, iso_path)
            return io.BytesIO(file_data)
        except pycdlib.backends.pycdlibexceptions.PyCdlibInvalidInput:
            # This exception is often raised for "file not found"
            raise FileNotFoundError(f"File not found in ISO: {member_name}") from None
        except PyCdlibException as e:
            raise ArchiveError(
                f"Failed to open member '{member_name}' from ISO: {e}"
            ) from e

    def get_archive_info(self) -> ArchiveInfo:
        if not self.iso:
            raise ArchiveError("ISO not opened")

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
        except PyCdlibException:
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
            except PyCdlibException:
                # Log error or handle as needed, but don't let it prevent closing.
                pass
            self.iso = None
