import io
import logging
import lzma
from queue import Empty, Queue
from threading import Thread
from typing import IO, TYPE_CHECKING, Callable, Iterator, List, Optional, Union, cast

from archivey.base_reader import BaseArchiveReaderRandomAccess

if TYPE_CHECKING:
    import py7zr
    import py7zr.compressor
    import py7zr.exceptions
    import py7zr.helpers
    import py7zr.io
    from py7zr import Py7zIO, WriterFactory
    from py7zr.py7zr import ArchiveFile
else:
    try:
        import py7zr
        import py7zr.compressor
        import py7zr.exceptions
        import py7zr.helpers
        import py7zr.io
        from py7zr import Py7zIO, WriterFactory
        from py7zr.py7zr import ArchiveFile
    except ImportError:
        py7zr = None  # type: ignore[assignment]
        ArchiveFile = None  # type: ignore[misc,assignment]
        Py7zIO = object  # type: ignore[misc,assignment]
        WriterFactory = object  # type: ignore[misc,assignment]


from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    PackageNotInstalledError,
)
from archivey.formats import ArchiveFormat
from archivey.types import (
    ArchiveInfo,
    ArchiveMember,
    MemberType,
)
from archivey.utils import bytes_to_str

logger = logging.getLogger(__name__)


class StreamingFile(Py7zIO):
    class Reader(io.RawIOBase):
        def __init__(self, parent: "StreamingFile"):
            self._parent = parent
            self._buffer = bytearray()
            self._eof = False

        def read(self, size=-1) -> bytes:
            while not self._eof and (size < 0 or len(self._buffer) < size):
                try:
                    chunk = self._parent._data_queue.get(timeout=0.1)
                    if chunk is None:
                        self._eof = True
                        break
                    self._buffer.extend(chunk)
                except Empty:
                    continue

            if size < 0:
                size = len(self._buffer)

            data = self._buffer[:size]
            self._buffer = self._buffer[size:]
            return bytes(data)

        def close(self):
            self._parent._reader_alive = False
            self._parent._data_queue
            super().close()

        def readable(self):
            return True

        def writable(self):
            return False

        def seekable(self):
            return False

        # TODO: do we need to implement readall / readinto?

    def __init__(self, fname: str, files_queue: Queue, max_chunks=64):
        self._fname = fname
        self._data_queue = Queue(maxsize=max_chunks)
        self._reader_alive = True
        self._files_queue = files_queue
        self._reader = self.Reader(self)
        self._started = False
        self._closed = False

    def write(self, b: Union[bytes, bytearray]) -> int:
        if not self._started:
            self._started = True
            self._files_queue.put((self._fname, self._reader))
        if not self._reader_alive:
            return 0
        self._data_queue.put(b)
        return len(b)

    def seek(self, offset, whence=0):
        # After py7zr has finished writing, it calls seek(0) to prepare the stream
        # for reading. But since the stream is already being read, we use this as
        # an indication that the writing is finished.
        if offset == 0 and whence == 0:
            if not self._closed:
                self._data_queue.put(None)
                self._closed = True
            return 0
        raise io.UnsupportedOperation()

    def close(self):
        if not self._closed:
            self._data_queue.put(None)
            self._closed = True

    def size(self):
        return None

    def readable(self):
        return False

    def writable(self):
        return True

    def seekable(self):
        return False

    def read(self, size: Optional[int] = None) -> bytes:
        raise NotImplementedError("read not supported")

    def flush(self):
        raise NotImplementedError("flush not supported")


class StreamingFactory(WriterFactory):
    def __init__(self, q: Queue):
        self._queue = q

    def create(self, fname: str) -> Py7zIO:
        return StreamingFile(fname, self._queue)

    def yield_files(self) -> Iterator[tuple[str, IO[bytes]]]:
        while True:
            item = self._queue.get()
            if item is None:
                break
            yield item

    def finish(self):
        self._queue.put(None)


class SevenZipReader(BaseArchiveReaderRandomAccess):
    """Reader for 7-Zip archives."""

    def __init__(
        self,
        archive_path: str,
        *,
        pwd: bytes | str | None = None,
        resolve_links_in_get_members: bool = False,
    ):
        super().__init__(ArchiveFormat.SEVENZIP, archive_path)
        self._members: list[ArchiveMember] | None = None
        self._format_info: ArchiveInfo | None = None
        self._resolve_links_in_get_members = resolve_links_in_get_members

        if py7zr is None:
            raise PackageNotInstalledError(
                "py7zr package is not installed. Please install it to work with 7-Zip archives."
            )

        try:
            self._archive = py7zr.SevenZipFile(
                archive_path, "r", password=bytes_to_str(pwd)
            )

        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(f"Invalid 7-Zip archive {archive_path}") from e
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"7-Zip archive {archive_path} is encrypted"
            ) from e
        except TypeError as e:
            if "Unknown field" in str(e):
                raise ArchiveCorruptedError(
                    f"Corrupted header data or wrong password for {archive_path}"
                ) from e
            else:
                raise
        except EOFError as e:
            raise ArchiveCorruptedError(f"Invalid 7-Zip archive {archive_path}") from e
        except lzma.LZMAError as e:
            if "Corrupt input data" in str(e) and pwd is not None:
                raise ArchiveEncryptedError(
                    f"Corrupted header data or wrong password for {archive_path}"
                ) from e
            else:
                raise ArchiveCorruptedError(
                    f"Invalid 7-Zip archive {archive_path}"
                ) from e

    def close(self) -> None:
        """Close the archive and release any resources."""
        if self._archive:
            self._archive.close()
            self._archive = None
            self._members = None

    def _is_member_encrypted(self, file: ArchiveFile) -> bool:
        # This information is not directly exposed by py7zr, so we need to use an
        # internal function to infer it.
        if file.folder is None:
            return False

        return py7zr.compressor.SupportedMethods.needs_password(file.folder.coders)

    def get_members(self) -> List[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        if self._members is None:
            self._members = []

            links_to_resolve = {}

            for file in self._archive.files:
                member = ArchiveMember(
                    filename=file.filename,
                    # The uncompressed field is wrongly typed in py7zr as list[int].
                    # It's actually an int.
                    file_size=file.uncompressed,  # type: ignore
                    compress_size=file.compressed,
                    mtime=py7zr.helpers.filetime_to_dt(file.lastwritetime).replace(
                        tzinfo=None
                    )
                    if file.lastwritetime
                    else None,
                    type=(
                        MemberType.DIR
                        if file.is_directory
                        else MemberType.LINK
                        if file.is_symlink
                        else MemberType.OTHER
                        if file.is_junction or file.is_socket
                        else MemberType.FILE
                    ),
                    mode=file.posix_mode,
                    crc32=file.crc32,
                    compression_method=None,  # Not exposed by py7zr
                    encrypted=self._is_member_encrypted(file),
                    raw_info=file,
                )

                if member.is_link:
                    links_to_resolve[member.filename] = member
                self._members.append(member)

            if links_to_resolve and self._resolve_links_in_get_members:
                for filename, file_io in self.iter_members_with_io(
                    files=list(links_to_resolve.keys())
                ):
                    links_to_resolve[filename].link_target = file_io.read().decode(
                        "utf-8"
                    )

        return self._members

    def open(
        self, member_or_filename: ArchiveMember | str, *, pwd: str | None = None
    ) -> IO[bytes]:
        """Open a member of the archive.

        Warning: this is slow for 7-zip archives. Prefer using iter_members() if you
        need to read multiple members.

        Args:
            member: The member to open
            pwd: The password to use to open the member

        Returns:
            An IO object for the member.
        """

        if self._archive is None:
            raise ValueError("Archive is closed")

        member = self.get_member(member_or_filename)

        try:
            # Hack: py7zr only supports setting a password when creating the
            # SevenZipFile object, not when reaading a specific file. When uncompressing
            # a file, the password is read from the file's folder, so we can set it
            # there directly.
            file_info = cast(ArchiveFile, member.raw_info)
            if pwd is not None and file_info.folder is not None:
                previous_password = file_info.folder.password
                file_info.folder.password = bytes_to_str(pwd)

            it = list(self.iter_members_with_io(files=[member.filename]))
            assert len(it) == 1, (
                f"Expected exactly one member, got {len(it)}. {member.filename}"
            )
            return it[0][1]

        except py7zr.exceptions.ArchiveError as e:
            raise ArchiveCorruptedError(f"Error reading member {member.filename}: {e}")
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"Password required to read member {member.filename}"
            ) from e
        except lzma.LZMAError as e:
            raise ArchiveCorruptedError(
                f"Error reading member {member.filename}: {e}"
            ) from e
        finally:
            # Restore the folder to its previous state, to avoid side effects.
            if pwd is not None and file_info.folder is not None:
                file_info.folder.password = previous_password

    def iter_members_with_io(
        self,
        files: list[str] | None = None,
        filter: Callable[[ArchiveMember], bool] | None = None,
    ) -> Iterator[tuple[ArchiveMember, IO[bytes]]]:
        if self._archive is None:
            raise ValueError("Archive is closed")
        members_dict = {m.filename: m for m in self.get_members()}

        # Allow the queue to carry tuples, exceptions, or None
        q = Queue[tuple[str, IO[bytes]] | Exception | None]()

        def extractor():
            try:
                assert self._archive is not None
                self._archive.reset()
                factory = StreamingFactory(q)
                self._archive.extract(targets=files, factory=factory)
                factory.finish()
            except Exception as exc:
                q.put(exc)
                q.put(None)  # Ensure the main thread breaks out of the loop

        thread = Thread(target=extractor)
        thread.start()

        while True:
            item = q.get()
            if item is None:
                # End of file stream
                break
            if isinstance(item, Exception):
                thread.join()
                raise item
            fname, io = item
            member_info = members_dict.get(fname)
            assert member_info is not None, f"Member {fname} not found"
            if member_info.is_link and member_info.link_target is None:
                member_info.link_target = io.read().decode("utf-8")

            if filter is None or filter(member_info):
                yield member_info, io

            io.close()

        # TODO: the extractor may skip non-files or files with errors. Yield all remaining members. (but yield dirs before files?)

        thread.join()

    def get_archive_info(self) -> ArchiveInfo:
        """Get detailed information about the archive's format.

        Returns:
            ArchiveInfo: Detailed format information
        """
        if self._archive is None:
            raise ValueError("Archive is closed")

        sevenzip_info = self._archive.archiveinfo()

        if self._format_info is None:
            self._format_info = ArchiveInfo(
                format=self.format,
                is_solid=sevenzip_info.solid,
                extra={
                    "is_encrypted": self._archive.password_protected,
                },
            )
        return self._format_info
