import io
import logging
import lzma
import os
import struct
from queue import Empty, Queue
from threading import Thread
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    Iterator,
    List,
    Optional,
    Union,
    cast,
)

from archivey.base_reader import (
    BaseArchiveReaderRandomAccess,
    create_member_filter,
    _write_member,
)

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
    ArchiveEOFError,
    ArchiveError,
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
            self._first_read = True

        def read(self, size=-1) -> bytes:
            logger.info(
                f"Reading from reader file {self._parent._fname}: {size}",
                stack_info=self._first_read,
            )
            self._first_read = False
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
            # logger.info(
            #     f"Read from reader file {self._parent._fname}: asked {size}, got {len(data)}"
            # )
            return bytes(data)

        def close(self):
            # logger.info(
            #     f"Closing reader file {self._parent._fname}"
            # )  # , stack_info=True)
            self._parent._reader_alive = False
            self._parent._data_queue
            super().close()
            # logger.info(f"Closed reader file {self._parent._fname}")

        def readable(self):
            return True

        def writable(self):
            return False

        def seekable(self):
            return False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

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
        # logger.info(f"Writing to streaming file {self._fname}: {len(b)} bytes")
        if not self._started:
            self._started = True
            self._files_queue.put((self._fname, self._reader))
        if not self._reader_alive:
            return 0
        self._data_queue.put(b)
        return len(b)

    def seek(self, offset, whence=0):
        logger.info(f"Seek to writer file {self._fname}: {offset} {whence}")
        # After py7zr has finished writing, it calls seek(0) to prepare the stream
        # for reading. But since the stream is already being read, we use this as
        # an indication that the writing is finished.
        if offset == 0 and whence == 0:
            # logger.info(f"Closing writer file {self._fname} because of seek(0, 0)")
            if not self._closed:
                self._data_queue.put(None)
                self._closed = True
            # logger.info(f"Closed writer file {self._fname} because of seek(0, 0)")
            return 0
        raise io.UnsupportedOperation()

    def close(self):
        if not self._closed:
            # logger.info(f"Closing writer file {self._fname}")
            self._data_queue.put(None)
            self._closed = True
        # logger.info(f"Closed writer file {self._fname}")

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
        logger.info(f"Creating streaming file {fname}")
        return StreamingFile(fname, self._queue)

    def yield_files(self) -> Iterator[tuple[str, BinaryIO]]:
        while True:
            item = self._queue.get()
            if item is None:
                break
            logger.info(f"Yielding streaming file {item}")
            yield item

    def finish(self):
        self._queue.put(None)


# class BufferedReaderContextManager(io.BufferedReader):
#     def __init__(self, stream: io.RawIOBase):
#         super().__init__(stream, 65 * 1024)

#     def __enter__(self):
#         return self

#     def __exit__(self, exc_type, exc_value, traceback):
#         self.close()


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
        except struct.error as e:
            raise ArchiveEOFError(
                f"Possibly truncated 7-Zip archive {archive_path}"
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
                        else MemberType.SYMLINK
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
    ) -> BinaryIO:
        """Open a member of the archive.

        Warning: this is slow for 7-zip archives. Prefer using iter_members() if you
        need to read multiple members.

        Args:
            member: The member to open
            pwd: The password to use to open the member

        Returns:
            An IO object for the member.
        """

        logger.info(f"Opening member {member_or_filename} with password {pwd}")
        if self._archive is None:
            raise ValueError("Archive is closed")

        member = self.get_member(member_or_filename)
        logger.info(f"member {member}")

        try:
            # Hack: py7zr only supports setting a password when creating the
            # SevenZipFile object, not when reaading a specific file. When uncompressing
            # a file, the password is read from the file's folder, so we can set it
            # there directly.
            file_info = cast(ArchiveFile, member.raw_info)
            if pwd is not None and file_info.folder is not None:
                previous_password = file_info.folder.password
                file_info.folder.password = bytes_to_str(pwd)

            # logger.info("starting iterator")

            it = list(
                self.iter_members_with_io(files=[member.filename], close_streams=False)
            )
            # logger.info("iterator done")
            assert len(it) == 1, (
                f"Expected exactly one member, got {len(it)}. {member.filename}"
            )
            stream = cast(StreamingFile.Reader, it[0][1])
            return stream  # BufferedReaderContextManager(stream)

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
        *,
        close_streams: bool = True,
        pwd: bytes | str | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO]]:
        # TODO: set pwd in the folders

        if self._archive is None:
            raise ValueError("Archive is closed")
        # py7zr renames duplicate files when extracting by appending a
        # ``_<n>`` suffix to the later occurrences.  When we stream files
        # through a custom ``WriterFactory`` we receive those renamed
        # filenames, so we need to map them back to the actual archive
        # members.  Replicate py7zr's naming logic to build this mapping.
        members_dict: dict[str, ArchiveMember] = {}
        members_order: dict[str, int] = {}
        name_counters: dict[str, int] = {}
        for member in self.get_members():
            count = name_counters.get(member.filename, 0)
            if count == 0:
                outname = member.filename
            else:
                outname = f"{member.filename}_{count - 1}"
            name_counters[member.filename] = count + 1
            members_dict[outname] = member
            members_order[outname] = len(members_order)

        # Allow the queue to carry tuples, exceptions, or None
        q = Queue[tuple[str, BinaryIO] | Exception | None]()

        def extractor():
            try:
                assert self._archive is not None
                self._archive.reset()
                factory = StreamingFactory(q)
                # logger.info(f"extracting {files}")
                self._archive.extract(targets=files, factory=factory)
                # logger.info(f"extracting {files} done")
                factory.finish()
            except Exception as e:
                logger.error(
                    f"Error in extractor thread for archive {self.archive_path}",
                    exc_info=True,
                )
                q.put(e)
                # Catch all exceptions to avoid the main thread waiting forever, but
                # it will be re-raised in the main thread..
            # except (py7zr.exceptions.ArchiveError, OSError, lzma.LZMAError) as exc:
            # q.put(ArchiveCorruptedError(str(exc)))
            # q.put(None)  # Ensure the main thread breaks out of the loop

        thread = Thread(target=extractor)
        thread.start()

        outputs: list[tuple[str, StreamingFile.Reader]] = []
        try:
            while True:
                item = q.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    thread.join()
                    raise item
                fname, stream = item
                outputs.append((fname, cast(StreamingFile.Reader, stream)))

            for fname, stream in sorted(
                outputs, key=lambda x: members_order.get(x[0], 0)
            ):
                member_info = members_dict[fname]
                if member_info.is_link and member_info.link_target is None:
                    member_info.link_target = stream.read().decode("utf-8")
                if filter is None or filter(member_info):
                    yield member_info, stream
                if close_streams:
                    stream.close()

            # TODO: the extractor may skip non-files or files with errors. Yield all remaining members. (but yield dirs before files?)
        except (py7zr.exceptions.ArchiveError, lzma.LZMAError) as e:
            raise ArchiveCorruptedError(f"Error reading archive: {e}") from e

        finally:
            thread.join()

    def extract(
        self,
        member: ArchiveMember | str,
        path: str | None = None,
    ) -> str:
        if self._archive is None:
            raise ValueError("Archive is closed")

        member_obj = self.get_member(member)

        try:
            self._archive.extract(path=path, targets=[member_obj.filename])
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"Password required to extract member {member_obj.filename}"
            ) from e
        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(
                f"Invalid 7-Zip archive {self.archive_path}"
            ) from e
        except py7zr.exceptions.ArchiveError as e:
            raise ArchiveError(
                f"Error extracting member {member_obj.filename}: {e}"
            ) from e

        return os.path.join(path or os.getcwd(), member_obj.filename)

    def _extract_regular_files(
        self, path: str, members: list[ArchiveMember], pwd: bytes | str | None
    ) -> dict[str, str]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        written: dict[str, str] = {}
        if not members:
            return written

        names = [m.filename for m in members]
        if len(set(names)) != len(names):
            for m in members:
                for info, stream in self.iter_members_with_io(
                    files=[m.filename], filter=lambda x, r=m: x.raw_info is r.raw_info, pwd=pwd
                ):
                    wp = _write_member(path, m, True, stream)
                    if wp is not None:
                        written[m.filename] = wp
                    stream.close()
            return written

        try:
            self._archive.extract(path=path, targets=names)
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError("Password required to extract archive") from e
        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(
                f"Invalid 7-Zip archive {self.archive_path}"
            ) from e
        except py7zr.exceptions.ArchiveError as e:
            raise ArchiveError(f"Error extracting archive: {e}") from e
        for m in members:
            written[m.filename] = os.path.join(path, m.filename)

        return written

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
