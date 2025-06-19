import collections
import io
import logging
import lzma
import os
import pathlib
import struct
from abc import abstractmethod
from queue import Empty, Queue
from threading import Lock, Thread
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    Iterator,
    Optional,
    Union,
    cast,
)

from archivey.base_reader import (
    BaseArchiveReader,
    _build_iterator_filter,
)
from archivey.io_helpers import ErrorIOStream

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


from contextlib import contextmanager

from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    PackageNotInstalledError,
)
from archivey.extraction_helper import ExtractionHelper
from archivey.formats import ArchiveFormat
from archivey.types import (
    ArchiveInfo,
    ArchiveMember,
    MemberType,
)
from archivey.utils import bytes_to_str

logger = logging.getLogger(__name__)


class BasePy7zIOWriter(Py7zIO):
    def seek(self, offset, whence=0):
        if offset == 0 and whence == 0:
            self.close()
            return 0
        raise io.UnsupportedOperation()

    @abstractmethod
    def close(self):
        pass

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def read(self, size: Optional[int] = None) -> bytes:
        raise io.UnsupportedOperation()

    def flush(self):
        raise io.UnsupportedOperation()

    def size(self) -> int:
        raise io.UnsupportedOperation()


class StreamingFile(BasePy7zIOWriter):
    class Reader(io.RawIOBase, BinaryIO):
        def __init__(self, parent: "StreamingFile", pwd: bytes | str | None = None):
            self._parent = parent
            self._buffer = bytearray()
            self._eof = False
            self._first_read = True

        def read(self, size=-1) -> bytes:
            if self.closed:
                raise ValueError("Stream is closed")

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
            return bytes(data)

        def close(self):
            self._parent._reader_alive = False
            self._parent._data_queue.put(None)
            super().close()
            self._buffer = bytearray()
            self._eof = True

        def readable(self) -> bool:
            return True

        def writable(self) -> bool:
            return False

        def seekable(self) -> bool:
            return False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

        # TODO: do we need to implement readall / readinto?

    def __init__(
        self,
        fname: str,
        files_queue: Queue,
        max_chunks=64,
        pwd: bytes | str | None = None,
    ):
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

    def close(self):
        # logger.info(f"Closing streaming file {self._fname}")
        if not self._closed:
            self._data_queue.put(None)
            self._closed = True


class StreamingFactory(WriterFactory):
    def __init__(self, q: Queue, pwd: bytes | str | None = None):
        self._queue = q

    def create(self, fname: str) -> Py7zIO:
        # logger.info(f"Creating streaming file {fname}")
        return StreamingFile(fname, self._queue)

    def yield_files(self) -> Iterator[tuple[str, BinaryIO]]:
        while True:
            item = self._queue.get()
            if item is None:
                break
            # logger.info(f"Yielding streaming file {item}")
            yield item

    def finish(self):
        self._queue.put(None)


class ExtractFileWriter(BasePy7zIOWriter):
    def __init__(self, full_path: str, pwd: bytes | str | None = None):
        self.full_path = full_path
        os.makedirs(os.path.dirname(self.full_path), exist_ok=True)

        self.file = open(self.full_path, "wb")

    def write(self, b: Union[bytes, bytearray]) -> int:
        self.file.write(b)
        return len(b)

    def close(self):
        logger.debug(f"Closing file writer for {self.full_path}")
        self.file.close()


class ExtractLinkWriter(BasePy7zIOWriter):
    def __init__(self, member: ArchiveMember, pwd: bytes | str | None = None):
        self.data = bytearray()
        self.member = member

    def write(self, b: Union[bytes, bytearray]) -> int:
        self.data.extend(b)
        return len(b)

    def close(self):
        self.member.link_target = self.data.decode("utf-8")
        # self._extraction_helper.extract_member(self._member, None)


class ExtractWriterFactory(WriterFactory):
    def __init__(
        self,
        path: str,
        extract_filename_to_member: dict[str, ArchiveMember],
    ):
        self._path = path
        self._extract_filename_to_member = extract_filename_to_member
        self.member_id_to_outfile: dict[int, str] = {}
        self.outfiles: set[str] = set()

    def create(self, fname: str) -> Py7zIO:
        member = self._extract_filename_to_member.get(fname)
        if member is None:
            logger.error(f"Member {fname} not found")
            return py7zr.io.NullIO()
        elif member.is_link:
            logger.debug(f"Extracting link {fname}")
            return ExtractLinkWriter(member)
        elif not member.is_file:
            logger.debug(f"Ignoring non-file member {fname}")
            return py7zr.io.NullIO()

        full_path = os.path.join(self._path, fname)
        if os.path.lexists(full_path) or full_path in self.outfiles:
            full_path += f"_{member.member_id}"

        self.member_id_to_outfile[member.member_id] = full_path
        self.outfiles.add(full_path)

        logger.debug(f"Creating writer for {fname}, path={full_path}")
        return ExtractFileWriter(full_path)


class SevenZipReader(BaseArchiveReader):
    """Reader for 7-Zip archives."""

    _password_lock: Lock = Lock()

    @contextmanager
    def _temporary_password(self, pwd: bytes | str | None):
        """Temporarily set the password for all folders in the archive."""
        if pwd is None or self._archive is None:
            yield
            return

        SevenZipReader._password_lock.acquire()
        try:
            folders = []
            try:
                folders = []
                if (
                    self._archive.header
                    and self._archive.header.main_streams
                    and self._archive.header.main_streams.unpackinfo
                ):
                    folders = self._archive.header.main_streams.unpackinfo.folders
            except AttributeError:
                folders = []

            previous = [f.password for f in folders]
            for f in folders:
                f.password = bytes_to_str(pwd)

            try:
                yield
            finally:
                for f, p in zip(folders, previous):
                    f.password = p
        finally:
            if pwd is not None:
                SevenZipReader._password_lock.release()

    def __init__(
        self,
        archive_path: BinaryIO | str,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ):
        super().__init__(
            ArchiveFormat.SEVENZIP,
            archive_path,
            members_list_supported=True,
            random_access_supported=not streaming_only,
            pwd=pwd,
        )
        self._format_info: ArchiveInfo | None = None
        self._streaming_only = streaming_only

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

    def _is_member_encrypted(self, file: ArchiveFile) -> bool:
        # This information is not directly exposed by py7zr, so we need to use an
        # internal function to infer it.
        if file.folder is None:
            return False

        return py7zr.compressor.SupportedMethods.needs_password(file.folder.coders)

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        name_counters: collections.defaultdict[str, int] = collections.defaultdict(int)
        links_to_resolve = {}

        for file in self._archive.files:
            # py7zr renames duplicate files when extracting by appending a
            # ``_<n>`` suffix to the later occurrences.  When we stream files
            # through a custom ``WriterFactory`` we receive those renamed
            # filenames, so we need to map them back to the actual archive
            # members.  Replicate py7zr's naming logic to build this mapping.

            count = name_counters[file.filename]
            if count == 0:
                extract_filename = file.filename
            else:
                extract_filename = f"{file.filename}_{count - 1}"

            name_counters[file.filename] += 1

            # 7z format doesn't include the trailing slash for directories, so we need
            # to add them for consistent behavior.
            filename = file.filename
            if file.is_directory and not filename.endswith("/"):
                filename += "/"
            file_type = (
                MemberType.DIR
                if file.is_directory
                else MemberType.SYMLINK
                if file.is_symlink
                else MemberType.OTHER
                if file.is_junction or file.is_socket
                else MemberType.FILE
            )
            crc32 = (
                file.crc32
                if file.crc32 is not None
                else 0
                if (file_type == MemberType.FILE and file.uncompressed == 0)
                else None
            )

            member = ArchiveMember(
                filename=filename,
                # The uncompressed field is wrongly typed in py7zr as list[int].
                # It's actually an int.
                file_size=file.uncompressed,  # type: ignore
                compress_size=file.compressed,
                mtime_with_tz=py7zr.helpers.filetime_to_dt(file.lastwritetime)
                if file.lastwritetime
                else None,
                type=file_type,
                # link_target_type=
                mode=file.posix_mode,
                crc32=crc32,
                compression_method=None,  # Not exposed by py7zr
                encrypted=self._is_member_encrypted(file),
                raw_info=file,
                extra={
                    "extract_filename": extract_filename,
                },
            )

            if member.is_link:
                links_to_resolve[member.filename] = member
            yield member

        if links_to_resolve and not self._streaming_only:
            try:
                for member, stream in self._extract_members_iterator(
                    members=list(links_to_resolve.values())
                ):
                    member.link_target = stream.read().decode("utf-8")
            except ArchiveError as e:
                logger.error(f"Error resolving links: {e}")
                # Skip the links that failed to resolve, they'll just have an empty
                # link target.

        self._all_members_registered = True

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

        if self._archive is None:
            raise ValueError("Archive is closed")

        member, filename = self._resolve_member_to_open(member_or_filename)

        try:
            it = list(
                self.iter_members_with_io(
                    members=[member], pwd=pwd, close_streams=False
                )
            )
            assert len(it) == 1, (
                f"Expected exactly one member, got {len(it)}. {member.filename}"
            )
            stream = cast(StreamingFile.Reader, it[0][1])
            if isinstance(stream, ErrorIOStream):
                # When a wrong password is provided, iter_members_with_io
                # returns an ErrorIOStream instead of raising. In the context of
                # `open`, propagate the error to match the behaviour of other
                # readers.
                stream.read()
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
            pass

    def _extract_members_iterator(
        self,
        members: list[ArchiveMember],
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO]]:
        extract_filename_to_member = {
            member.extra["extract_filename"]: member for member in members
        }
        # The original filenames in the raw infos.
        extract_targets = [
            cast(ArchiveFile, member.raw_info).filename for member in members
        ]

        # Allow the queue to carry tuples, exceptions, or None
        q = Queue[tuple[str, BinaryIO] | Exception | None]()

        # TODO: check that all the requested files to extract() were actually
        # extracted exactly once.
        def extractor():
            try:
                assert self._archive is not None
                self._archive.reset()
                factory = StreamingFactory(q)
                with self._temporary_password(pwd):
                    self._archive.extract(targets=extract_targets, factory=factory)
                    factory.finish()
            except Exception as e:
                q.put(e)
                # Catch all exceptions to avoid the main thread waiting forever.
                # Any exception will be re-raised in the main thread.

        thread = Thread(target=extractor)
        thread.start()

        logger.debug(f"iter_members_iterator: starting -- targets: {extract_targets}")
        try:
            while True:
                item = q.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    thread.join()
                    raise item
                fname, stream = item

                if fname not in extract_filename_to_member:
                    logger.warning(
                        f"fname not in extract_filename_to_member: {fname} (names: {extract_filename_to_member.keys()})"
                    )
                    continue

                member_info = extract_filename_to_member[fname]
                # logger.info(f"fname: {fname}, member_info: {member_info}")
                yield member_info, stream

            # TODO: the extractor may skip non-files or files with errors. Yield all remaining members. (but yield dirs before files?)
        except (py7zr.exceptions.ArchiveError, lzma.LZMAError) as e:
            raise ArchiveCorruptedError(f"Error reading archive: {e}") from e
        except py7zr.exceptions.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"Password required to read archive: {e}"
            ) from e

        finally:
            thread.join()

    def iter_members_with_io(
        self,
        members: list[str | ArchiveMember]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
        close_streams: bool = True,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        if self._archive is None:
            raise ValueError("Archive is closed")

        self._start_streaming_iteration()

        # Don't apply the filter now, as the link members may not have the extracted path.
        # logger.info(f"iter members arg: {members}")
        member_filter_func = _build_iterator_filter(members, None)
        # logger.info(f"members: {self.get_members()}")
        # filtered_members = [m for m in self.get_members() if member_filter_func(m)]

        # logger.info(f"filtered_members: {filtered_members}")  # TODO: remove

        # logger.debug(f"extract_filename_to_member: {extract_filename_to_member}")

        # member_included = _build_member_included_func(members)

        # members_to_extract = [] #m for m in self.get_members() if member_included(m)]
        # extract_filename_to_member = {}
        # filenames_to_extract = []

        pending_filtered_members_by_id: dict[int, ArchiveMember] = {}
        pending_links_by_id: dict[int, ArchiveMember] = {}

        logger.debug("iter_members_with_io: starting first pass")
        for member in self.iter_members():
            if not member_filter_func(member):
                continue

            if member.is_link and member.link_target is None:
                # We'll need to resolve the link target later.
                # filenames_to_extract.append(member.filename)
                # extract_filename_to_member[member.extra["extract_filename"]] = member
                pending_links_by_id[member.member_id] = member
                continue

            filtered_member = filter(member) if filter is not None else member
            if filtered_member is None:
                continue

            if member.is_dir or member.is_link:
                # TODO: accumulate all links to yield at the end, after resolving any
                # pending target members.
                yield filtered_member, None

            elif member.is_file and member.file_size == 0:
                # Yield any empty files immediately, as py7zr doesn't actually call any
                # methods on the PyZ7IO object for them, and so they're not added to the
                # queue.
                stream = io.BytesIO(b"")
                yield filtered_member, stream
                if close_streams:
                    stream.close()

            elif member.is_file:
                # filenames_to_extract.append(member.filename)
                # extract_filename_to_member[member.extra["extract_filename"]] = member
                pending_filtered_members_by_id[member.member_id] = filtered_member
            else:
                logger.error(
                    f"Unknown member type: {member.type} for {member.filename}"
                )
                continue

        # logger.debug("iter_members_with_io: first pass done")
        # # logger.debug(f"extract_filename_to_member: {extract_filename_to_member}")
        # logger.debug(f"pending_filtered_members_by_id: {pending_filtered_members_by_id}")

        try:
            for member, stream in self._extract_members_iterator(
                members=list(pending_filtered_members_by_id.values())
                + list(pending_links_by_id.values()),
                pwd=pwd,
                filter=filter,
            ):
                if member.is_link:
                    # The links we extracted are the ones with the link target not yet set.
                    member.link_target = stream.read().decode("utf-8")

                else:
                    filtered_member = pending_filtered_members_by_id.pop(
                        member.member_id
                    )
                    yield filtered_member, stream
                    if close_streams:
                        stream.close()
        except ArchiveError as e:
            logger.error(f"Error in iter_members_with_io: {e}")
            # Yield any remaining members that were not extracted, with the error.
            for member in pending_filtered_members_by_id.values():
                yield member, ErrorIOStream(e)
            # raise e

        for member in pending_links_by_id.values():
            yield member, None

    def extract(
        self,
        member: ArchiveMember | str,
        path: str | None = None,
        *,
        pwd: bytes | str | None = None,
    ) -> str:
        if self._archive is None:
            raise ValueError("Archive is closed")

        member_obj = self.get_member(member)

        try:
            with self._temporary_password(pwd):
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

    def _extract_pending_files(
        self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None
    ) -> None:
        pending_extractions = extraction_helper.get_pending_extractions()
        paths_to_extract = [member.filename for member in pending_extractions]
        # Perform a regular extraction
        assert self._archive is not None

        canonical_path = pathlib.Path(os.getcwd()).joinpath(path)

        def _py7zr_full_path(member: ArchiveMember) -> str:
            outname = member.extra["extract_filename"]
            return py7zr.helpers.get_sanitized_output_path(
                outname, canonical_path
            ).as_posix()

        pending_extractions_to_member = {
            _py7zr_full_path(member): member for member in pending_extractions
        }
        factory = ExtractWriterFactory(path, pending_extractions_to_member)

        logger.info(f"Extracting {paths_to_extract} to {path}")
        try:
            with self._temporary_password(pwd):
                self._archive.extract(
                    path, targets=paths_to_extract, recursive=False, factory=factory
                )
        except py7zr.PasswordRequired as e:
            raise ArchiveEncryptedError(
                f"Password required to extract archive {self.archive_path}"
            ) from e
        except py7zr.Bad7zFile as e:
            raise ArchiveCorruptedError(
                f"Invalid 7-Zip archive {self.archive_path}"
            ) from e
        except py7zr.exceptions.ArchiveError as e:
            raise ArchiveError(
                f"Error extracting archive {self.archive_path}: {e}"
            ) from e
        except lzma.LZMAError as e:
            raise ArchiveCorruptedError(
                f"Error extracting archive {self.archive_path}: {e}"
            ) from e
        logger.info("Extraction done")

        for member in pending_extractions:
            outfile = factory.member_id_to_outfile.get(member.member_id)
            extraction_helper.process_file_extracted(member, outfile)

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

    @classmethod
    def is_7z_file(cls, file: BinaryIO | str) -> bool:
        if py7zr is not None:
            return py7zr.is_7zfile(file)
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python -m archivey.sevenzip_reader <archive_path>")
        sys.exit(1)

    archive_path = sys.argv[1]
    with SevenZipReader(archive_path, format=ArchiveFormat.SEVENZIP) as archive:
        for member in archive.get_members():
            print(member)

        print()
        for member, stream in archive.iter_members_with_io():
            assert isinstance(member.raw_info, ArchiveFile)
            print(
                member.member_id,
                member.filename,
                member.raw_info.filename,
                stream.read() if stream is not None else "NO STREAM",
            )

        print()
        for member in archive.get_members():
            assert isinstance(member.raw_info, ArchiveFile)
            stream = archive.open(member)
            print(
                member.member_id,
                member.filename,
                member.raw_info.filename,
                stream.read() if stream is not None else "NO STREAM",
            )
