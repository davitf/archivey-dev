import collections
import io
import logging
import lzma
import os
import pathlib
import struct
from abc import abstractmethod
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
    _build_iterator_filter,
    _build_member_included_func,
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
from archivey.extraction_helper import ExtractionHelper
from archivey.formats import ArchiveFormat
from archivey.types import (
    ArchiveInfo,
    ArchiveMember,
    MemberType,
)
from archivey.utils import bytes_to_str

logger = logging.getLogger(__name__)


class StreamingFile(Py7zIO):
    class Reader(io.RawIOBase, BinaryIO):
        def __init__(self, parent: "StreamingFile"):
            self._parent = parent
            self._buffer = bytearray()
            self._eof = False
            self._first_read = True

        def read(self, size=-1) -> bytes:
            # logger.info(
            #     f"Reading from reader file {self._parent._fname}: {size}",
            #     stack_info=self._first_read,
            # )
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
            self._parent._data_queue
            super().close()

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


class BaseExtractWriter(Py7zIO):
    def __init__(
        self, member: ArchiveMember | None, extraction_helper: ExtractionHelper
    ):
        self._member = member
        self._extraction_helper = extraction_helper

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


class ExtractFileWriter(BaseExtractWriter):
    def __init__(
        self, member: ArchiveMember, extraction_helper: ExtractionHelper, full_path: str
    ):
        super().__init__(member, extraction_helper)
        self._full_path = full_path
        os.makedirs(os.path.dirname(self._full_path), exist_ok=True)
        self._file = open(self._full_path, "wb")

    def write(self, b: Union[bytes, bytearray]) -> int:
        self._file.write(b)
        return len(b)

    def close(self):
        assert self._member is not None
        logger.error(f"Closing file writer for {self._member.filename}")
        self._file.close()
        self._extraction_helper.process_file_extracted(self._member, self._full_path)


class ExtractLinkWriter(BaseExtractWriter):
    def __init__(self, member: ArchiveMember, extraction_helper: ExtractionHelper):
        super().__init__(member, extraction_helper)
        self.data = bytearray()

    def write(self, b: Union[bytes, bytearray]) -> int:
        self.data.extend(b)
        return len(b)

    def close(self):
        assert self._member is not None
        logger.error(
            f"Closing link writer for {self._member.filename}, target={self.data.decode('utf-8')}"
        )
        self._member.link_target = self.data.decode("utf-8")
        self._extraction_helper.extract_member(self._member, None)


class ExtractWriterFactory(WriterFactory):
    def __init__(
        self,
        path: str,
        extract_filename_to_member: dict[str, ArchiveMember],
        extraction_helper: ExtractionHelper,
    ):
        self._path = path
        self._extract_filename_to_member = extract_filename_to_member
        self._extraction_helper = extraction_helper

    def create(self, fname: str) -> Py7zIO:
        logger.error(f"Creating writer for {fname}, path={self._path}")

        # if os.path.commonpath([self._path, fname]) == self._path:
        #     fname = os.path.relpath(fname, self._path)
        #     logger.error(f"fname={fname}")

        member = self._extract_filename_to_member.get(fname)
        if member is None:
            logger.error(f"Member {fname} not found")
            return py7zr.io.NullIO()
        if member.is_file:
            logger.error(f"Extracting file {fname}")
            return ExtractFileWriter(
                member, self._extraction_helper, os.path.join(self._path, fname)
            )
        elif member.is_link:
            logger.error(f"Extracting link {fname}")
            return ExtractLinkWriter(member, self._extraction_helper)
        else:
            logger.error(f"Ignoring member {fname}")
            return py7zr.io.NullIO()


class SevenZipReader(BaseArchiveReaderRandomAccess):
    """Reader for 7-Zip archives."""

    def __init__(
        self,
        archive_path: str,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ):
        super().__init__(ArchiveFormat.SEVENZIP, archive_path)
        self._members: list[ArchiveMember] | None = None
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

        if self._members is not None:
            return self._members

        self._members = []

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
                # link_target_type=
                mode=file.posix_mode,
                crc32=file.crc32,
                compression_method=None,  # Not exposed by py7zr
                encrypted=self._is_member_encrypted(file),
                raw_info=file,
                extra={
                    "extract_filename": extract_filename,
                },
            )

            if member.is_link:
                links_to_resolve[member.filename] = member
            self._members.append(member)
            self.register_member(member)

        if links_to_resolve and not self._streaming_only:
            # iter_members_with_io() automatically populates the link_target field.
            for filename, file_io in self.iter_members_with_io(
                members=list(links_to_resolve.keys())
            ):
                pass

        self.set_all_members_retrieved()

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

            it = list(self.iter_members_with_io(members=[member], close_streams=False))
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
        members: list[str | ArchiveMember]
        | Callable[[ArchiveMember], bool]
        | None = None,
        *,
        pwd: bytes | str | None = None,
        filter: Callable[[ArchiveMember], ArchiveMember | None] | None = None,
        close_streams: bool = True,
    ) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
        # TODO: set pwd in the folders

        if self._archive is None:
            raise ValueError("Archive is closed")

        # Don't apply the filter now, as the link members may not have the extracted path.
        member_filter_func = _build_iterator_filter(members, None)
        filtered_members = [m for m in self.get_members() if member_filter_func(m)]

        extract_filename_to_member = {
            member.extra["extract_filename"]: member for member in filtered_members
        }

        reverse_map = {
            m.internal_id: name for name, m in extract_filename_to_member.items()
        }

        member_included = _build_member_included_func(members)
        members_to_extract = [m for m in self.get_members() if member_included(m)]
        members_order = {
            reverse_map[m.internal_id]: i for i, m in enumerate(members_to_extract)
        }
        files = [reverse_map[m.internal_id] for m in members_to_extract]

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
                # Catch all exceptions to avoid the main thread waiting forever.
                # Any exception will be re-raised in the main thread.

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
                member_info = extract_filename_to_member[fname]
                if member_info.is_link and member_info.link_target is None:
                    member_info.link_target = stream.read().decode("utf-8")
                    # TODO: fill link_target_member for hard links

                filtered_member = (
                    filter(member_info) if filter is not None else member_info
                )
                if filtered_member is not None:
                    yield filtered_member, stream if filtered_member.is_file else None
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

    # def _extract_files_batch(
    #     self,
    #     files_to_extract: List[ArchiveMember],
    #     target_path: str,
    #     pwd: bytes | str | None,  # Note: py7zr uses password from SevenZipFile instance
    #     written_paths: dict[str, str],
    # ) -> None:
    #     if not files_to_extract:
    #         return

    #     if self._archive is None:
    #         logger.error(
    #             f"SevenZipReader._archive is None for {self.archive_path}, cannot extract files."
    #         )
    #         # Raise an error to make it clear that the operation cannot proceed.
    #         raise ArchiveError(
    #             f"Archive object not available for {self.archive_path} during _extract_files_batch"
    #         )

    #     filenames_to_extract = [member.filename for member in files_to_extract]

    #     try:
    #         # py7zr's extract method can take a list of targets.
    #         # Password is handled by the self._archive instance, set at initialization.
    #         self._archive.extract(path=target_path, targets=filenames_to_extract)

    #         # Verify extraction and update written_paths
    #         for member in files_to_extract:
    #             extracted_file_path = os.path.join(target_path, member.filename)
    #             # We are interested if the *file* was created.
    #             # py7zr might create parent directories, but _extract_files_batch is for files.
    #             if os.path.isfile(extracted_file_path):
    #                 written_paths[member.filename] = extracted_file_path
    #             elif os.path.exists(
    #                 extracted_file_path
    #             ):  # It's not a file, maybe a dir
    #                 logger.debug(
    #                     f"Path {extracted_file_path} for member {member.filename} "
    #                     "exists but is not a file (likely a directory created by py7zr), not adding to written_paths as a file."
    #                 )
    #             else:
    #                 # This case means py7zr was asked to extract it, but it's not there.
    #                 logger.warning(
    #                     f"File {member.filename} was targeted for extraction by py7zr from archive {self.archive_path} but not found at {extracted_file_path}."
    #                 )
    #     except py7zr.exceptions.PasswordRequired as e:
    #         logger.error(
    #             f"Password required for extracting files from 7zip archive {self.archive_path}: {e}",
    #             exc_info=True,
    #         )
    #         # Ensure this error propagates, as it's a critical failure for these files.
    #         raise ArchiveEncryptedError(
    #             f"Password required for 7zip extraction from {self.archive_path}: {e}"
    #         ) from e
    #     except (py7zr.exceptions.ArchiveError, lzma.LZMAError) as e:
    #         logger.error(
    #             f"Error during 7zip batch extraction from {self.archive_path}: {e}",
    #             exc_info=True,
    #         )
    #         raise ArchiveError(
    #             f"Error during 7zip batch extraction from {self.archive_path}: {e}"
    #         ) from e
    #     except Exception as e:  # Catch any other unexpected errors
    #         logger.error(
    #             f"Unexpected error during 7zip batch extraction from {self.archive_path}: {e}",
    #             exc_info=True,
    #         )
    #         raise ArchiveError(
    #             f"Unexpected error during 7zip batch extraction from {self.archive_path}: {e}"
    #         ) from e

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
        factory = ExtractWriterFactory(
            path, pending_extractions_to_member, extraction_helper
        )

        logger.info(f"Extracting {paths_to_extract} to {path}")
        self._archive.extract(
            path, targets=paths_to_extract, recursive=False, factory=factory
        )
        logger.info("Extraction done")

        # for member in pending_extractions:
        #     rel_path = os.path.relpath(member.filename, path)
        #     extraction_helper.process_external_extraction(member, rel_path)

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


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python -m archivey.sevenzip_reader <archive_path>")
        sys.exit(1)

    archive_path = sys.argv[1]
    with SevenZipReader(archive_path) as archive:
        for member in archive.get_members():
            print(member)

        print()
        for member, stream in archive.iter_members_with_io():
            assert isinstance(member.raw_info, ArchiveFile)
            print(
                member.internal_id,
                member.filename,
                member.raw_info.filename,
                stream.read() if stream is not None else "NO STREAM",
            )

        print()
        for member in archive.get_members():
            assert isinstance(member.raw_info, ArchiveFile)
            stream = archive.open(member)
            print(
                member.internal_id,
                member.filename,
                member.raw_info.filename,
                stream.read() if stream is not None else "NO STREAM",
            )
