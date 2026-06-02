"""Seekable decompressor stream base class and format-specific subclasses."""

import abc
import bisect
import io
import logging
import os
import zlib
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Generic,
    TypeVar,
    cast,
)

from typing_extensions import Buffer

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError
from archivey.formats.lzip_stream import _LzipState, _read_index_backwards
from archivey.internal.io_helpers import ensure_bufferedio

if TYPE_CHECKING:
    import brotli
else:
    try:
        import brotli
    except ImportError:
        brotli = None

logger = logging.getLogger(__name__)


@dataclass(order=True)
class SeekPoint:
    """A point in a compressed stream from which decompression can resume.

    Ordered by decompressed_offset only, so bisect operations on a
    list[SeekPoint] work without a key= argument.
    """

    decompressed_offset: int
    compressed_offset: int = field(compare=False)
    state: Any = field(default=None, compare=False)


DecompressorT = TypeVar("DecompressorT")


class DecompressorStream(io.RawIOBase, BinaryIO, Generic[DecompressorT]):
    """Seekable decompressor stream with optional seek-point-based random access.

    Subclasses implement the four abstract methods to provide the actual
    decompression.  Subclasses that support efficient random access may also:

    - Call add_seek_points() from _decompress_chunk/_flush_decompressor to
      register known positions as they are discovered during forward reads.
    - Override _build_index() for a one-shot full index build (e.g. lzip reads
      member trailers backwards).  _build_index is called at most once.
    - Override _reset_to_seek_point() for any format-specific state updates
      when jumping to a known seek point.
    """

    def __init__(self, path: str | BinaryIO) -> None:
        super().__init__()
        if isinstance(path, (str, bytes, os.PathLike)):
            self._inner = open(path, "rb")
            self._should_close = True
        else:
            self._inner = ensure_bufferedio(path)
            self._should_close = False
        self._seek_points: list[SeekPoint] = []
        self._index_built: bool = False
        self._decompressor: DecompressorT = self._create_decompressor()
        self._buffer = bytearray()
        self._eof = False
        self._pos = 0
        self._size: int | None = None

    @abc.abstractmethod
    def _create_decompressor(self, state: Any = None) -> DecompressorT: ...

    @abc.abstractmethod
    def _decompress_chunk(self, chunk: bytes) -> bytes: ...

    @abc.abstractmethod
    def _flush_decompressor(self) -> bytes: ...

    @abc.abstractmethod
    def _is_decompressor_finished(self) -> bool: ...

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:  # pragma: no cover - not used
        return False

    def seekable(self) -> bool:
        return self._inner.seekable()

    def add_seek_points(self, points: list[SeekPoint]) -> None:
        """Merge seek points into the sorted index, skipping duplicates.

        Callers should pass points in ascending decompressed_offset order.
        The common path (new point is after all existing ones) is O(1) append;
        out-of-order insertions fall back to bisect + insert.
        """
        for point in points:
            if self._seek_points and point < self._seek_points[-1]:
                # Out-of-order (should be rare): bisect insert
                i = bisect.bisect_left(self._seek_points, point)
                if i < len(self._seek_points) and self._seek_points[i] == point:
                    continue  # duplicate
                self._seek_points.insert(i, point)
            elif self._seek_points and self._seek_points[-1] == point:
                continue  # duplicate
            else:
                self._seek_points.append(point)  # fast path: in-order append

    def _find_best_seek_point(self, pos: int) -> SeekPoint | None:
        """Return the last seek point with decompressed_offset <= pos."""
        if not self._seek_points:
            return None
        i = bisect.bisect_right(self._seek_points, SeekPoint(pos, 0)) - 1
        return self._seek_points[i] if i >= 0 else None

    def _reset_to_seek_point(self, point: SeekPoint) -> None:
        """Jump to a known seek point.  Does not touch _size."""
        self._inner.seek(point.compressed_offset)
        self._decompressor = self._create_decompressor(point.state)
        self._buffer.clear()
        self._eof = False
        self._pos = point.decompressed_offset

    def _build_index(self, last_known: SeekPoint | None) -> None:
        """One-shot full index build.  Default: no-op.

        Subclasses that support random access override this to populate
        _seek_points (and optionally set _size) without decompressing.
        Called at most once per stream (guarded by _index_built in seek()).

        last_known: the highest-offset SeekPoint currently in _seek_points,
        or None if the index is empty.  Subclasses may use it to start
        forward indexing from there or to stop a backwards scan at that point.
        """

    def _rewind(self) -> None:
        self._reset_to_seek_point(SeekPoint(0, 0))
        self._size = None
        # _seek_points and _index_built are preserved across rewinds

    def _read_decompressed_chunk(self) -> bytes:
        chunk = self._inner.read(65536)
        if not chunk:
            self._eof = True
            leftover = self._flush_decompressor()
            logger.info("EOF reached, leftover: %d", len(leftover))
            if not self._is_decompressor_finished():
                raise ArchiveEOFError("File is truncated")
            self._size = self._pos + len(self._buffer) + len(leftover)
            self._index_built = True  # forward scan to EOF implies complete index
            logger.info("EOF reached, size: %d", self._size)
            return leftover
        return self._decompress_chunk(chunk)

    def _seek_to_pos(self, pos: int) -> None:
        # Short-circuit when we know the total size and pos is at or past EOF
        if self._size is not None and pos >= self._size:
            self._buffer.clear()
            self._eof = True
            self._pos = pos
            return

        if pos == self._pos:
            return

        if pos < self._pos:
            best = self._find_best_seek_point(pos)
            if best is not None:
                self._reset_to_seek_point(best)
            else:
                self._rewind()

        # pos already in the buffer?
        if self._pos + len(self._buffer) >= pos:
            del self._buffer[: pos - self._pos]
            self._pos = pos
            return

        # Forward jump to a closer seek point
        best = self._find_best_seek_point(pos)
        if best is not None and best.decompressed_offset > self._pos:
            self._reset_to_seek_point(best)
            # Re-check buffer after jump (handles exact seek-point boundary)
            if self._pos + len(self._buffer) >= pos:
                del self._buffer[: pos - self._pos]
                self._pos = pos
                return

        # Forward-read loop
        self._pos += len(self._buffer)
        self._buffer.clear()

        while not self._eof:
            decompressed = self._read_decompressed_chunk()
            if self._pos + len(decompressed) >= pos:
                self._buffer.extend(decompressed[pos - self._pos :])
                self._pos = pos
                return
            self._pos += len(decompressed)

        # Past EOF
        self._pos = pos

    def readall(self) -> bytes:
        while not self._eof:
            self._buffer.extend(self._read_decompressed_chunk())

        data = bytes(self._buffer)
        self._pos += len(data)
        if self._size is not None:
            assert self._size == self._pos
        self._size = self._pos
        self._buffer.clear()
        return data

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n is None or n < 0:
            return self.readall()

        if len(self._buffer) < n and not self._eof:
            # Read only one more block
            self._buffer.extend(self._read_decompressed_chunk())

        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        self._pos += len(data)
        return data

    def readinto(self, b: Buffer) -> int:
        mv = memoryview(b).cast("B")  # type: ignore[arg-type]
        data = self.read(len(mv))
        mv[: len(data)] = data
        return len(data)

    def close(self) -> None:
        if self._should_close:
            self._inner.close()
        super().close()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos: int | None = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = None  # resolved after _build_index / readall
        else:
            raise ValueError(f"Invalid whence: {whence}")

        # Trigger _build_index once when the target is past known territory or SEEK_END
        if not self._index_built and self._inner.seekable():
            last = self._seek_points[-1] if self._seek_points else None
            should_build = new_pos is None or (
                new_pos > 0
                and (
                    not self._seek_points or new_pos > last.decompressed_offset  # type: ignore[union-attr]
                )
            )
            if should_build:
                self._build_index(last)
                self._index_built = True

        if whence == io.SEEK_END:
            if self._size is None:
                self.readall()
                assert self._size is not None
            new_pos = self._size + offset

        assert new_pos is not None
        if new_pos < 0:
            raise ValueError(f"Invalid offset: {offset}")

        self._seek_to_pos(new_pos)
        return self._pos

    def tell(self) -> int:
        return self._pos


class LzipDecompressorStream(DecompressorStream[_LzipState]):
    """Seekable lzip decompressor backed by Python's stdlib lzma.

    Builds a seek-point table from member headers/trailers:
      - Progressively via _update_index() as members are decoded forward.
      - On-demand via a one-shot backwards trailer scan (_build_index()).

    The table enables efficient SEEK_END (no decompression), backward seeks
    (jump to the nearest indexed member), and forward seeks across already-
    indexed members.
    """

    def __init__(self, path: str | BinaryIO) -> None:
        # Initialise cursor state BEFORE super().__init__() because that call
        # invokes _create_decompressor(), which may reference these attributes.
        self._comp_cursor: int = 0  # compressed offset of the member being decoded
        self._decomp_cursor: int = 0  # decompressed offset of the member being decoded
        super().__init__(path)

    # ------------------------------------------------------------------
    # DecompressorStream abstract interface
    # ------------------------------------------------------------------

    def _create_decompressor(self, state: Any = None) -> _LzipState:
        return _LzipState()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        data, new_members = self._decompressor.feed(chunk)
        self._update_index(new_members)
        return data

    def _flush_decompressor(self) -> bytes:
        data, new_members = self._decompressor.flush()
        self._update_index(new_members)
        return data

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()

    # ------------------------------------------------------------------
    # Seek-point machinery
    # ------------------------------------------------------------------

    def _update_index(self, new_members: list[tuple[int, int]]) -> None:
        """Extend the seek-point table with newly completed members."""
        for decompressed_size, compressed_size in new_members:
            self.add_seek_points([SeekPoint(self._decomp_cursor, self._comp_cursor)])
            self._comp_cursor += compressed_size
            self._decomp_cursor += decompressed_size

    def _build_index(self, last_known: SeekPoint | None) -> None:
        """Scan member trailers backwards to build the complete index.

        On failure (e.g. trailing data after the last member, which is valid
        per the lzip spec §7), logs a warning and leaves the partial index
        intact so the base class falls back to sequential decompression.
        """
        saved = self._inner.tell()
        try:
            file_size = self._inner.seek(0, io.SEEK_END)
            members = _read_index_backwards(cast("BinaryIO", self._inner), file_size)
            self.add_seek_points(
                [SeekPoint(m.decompressed_start, m.compressed_start) for m in members]
            )
            self._size = sum(m.decompressed_size for m in members)
        except ArchiveCorruptedError as e:
            logger.warning(
                "Lzip backwards index scan failed (the file may have trailing "
                "data after the last member, which is valid per the lzip spec); "
                "falling back to sequential decompression. Reason: %s",
                e,
            )
        finally:
            self._inner.seek(saved)

    def _reset_to_seek_point(self, point: SeekPoint) -> None:
        self._comp_cursor = point.compressed_offset
        self._decomp_cursor = point.decompressed_offset
        super()._reset_to_seek_point(point)

    def _rewind(self) -> None:
        saved_size = self._size
        super()._rewind()  # _reset_to_seek_point(SeekPoint(0, 0)) + _size = None
        if self._index_built:
            self._size = saved_size


class ZlibDecompressorStream(DecompressorStream):
    def _create_decompressor(self, state: Any = None) -> "zlib._Decompress":
        return zlib.decompressobj()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.decompress(chunk)

    def _flush_decompressor(self) -> bytes:
        return self._decompressor.flush()

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.eof


class BrotliDecompressorStream(DecompressorStream):
    """Wrap a file-like object and decompress it using ``brotli``."""

    def _create_decompressor(self, state: Any = None) -> "brotli.Decompressor":
        return brotli.Decompressor()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.process(chunk)

    def _flush_decompressor(self) -> bytes:
        # brotli's decompressor doesn't have a flush method.
        return b""

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()
