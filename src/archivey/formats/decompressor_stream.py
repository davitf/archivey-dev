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
from archivey.formats.xz_stream import (
    _read_xz_index_backwards,
    _XzBlockBounds,
    _XzBlockChain,
    _XzState,
)
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
    """

    def __init__(self, path: str | BinaryIO) -> None:
        super().__init__()
        if isinstance(path, (str, bytes, os.PathLike)):
            self._inner = open(path, "rb")
            self._should_close = True
        else:
            self._inner = ensure_bufferedio(path)
            self._should_close = False
        self._seek_points: list[SeekPoint] = [SeekPoint(0, 0)]
        self._index_built: bool = False
        self._index_build_attempted: bool = False
        self._decompressor: DecompressorT = self._create_decompressor(
            self._seek_points[0]
        )
        self._buffer = bytearray()
        self._eof = False
        self._pos = 0
        self._size: int | None = None

    @abc.abstractmethod
    def _create_decompressor(self, point: SeekPoint) -> DecompressorT: ...

    @abc.abstractmethod
    def _decompress_chunk(self, chunk: bytes) -> bytes: ...

    @abc.abstractmethod
    def _flush_decompressor(self) -> bytes:
        """Flush pending data from the decompressor and return it.

        Called exactly once when the compressed input is exhausted.  Most
        decompressors decode eagerly and return b"" here.  zlib is an
        exception: its flush() processes all remaining buffered input and
        returns the last portion of decompressed data.  After this call the
        decompressor must not be used again.
        """
        ...

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
            if point < self._seek_points[-1]:
                # Out-of-order (should be rare): bisect insert
                i = bisect.bisect_left(self._seek_points, point)
                if i < len(self._seek_points) and self._seek_points[i] == point:
                    continue  # duplicate
                self._seek_points.insert(i, point)
            elif self._seek_points[-1] == point:
                continue  # duplicate
            else:
                self._seek_points.append(point)  # fast path: in-order append

    def _find_best_seek_point(self, pos: int) -> SeekPoint:
        """Return the last seek point with decompressed_offset <= pos."""
        # i >= 0 because SeekPoint(0, 0) is always the first entry and pos >= 0
        i = bisect.bisect_right(self._seek_points, SeekPoint(pos, 0)) - 1
        return self._seek_points[i]

    def _reset_to_seek_point(self, point: SeekPoint) -> None:
        """Jump to a known seek point.  Does not touch _size."""
        self._inner.seek(point.compressed_offset)
        self._decompressor = self._create_decompressor(point)
        self._buffer.clear()
        self._eof = False
        self._pos = point.decompressed_offset

    def _build_index(self, last_known: SeekPoint) -> tuple[list[SeekPoint], int | None]:
        """One-shot full index build.  Default: no-op, returns empty list and no size.

        Subclasses that support random access override this to return a list of
        new seek points and the total decompressed size (or None if unknown).
        Called at most once per stream (guarded by _index_built in seek()).

        last_known: the highest-offset SeekPoint currently in _seek_points.
        Subclasses may use it to start forward indexing from there or to stop
        a backwards scan at that point.

        The inner stream's position after this call is unspecified; seek()
        repositions it via _reset_to_seek_point as needed.
        """
        return [], None

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

    def readall(self) -> bytes:
        while not self._eof:
            self._buffer.extend(self._read_decompressed_chunk())

        data = bytes(self._buffer)
        self._buffer.clear()
        # _pos may be past _size when the caller seeked beyond EOF; in that case
        # the buffer is already empty and we must not overwrite _size with _pos.
        if self._size is None or self._pos <= self._size:
            self._pos += len(data)
            self._size = self._pos
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

    def _ensure_index_built(self) -> None:
        if self._index_built or self._index_build_attempted:
            return

        inner_pos = self._inner.tell()
        new_points, new_size = self._build_index(self._seek_points[-1])
        self._index_build_attempted = True
        if new_points or new_size is not None:
            self._index_built = True

        if new_points:
            self.add_seek_points(new_points)
        if new_size is not None:
            self._size = new_size

        # _build_index may seek _inner for index reads (e.g. lzip's
        # backward trailer scan); restore it so the decompressor's
        # expected read position is still valid.
        if self._inner.tell() != inner_pos:
            self._inner.seek(inner_pos)

    def try_get_size(self) -> int | None:
        """Return the total decompressed size if cheaply available, else None.

        Attempts to build the index (backward scan) to learn the size without
        falling back to decompressing the whole stream.  Safe to call on open;
        returns None rather than blocking if the index scan fails.
        """
        if self._size is not None:
            return self._size
        if not self._inner.seekable():
            return None
        self._ensure_index_built()
        return self._size

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if not self._inner.seekable():
            raise io.UnsupportedOperation("seek")

        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = -1  # sentinel; resolved below after determining _size
        else:
            raise ValueError(f"Invalid whence: {whence}")

        # Build the index if we're seeking from the end (as we need to know the
        # total decompressed size) or to a target posiiton after the current
        # buffer end and after the last known seek point.
        if whence == io.SEEK_END or (
            new_pos > self._pos + len(self._buffer)
            and new_pos > self._seek_points[-1].decompressed_offset
        ):
            self._ensure_index_built()

        if whence == io.SEEK_END:
            if self._size is None:
                # If we don't know the stream size (building the index above
                # doesn't always provide it), scan to EOF to discover it.
                # We don't use readall() to avoid buffering all the remaining
                # data in RAM.
                self._pos += len(self._buffer)
                self._buffer.clear()
                while not self._eof:
                    data = self._read_decompressed_chunk()
                    self._pos += len(data)
                # _read_decompressed_chunk() sets _size when it reaches the
                # end of the stream.
                assert self._size is not None

            new_pos = self._size + offset

        if new_pos < 0:
            raise ValueError(f"Invalid offset: {offset}")

        # Short-circuit when past EOF
        if self._size is not None and new_pos >= self._size:
            self._buffer.clear()
            self._eof = True
            self._pos = new_pos
            return self._pos

        if new_pos == self._pos:
            return self._pos

        if new_pos < self._pos:
            # Backward seek: jump to the nearest seek point, then forward-read
            self._reset_to_seek_point(self._find_best_seek_point(new_pos))
        elif new_pos <= self._pos + len(self._buffer):
            # Target is inside the current look-ahead buffer
            del self._buffer[: new_pos - self._pos]
            self._pos = new_pos
            return self._pos
        else:
            # Forward seek past the current buffer: jump to a closer seek point
            # if one exists, otherwise advance past the buffered data.
            best = self._find_best_seek_point(new_pos)
            if best.decompressed_offset > self._pos:
                self._reset_to_seek_point(best)
            else:
                self._pos += len(self._buffer)
                self._buffer.clear()

        # Buffer is empty after any reset or explicit clear above.
        assert not self._buffer
        if self._pos == new_pos:
            return self._pos

        # Forward-read loop (handles tail of backward seeks and forward jumps)
        while not self._eof:
            decompressed = self._read_decompressed_chunk()
            if self._pos + len(decompressed) >= new_pos:
                self._buffer.extend(decompressed[new_pos - self._pos :])
                self._pos = new_pos
                return self._pos
            self._pos += len(decompressed)

        # Past EOF
        self._pos = new_pos
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
        # Pre-declare cursor attributes so pyright knows they exist; they are
        # set to their correct values by _create_decompressor(SeekPoint(0, 0))
        # called from super().__init__().
        self._comp_cursor: int = 0
        self._decomp_cursor: int = 0
        super().__init__(path)

    # ------------------------------------------------------------------
    # DecompressorStream abstract interface
    # ------------------------------------------------------------------

    def _create_decompressor(self, point: SeekPoint) -> _LzipState:
        self._comp_cursor = point.compressed_offset
        self._decomp_cursor = point.decompressed_offset
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

    def _build_index(self, last_known: SeekPoint) -> tuple[list[SeekPoint], int | None]:
        """Scan member trailers backwards to build the complete index.

        Starts the scan at the file end and stops at last_known.compressed_offset,
        covering only the portion not yet indexed by forward decompression.

        On failure (e.g. trailing data after the last member, which is valid
        per the lzip spec §7), logs a warning and returns empty results so the
        base class falls back to sequential decompression.

        The inner stream's position after this call is unspecified.
        """
        file_size = self._inner.seek(0, io.SEEK_END)
        try:
            members = _read_index_backwards(
                cast("BinaryIO", self._inner),
                file_size,
                stop_at=last_known.compressed_offset,
                start_decompressed_offset=last_known.decompressed_offset,
            )
            points = [
                SeekPoint(m.decompressed_start, m.compressed_start) for m in members
            ]
            total_size = (
                members[-1].decompressed_start + members[-1].decompressed_size
                if members
                else None
            )
            return points, total_size
        except ArchiveCorruptedError as e:
            logger.warning(
                "Lzip backwards index scan failed (the file may have trailing "
                "data after the last member, which is valid per the lzip spec); "
                "falling back to sequential decompression. Reason: %s",
                e,
            )
            return [], None


class XzDecompressorStream(DecompressorStream["_XzState | _XzBlockChain"]):
    """Seekable XZ decompressor backed by Python's stdlib lzma.

    Builds a block-level seek-point table:
      - Progressively via _update_index() as streams complete during forward reads.
      - On-demand via _build_index() (triggered by SEEK_END or forward seek past frontier).

    SeekPoint.state == None  →  stream-level decompressor (_XzState), used only for the
                                initial SeekPoint(0, 0) before any index is known.
    SeekPoint.state == tuple →  block metadata (check, unpadded_size, uncompressed_size);
                                uses _XzBlockChain for block-level decompression.
    """

    def __init__(self, path: "str | BinaryIO") -> None:
        self._comp_cursor: int = 0
        self._decomp_cursor: int = 0
        super().__init__(path)

    def _create_decompressor(self, point: SeekPoint) -> "_XzState | _XzBlockChain":
        self._comp_cursor = point.compressed_offset
        self._decomp_cursor = point.decompressed_offset
        if point.state is None:
            return _XzState()
        # Block-level: collect this point and all subsequent block-level points
        check, unpadded_size, uncompressed_size = point.state
        start_block = _XzBlockBounds(
            compressed_start=point.compressed_offset,
            decompressed_start=point.decompressed_offset,
            unpadded_size=unpadded_size,
            uncompressed_size=uncompressed_size,
            check=check,
        )
        # Gather all subsequent block-level seek points
        subsequent = [
            sp for sp in self._seek_points
            if sp.decompressed_offset > point.decompressed_offset and sp.state is not None
        ]
        extra_blocks = [
            _XzBlockBounds(
                compressed_start=sp.compressed_offset,
                decompressed_start=sp.decompressed_offset,
                unpadded_size=sp.state[1],
                uncompressed_size=sp.state[2],
                check=sp.state[0],
            )
            for sp in subsequent
        ]
        blocks = [start_block] + extra_blocks
        return _XzBlockChain(blocks, cast("BinaryIO", self._inner))

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        result = self._decompressor.feed(chunk)
        data, new_streams = result
        if isinstance(self._decompressor, _XzState):
            self._update_index(new_streams)
        else:
            # _XzBlockChain: update cursors only
            for decomp_size, comp_size in new_streams:
                self._comp_cursor += comp_size
                self._decomp_cursor += decomp_size
        return data

    def _flush_decompressor(self) -> bytes:
        data, new_streams = self._decompressor.flush()
        if isinstance(self._decompressor, _XzState):
            self._update_index(new_streams)
        else:
            for decomp_size, comp_size in new_streams:
                self._comp_cursor += comp_size
                self._decomp_cursor += decomp_size
        return data

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()

    def _update_index(self, new_streams: list[tuple[int, int]]) -> None:
        """Extend seek points with newly completed streams.

        For each completed stream: adds a stream-level SeekPoint (so the stream
        can be re-entered from its start), then immediately scans that stream's
        compressed range backwards to populate block-level seek points.
        """
        for decompressed_size, compressed_size in new_streams:
            stream_comp_start = self._comp_cursor
            stream_decomp_start = self._decomp_cursor

            # Add stream-level seek point (skip for stream 0 — SeekPoint(0,0) covers it)
            if stream_decomp_start > 0:
                self.add_seek_points(
                    [SeekPoint(stream_decomp_start, stream_comp_start, state=None)]
                )

            stream_comp_end = stream_comp_start + compressed_size

            # Per-stream backward scan to populate block-level seek points
            if not self._index_built and self._inner.seekable():
                saved_pos = self._inner.tell()
                try:
                    blocks = _read_xz_index_backwards(
                        cast("BinaryIO", self._inner),
                        stream_comp_end,
                        stop_at=stream_comp_start,
                        start_decompressed_offset=stream_decomp_start,
                    )
                    block_points = [
                        SeekPoint(
                            b.decompressed_start,
                            b.compressed_start,
                            state=(b.check, b.unpadded_size, b.uncompressed_size),
                        )
                        for b in blocks
                        if b.decompressed_start > 0  # skip block 0 of stream 0 duplicate
                    ]
                    if block_points:
                        self.add_seek_points(block_points)
                except ArchiveCorruptedError as e:
                    logger.warning(
                        "XZ per-stream backward scan failed, block-level seek points "
                        "for this stream will not be available: %s",
                        e,
                    )
                finally:
                    self._inner.seek(saved_pos)

            self._comp_cursor = stream_comp_end
            self._decomp_cursor += decompressed_size

    def _build_index(
        self, last_known: SeekPoint
    ) -> tuple[list[SeekPoint], int | None]:
        """Full backwards scan from EOF to last_known, building block seek points."""
        file_size = self._inner.seek(0, io.SEEK_END)
        try:
            blocks = _read_xz_index_backwards(
                cast("BinaryIO", self._inner),
                file_size,
                stop_at=last_known.compressed_offset,
                start_decompressed_offset=last_known.decompressed_offset,
            )
        except ArchiveCorruptedError as e:
            logger.warning(
                "XZ backwards index scan failed; falling back to sequential "
                "decompression. Reason: %s",
                e,
            )
            return [], None

        points = [
            SeekPoint(
                b.decompressed_start,
                b.compressed_start,
                state=(b.check, b.unpadded_size, b.uncompressed_size),
            )
            for b in blocks
            if b.decompressed_start > 0  # skip duplicate of SeekPoint(0,0)
        ]
        total_size = (
            blocks[-1].decompressed_start + blocks[-1].uncompressed_size
            if blocks
            else None
        )
        return points, total_size


class ZlibDecompressorStream(DecompressorStream):
    def _create_decompressor(self, point: SeekPoint) -> "zlib._Decompress":
        return zlib.decompressobj()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.decompress(chunk)

    def _flush_decompressor(self) -> bytes:
        return self._decompressor.flush()

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.eof


class BrotliDecompressorStream(DecompressorStream):
    """Wrap a file-like object and decompress it using ``brotli``."""

    def _create_decompressor(self, point: SeekPoint) -> "brotli.Decompressor":
        return brotli.Decompressor()

    def _decompress_chunk(self, chunk: bytes) -> bytes:
        return self._decompressor.process(chunk)

    def _flush_decompressor(self) -> bytes:
        # brotli's decompressor doesn't have a flush method.
        return b""

    def _is_decompressor_finished(self) -> bool:
        return self._decompressor.is_finished()
