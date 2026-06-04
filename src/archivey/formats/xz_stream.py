"""
Pure-stdlib XZ decompression with block-level seeking using Python's lzma module.

XZ file format spec: https://tukaani.org/xz/xz-file-format-1.0.4.txt

XZ file structure (one or more concatenated streams):

  Each stream:
    stream_header (12 bytes):
      magic         6 bytes   \\xfd7zXZ\\x00
      stream_flags  2 bytes   (check type in low 4 bits)
      CRC32         4 bytes

    [block ...]

    index (variable, multiple of 4 bytes):
      indicator     1 byte    0x00
      record_count  varint
      records       N x (unpadded_size: varint, uncompressed_size: varint)
      padding       0-3 bytes of zeros to reach 4-byte alignment
      CRC32         4 bytes

    stream_footer (12 bytes):
      CRC32              4 bytes
      backward_size      4 bytes LE   index_size = (backward_size + 1) * 4
      magic              2 bytes   "YZ"

  Each block:
    block_header (multiple of 4 bytes):
      header_size    1 byte   actual_size = (header_size + 1) * 4
      flags          1 byte   filter_count (bits 1-0) + has_comp (bit 6) + has_uncomp (bit 7)
      [comp_size     varint]  if has_comp
      [uncomp_size   varint]  if has_uncomp
      [filter specs  ...]     filter_count entries of (filter_id: varint, props_size: varint, props)
      padding        zeros to 4-byte alignment
      CRC32          4 bytes
    compressed data
    check (0/4/8/32 bytes depending on stream check type)
    block padding  0-3 zero bytes to round total block size to multiple of 4

  "unpadded_size" stored in the index = block_header_size + compressed_data_size + check_size.
  Actual padded block size = round_up(unpadded_size, 4).

Implementation notes:
  Block-level decompression uses lzma.FORMAT_RAW with the filter chain from the block
  header, decoded via lzma._decode_filter_properties (private but stable CPython API).
  This is the same pattern as constructing the synthetic LZMA_ALONE header for lzip.

  check_size is determined by stream_flags & 0x0f:
    0x00 -> 0 bytes (no check)
    0x01 -> 4 bytes (CRC32)
    0x04 -> 8 bytes (CRC64)
    0x0A -> 32 bytes (SHA-256)
"""

import lzma
import struct
import zlib
from dataclasses import dataclass
from typing import Any, BinaryIO

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError

_STREAM_HEADER_MAGIC = b"\xfd7zXZ\x00"
_STREAM_FOOTER_MAGIC = b"YZ"

_CHECK_SIZES: dict[int, int] = {0x00: 0, 0x01: 4, 0x04: 8, 0x0A: 32}
_DEFAULT_CHECK_SIZE = 4  # CRC32 is most common


@dataclass
class _XzBlockInfo:
    """Compressed/decompressed extents for one XZ block."""

    compressed_start: int  # absolute byte offset of block header in compressed stream
    decompressed_start: int  # cumulative decompressed bytes before this block
    block_header_size: int  # size of block header in bytes
    compressed_data_size: int  # size of raw compressed data (after header, before check)
    check_size: int  # size of check value (0, 4, 8, or 32 bytes)
    decompressed_size: int  # uncompressed size of this block
    filters: list[dict[str, Any]]  # lzma filter chain for FORMAT_RAW decompression

    @property
    def decompressed_end(self) -> int:
        return self.decompressed_start + self.decompressed_size

    def to_seek_state(self) -> dict[str, Any]:
        return {
            "block_header_size": self.block_header_size,
            "compressed_data_size": self.compressed_data_size,
            "check_size": self.check_size,
            "filters": self.filters,
        }


def _read_varint(data: bytes | bytearray, offset: int) -> tuple[int, int]:
    """Read an XZ variable-length integer from data at offset.

    Returns (value, new_offset).
    """
    value = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ArchiveEOFError("Truncated varint in XZ data")
        b = data[offset]
        offset += 1
        value |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
        if shift > 63:
            raise ArchiveCorruptedError("XZ varint too large (> 63-bit)")
    return value, offset


def _parse_block_header_bytes(header: bytes) -> tuple[list[dict[str, Any]], int | None]:
    """Parse an XZ block header (already fully buffered).

    Returns (filters, compressed_size_or_None).
    Raises ArchiveCorruptedError on invalid header.
    """
    if len(header) < 4:
        raise ArchiveCorruptedError("XZ block header too short")

    block_header_size = (header[0] + 1) * 4
    if len(header) != block_header_size:
        raise ArchiveCorruptedError(
            f"XZ block header buffer size {len(header)} != expected {block_header_size}"
        )

    stored_crc32 = struct.unpack_from("<I", header, block_header_size - 4)[0]
    computed_crc32 = zlib.crc32(header[: block_header_size - 4]) & 0xFFFFFFFF
    if stored_crc32 != computed_crc32:
        raise ArchiveCorruptedError(
            f"XZ block header CRC32 mismatch: stored {stored_crc32:#010x}, "
            f"computed {computed_crc32:#010x}"
        )

    flags = header[1]
    filter_count = (flags & 0x03) + 1
    has_compressed_size = bool(flags & 0x40)
    has_uncompressed_size = bool(flags & 0x80)

    offset = 2
    compressed_size: int | None = None
    if has_compressed_size:
        compressed_size, offset = _read_varint(header, offset)
    if has_uncompressed_size:
        _, offset = _read_varint(header, offset)

    filters: list[dict[str, Any]] = []
    for _ in range(filter_count):
        filter_id, offset = _read_varint(header, offset)
        props_size, offset = _read_varint(header, offset)
        if offset + props_size > block_header_size - 4:
            raise ArchiveCorruptedError("XZ filter properties extend past block header")
        props = bytes(header[offset : offset + props_size])
        offset += props_size
        try:
            # lzma._decode_filter_properties is private but stable in CPython.
            filter_dict = lzma._decode_filter_properties(filter_id, props)  # type: ignore[attr-defined]
        except (ValueError, lzma.LZMAError) as e:
            raise ArchiveCorruptedError(
                f"Unsupported or invalid XZ filter 0x{filter_id:x}: {e}"
            ) from e
        filters.append(filter_dict)

    return filters, compressed_size


def _parse_xz_block_header_from_stream(
    stream: BinaryIO, block_start: int
) -> tuple[int, list[dict[str, Any]], int | None]:
    """Read and parse an XZ block header from the stream at block_start.

    Returns (block_header_size, filters, compressed_size_or_None).
    """
    stream.seek(block_start)
    first_byte = stream.read(1)
    if not first_byte:
        raise ArchiveCorruptedError("Truncated XZ block header size byte")
    block_header_size = (first_byte[0] + 1) * 4

    stream.seek(block_start)
    header = stream.read(block_header_size)
    if len(header) < block_header_size:
        raise ArchiveCorruptedError("Truncated XZ block header")

    filters, compressed_size = _parse_block_header_bytes(bytes(header))
    return block_header_size, filters, compressed_size


def _read_xz_stream_index(
    stream: BinaryIO, index_offset: int, index_size: int
) -> list[tuple[int, int]]:
    """Read and parse an XZ stream index.

    Returns list of (unpadded_size, uncompressed_size) per block.
    """
    stream.seek(index_offset)
    index_data = stream.read(index_size)
    if len(index_data) < index_size:
        raise ArchiveCorruptedError("Truncated XZ stream index")

    if index_data[0] != 0x00:
        raise ArchiveCorruptedError(
            f"XZ index indicator byte is not 0x00: {index_data[0]:#x}"
        )

    offset = 1
    record_count, offset = _read_varint(index_data, offset)

    records: list[tuple[int, int]] = []
    for _ in range(record_count):
        unpadded_size, offset = _read_varint(index_data, offset)
        uncompressed_size, offset = _read_varint(index_data, offset)
        records.append((unpadded_size, uncompressed_size))

    return records


def _read_xz_index_backwards(
    stream: BinaryIO,
    file_size: int,
    stop_at: int = 0,
    start_decompressed_offset: int = 0,
) -> list[_XzBlockInfo]:
    """Build the block index by scanning XZ streams backwards.

    Reads only the stream footers, stream indexes, and block headers — no
    decompression is performed.  Supports multi-stream XZ files and stream
    padding (4-byte-aligned zero bytes between streams).

    stop_at: stop scanning when the current stream would start at or before
      this compressed offset.
    start_decompressed_offset: cumulative decompressed bytes before the first
      scanned block.

    Returns all blocks in forward order with correct decompressed_start values.
    Raises ArchiveCorruptedError or ArchiveEOFError on corrupt/truncated data.
    """
    streams_reversed: list[list[_XzBlockInfo]] = []
    compressed_end = file_size

    while compressed_end > stop_at:
        if compressed_end < 12:
            raise ArchiveCorruptedError("XZ file too small for a stream footer")

        # Skip trailing stream padding (4-byte-aligned zero blocks between streams)
        while compressed_end >= 4:
            stream.seek(compressed_end - 4)
            last4 = stream.read(4)
            if last4 == b"\x00\x00\x00\x00":
                compressed_end -= 4
                if compressed_end <= stop_at:
                    break
            else:
                break

        if compressed_end - stop_at < 12:
            break

        # Read stream footer
        stream.seek(compressed_end - 12)
        footer = stream.read(12)
        if len(footer) < 12:
            raise ArchiveCorruptedError("Truncated XZ stream footer")

        if footer[-2:] != _STREAM_FOOTER_MAGIC:
            raise ArchiveCorruptedError(
                f"XZ stream footer magic not found before offset {compressed_end}: "
                f"got {footer[-2:]!r}"
            )

        backward_size_field = struct.unpack("<I", footer[4:8])[0]
        index_size = (backward_size_field + 1) * 4
        stream_flags = struct.unpack(">H", footer[8:10])[0]
        check_size = _CHECK_SIZES.get(stream_flags & 0x0F, _DEFAULT_CHECK_SIZE)

        if compressed_end < 12 + index_size:
            raise ArchiveCorruptedError("XZ stream index extends before start of file")

        index_offset = compressed_end - 12 - index_size
        records = _read_xz_stream_index(stream, index_offset, index_size)

        # Compute total padded size of all blocks in this stream
        stream_data_size = sum((unp + 3) & ~3 for unp, _ in records)
        stream_start = index_offset - stream_data_size - 12  # 12-byte stream header

        if stream_start < 0:
            raise ArchiveCorruptedError(
                f"XZ stream data size {stream_data_size} puts stream start at "
                f"negative offset {stream_start}"
            )

        # Verify stream header magic
        stream.seek(stream_start)
        magic = stream.read(6)
        if magic != _STREAM_HEADER_MAGIC:
            raise ArchiveCorruptedError(
                f"XZ stream header magic not found at {stream_start}: got {magic!r}"
            )

        # Parse each block header to obtain the filter chain
        block_offset = stream_start + 12
        stream_blocks: list[_XzBlockInfo] = []

        for unpadded_size, uncomp_size in records:
            block_header_size, filters, _ = _parse_xz_block_header_from_stream(
                stream, block_offset
            )
            compressed_data_size = unpadded_size - block_header_size - check_size
            if compressed_data_size < 0:
                raise ArchiveCorruptedError(
                    f"XZ block at {block_offset}: negative compressed data size "
                    f"(unpadded={unpadded_size}, header={block_header_size}, check={check_size})"
                )
            stream_blocks.append(
                _XzBlockInfo(
                    compressed_start=block_offset,
                    decompressed_start=0,  # filled in the forward pass below
                    block_header_size=block_header_size,
                    compressed_data_size=compressed_data_size,
                    check_size=check_size,
                    decompressed_size=uncomp_size,
                    filters=filters,
                )
            )
            block_offset += (unpadded_size + 3) & ~3

        streams_reversed.append(stream_blocks)
        compressed_end = stream_start

    # Assign decompressed_start values in forward order
    result: list[_XzBlockInfo] = []
    decompressed_cursor = start_decompressed_offset
    for stream_blocks in reversed(streams_reversed):
        for block in stream_blocks:
            block.decompressed_start = decompressed_cursor
            decompressed_cursor += block.decompressed_size
        result.extend(stream_blocks)

    return result


# ---------------------------------------------------------------------------
# Streaming state machine
# ---------------------------------------------------------------------------


class _XzState:
    """Streaming state machine for multi-block, multi-stream XZ decompression.

    Two construction modes:

    1. Default (_XzState()): starts at the beginning of an XZ stream
       (NEED_STREAM_HEADER state).  Handles full stream framing including
       stream header, block headers, index, and stream footer.  Supports
       multiple concatenated streams.

    2. Seek mode (_XzState(seek_state=..., initial_compressed_offset=...)):
       starts mid-stream at a known block.  The block header bytes are
       skipped (its size is in seek_state), and the block's compressed data
       is decompressed with the stored filter chain.  After the block, the
       state machine continues normally (reads subsequent block headers or
       skips the index/footer).

    feed(chunk) returns (decompressed_bytes, new_blocks) where new_blocks is
    a list of dicts with keys:
      decomp_size, header_abs_start, block_header_size,
      compressed_data_size, check_size, filters
    for each block whose data was fully produced in this call.

    flush() succeeds cleanly when between streams (NEED_STREAM_HEADER with
    >= 1 block seen) and raises ArchiveEOFError if the stream is truncated.
    """

    _NEED_STREAM_HEADER = 0
    _NEED_BLOCK_HEADER = 1
    _IN_BLOCK = 2
    _SKIP_CHECK_AND_PADDING = 3
    _SKIP_INDEX = 4
    _NEED_STREAM_FOOTER = 5
    _SKIP_BLOCK_HEADER = 6  # initial state when starting from a seek point

    def __init__(
        self,
        initial_compressed_offset: int = 0,
        seek_state: dict[str, Any] | None = None,
    ) -> None:
        self._buf = bytearray()
        self._finished = False
        self._blocks_seen = 0
        self._streams_seen = 0
        self._consumed = 0
        self._initial_compressed_offset = initial_compressed_offset

        # Active block decompressor
        self._dec: lzma.LZMADecompressor | None = None
        self._stream_check_size = _DEFAULT_CHECK_SIZE

        # Per-block tracking
        self._current_block_abs_start = 0
        self._current_block_header_size = 0
        self._current_block_compressed_data_size = 0
        self._current_block_decomp_size = 0
        self._current_block_filters: list[dict[str, Any]] = []
        self._compressed_data_remaining: int | None = None

        # Generic byte-skip counter (check + block padding, or block header)
        self._skip_remaining = 0

        # Index-skip sub-state
        self._index_bytes_consumed = 0
        self._index_count: int | None = None
        self._index_varints_remaining = 0

        if seek_state is not None:
            self._state = self._SKIP_BLOCK_HEADER
            self._skip_remaining = seek_state["block_header_size"]
            self._current_block_abs_start = initial_compressed_offset
            self._current_block_header_size = seek_state["block_header_size"]
            self._current_block_filters = seek_state["filters"]
            self._compressed_data_remaining = seek_state["compressed_data_size"]
            self._stream_check_size = seek_state["check_size"]
        else:
            self._state = self._NEED_STREAM_HEADER

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def feed(self, data: bytes) -> tuple[bytes, list[dict[str, Any]]]:
        self._buf.extend(data)
        return self._process()

    def flush(self) -> tuple[bytes, list[dict[str, Any]]]:
        if self._state == self._NEED_STREAM_HEADER:
            if self._blocks_seen == 0 and self._streams_seen == 0:
                raise ArchiveCorruptedError("Not a valid XZ file: no blocks found")
            self._finished = True
            return b"", []
        raise ArchiveEOFError("XZ file is truncated")

    def is_finished(self) -> bool:
        return self._finished

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _consume(self, n: int) -> None:
        del self._buf[:n]
        self._consumed += n

    def _abs_offset(self) -> int:
        """Absolute position in the compressed file (of the next unread byte)."""
        return self._initial_compressed_offset + self._consumed

    def _enter_skip_check_and_padding(self) -> None:
        """Compute and record how many bytes to skip (check + block padding)."""
        unpadded = (
            self._current_block_header_size
            + self._current_block_compressed_data_size
            + self._stream_check_size
        )
        padded = (unpadded + 3) & ~3
        self._skip_remaining = self._stream_check_size + (padded - unpadded)
        self._state = self._SKIP_CHECK_AND_PADDING

    # ------------------------------------------------------------------
    # Main processing loop
    # ------------------------------------------------------------------

    def _process(self) -> tuple[bytes, list[dict[str, Any]]]:
        output = bytearray()
        new_blocks: list[dict[str, Any]] = []

        while True:
            # ---- SKIP_BLOCK_HEADER (seek mode: skip the known block header) ----
            if self._state == self._SKIP_BLOCK_HEADER:
                to_skip = min(self._skip_remaining, len(self._buf))
                self._consume(to_skip)
                self._skip_remaining -= to_skip
                if self._skip_remaining == 0:
                    self._dec = lzma.LZMADecompressor(
                        format=lzma.FORMAT_RAW, filters=self._current_block_filters
                    )
                    self._current_block_decomp_size = 0
                    self._current_block_compressed_data_size = 0
                    self._state = self._IN_BLOCK
                else:
                    break

            # ---- NEED_STREAM_HEADER ----
            elif self._state == self._NEED_STREAM_HEADER:
                # Skip stream padding (zero bytes between concatenated streams)
                while self._buf and self._buf[0] == 0x00:
                    self._consume(1)

                if len(self._buf) < 12:
                    break

                header = bytes(self._buf[:12])
                self._consume(12)

                if header[:6] != _STREAM_HEADER_MAGIC:
                    if self._blocks_seen > 0:
                        # Unrecognised data after a valid stream → treat as trailing
                        self._finished = True
                        break
                    raise ArchiveCorruptedError(
                        f"Not a valid XZ stream header: magic {header[:6]!r}"
                    )
                stream_flags = struct.unpack(">H", header[6:8])[0]
                self._stream_check_size = _CHECK_SIZES.get(
                    stream_flags & 0x0F, _DEFAULT_CHECK_SIZE
                )
                self._state = self._NEED_BLOCK_HEADER

            # ---- NEED_BLOCK_HEADER ----
            elif self._state == self._NEED_BLOCK_HEADER:
                if not self._buf:
                    break
                first_byte = self._buf[0]

                if first_byte == 0x00:
                    # Index indicator byte
                    self._consume(1)
                    self._index_bytes_consumed = 1
                    self._index_count = None
                    self._index_varints_remaining = 0
                    self._state = self._SKIP_INDEX
                    continue

                block_header_size = (first_byte + 1) * 4
                if len(self._buf) < block_header_size:
                    break

                # Record abs start before consuming
                block_abs_start = self._abs_offset()
                header_bytes = bytes(self._buf[:block_header_size])
                self._consume(block_header_size)

                try:
                    filters, compressed_size = _parse_block_header_bytes(header_bytes)
                except Exception as e:
                    raise ArchiveCorruptedError(
                        f"Invalid XZ block header at {block_abs_start}: {e}"
                    ) from e

                self._current_block_abs_start = block_abs_start
                self._current_block_header_size = block_header_size
                self._current_block_filters = filters
                self._current_block_decomp_size = 0
                self._current_block_compressed_data_size = 0
                self._compressed_data_remaining = compressed_size

                self._dec = lzma.LZMADecompressor(
                    format=lzma.FORMAT_RAW, filters=filters
                )
                self._state = self._IN_BLOCK

            # ---- IN_BLOCK ----
            elif self._state == self._IN_BLOCK:
                if not self._buf:
                    break

                if self._compressed_data_remaining == 0:
                    self._enter_skip_check_and_padding()
                    continue

                if self._compressed_data_remaining is not None:
                    chunk = bytes(self._buf[: self._compressed_data_remaining])
                else:
                    chunk = bytes(self._buf)
                self._consume(len(chunk))

                assert self._dec is not None
                try:
                    plain = self._dec.decompress(chunk)
                except lzma.LZMAError as e:
                    raise ArchiveCorruptedError(
                        f"XZ block decompression error at {self._current_block_abs_start}: {e}"
                    ) from e

                if plain:
                    output.extend(plain)
                    self._current_block_decomp_size += len(plain)

                if self._compressed_data_remaining is not None:
                    self._current_block_compressed_data_size += len(chunk)
                    self._compressed_data_remaining -= len(chunk)
                    if self._compressed_data_remaining == 0:
                        self._enter_skip_check_and_padding()
                elif self._dec.eof:
                    # LZMA2 EOS detected; unused_data is the check + possible next data
                    unused = self._dec.unused_data
                    self._current_block_compressed_data_size += len(chunk) - len(unused)
                    # Put unused bytes back
                    self._buf[0:0] = unused
                    self._consumed -= len(unused)
                    self._enter_skip_check_and_padding()

            # ---- SKIP_CHECK_AND_PADDING ----
            elif self._state == self._SKIP_CHECK_AND_PADDING:
                to_skip = min(self._skip_remaining, len(self._buf))
                self._consume(to_skip)
                self._skip_remaining -= to_skip
                if self._skip_remaining == 0:
                    new_blocks.append(
                        {
                            "decomp_size": self._current_block_decomp_size,
                            "header_abs_start": self._current_block_abs_start,
                            "block_header_size": self._current_block_header_size,
                            "compressed_data_size": self._current_block_compressed_data_size,
                            "check_size": self._stream_check_size,
                            "filters": self._current_block_filters,
                        }
                    )
                    self._blocks_seen += 1
                    self._dec = None
                    self._state = self._NEED_BLOCK_HEADER

            # ---- SKIP_INDEX ----
            elif self._state == self._SKIP_INDEX:
                if self._do_skip_index():
                    self._state = self._NEED_STREAM_FOOTER
                else:
                    break

            # ---- NEED_STREAM_FOOTER ----
            elif self._state == self._NEED_STREAM_FOOTER:
                if len(self._buf) < 12:
                    break
                footer = bytes(self._buf[:12])
                self._consume(12)
                if footer[-2:] != _STREAM_FOOTER_MAGIC:
                    raise ArchiveCorruptedError(
                        f"Invalid XZ stream footer magic: {footer[-2:]!r}"
                    )
                self._streams_seen += 1
                self._state = self._NEED_STREAM_HEADER

            else:  # pragma: no cover
                raise RuntimeError(f"Unknown _XzState state: {self._state}")

        return bytes(output), new_blocks

    def _do_skip_index(self) -> bool:
        """Consume index bytes from _buf.  Returns True when fully skipped."""
        if self._index_count is None:
            # Read the record-count varint
            off = 0
            val = 0
            shift = 0
            found = False
            while off < len(self._buf):
                b = self._buf[off]
                off += 1
                val |= (b & 0x7F) << shift
                if not (b & 0x80):
                    found = True
                    break
                shift += 7
            if not found:
                return False
            self._consume(off)
            self._index_bytes_consumed += off
            self._index_count = val
            self._index_varints_remaining = val * 2  # 2 varints per record

        # Skip the record varints one at a time
        while self._index_varints_remaining > 0:
            if not self._buf:
                return False
            off = 0
            found = False
            while off < len(self._buf):
                b = self._buf[off]
                off += 1
                if not (b & 0x80):
                    found = True
                    break
            if not found:
                return False
            self._consume(off)
            self._index_bytes_consumed += off
            self._index_varints_remaining -= 1

        # Padding to align index size to a multiple of 4 bytes
        padding = (-self._index_bytes_consumed) % 4
        if len(self._buf) < padding:
            return False
        self._consume(padding)

        # CRC32 (4 bytes)
        if len(self._buf) < 4:
            return False
        self._consume(4)

        return True
