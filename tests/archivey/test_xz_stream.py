"""Tests for XzDecompressorStream: multi-block files and efficient seeking."""

import io
import lzma
import subprocess
import sys
from typing import BinaryIO, cast

import pytest

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError
from archivey.formats.compressed_streams import _translate_xz_stream_exception
from archivey.formats.decompressor_stream import XzDecompressorStream, SeekPoint
from archivey.formats.xz_stream import _read_xz_index_backwards

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_XZ_CLI_AVAILABLE = bool(subprocess.run(
    ["xz", "--version"],
    capture_output=True,
    timeout=5,
).returncode == 0)

requires_xz_cli = pytest.mark.skipif(
    not _XZ_CLI_AVAILABLE,
    reason="xz CLI not available",
)


def make_xz(data: bytes) -> bytes:
    """Single-stream, single-block XZ using stdlib lzma (no CLI)."""
    return lzma.compress(data, format=lzma.FORMAT_XZ)


def make_xz_multiblock(data: bytes, block_size: int) -> bytes:
    """Multi-block XZ file with the given block size (bytes), using xz CLI."""
    result = subprocess.run(
        ["xz", "-c", f"--block-size={block_size}", "-"],
        input=data,
        capture_output=True,
        timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"xz failed: {result.stderr.decode()}")
    return result.stdout


def make_xz_multistream(parts: list[bytes]) -> bytes:
    """Concatenate multiple independent XZ streams."""
    return b"".join(lzma.compress(p, format=lzma.FORMAT_XZ) for p in parts)


class _NonSeekableStream(io.RawIOBase):
    """Wraps BytesIO but refuses to seek."""

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._inner = io.BytesIO(data)

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        data = self._inner.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def seekable(self) -> bool:
        return False

    def seek(self, *args, **kwargs):
        raise io.UnsupportedOperation("seek")


class _CountingStream(io.RawIOBase):
    """Wraps BytesIO and counts total bytes read."""

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._inner = io.BytesIO(data)
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        n = self._inner.readinto(b)
        if n:
            self.bytes_read += n
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def seekable(self) -> bool:
        return True


def open_xz(data: bytes) -> XzDecompressorStream:
    return XzDecompressorStream(io.BytesIO(data))


# ---------------------------------------------------------------------------
# Basic reading
# ---------------------------------------------------------------------------


def test_single_block_read_all():
    payload = b"hello world"
    with open_xz(make_xz(payload)) as f:
        assert f.read() == payload


def test_single_block_read_chunked():
    payload = b"abcdefghij" * 100
    xz_data = make_xz(payload)
    with open_xz(xz_data) as f:
        result = bytearray()
        while True:
            chunk = f.read(64)
            if not chunk:
                break
            result.extend(chunk)
    assert bytes(result) == payload


def test_empty_payload_single_block():
    """An XZ file with zero blocks (empty payload) must decompress to empty bytes."""
    payload = b""
    xz_data = make_xz(payload)
    with open_xz(xz_data) as f:
        assert f.read() == payload


@requires_xz_cli
def test_multi_block_read_all():
    payload = b"x" * 12000
    xz_data = make_xz_multiblock(payload, block_size=4000)
    with open_xz(xz_data) as f:
        assert f.read() == payload


@requires_xz_cli
def test_multi_block_read_chunked():
    parts = [b"A" * 3000, b"B" * 3000, b"C" * 3000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=3000)
    with open_xz(xz_data) as f:
        result = bytearray()
        while True:
            chunk = f.read(256)
            if not chunk:
                break
            result.extend(chunk)
    assert bytes(result) == payload


def test_multi_stream_read():
    parts = [b"first stream", b"second stream", b"third stream"]
    xz_data = make_xz_multistream(parts)
    with open_xz(xz_data) as f:
        assert f.read() == b"".join(parts)


# ---------------------------------------------------------------------------
# Forward seeking
# ---------------------------------------------------------------------------


def test_single_block_seek_forward():
    payload = b"abcdefghij"
    with open_xz(make_xz(payload)) as f:
        f.seek(3)
        assert f.read(4) == b"defg"


@requires_xz_cli
def test_multi_block_seek_forward_into_later_block():
    parts = [b"A" * 4000, b"B" * 4000, b"C" * 4000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=4000)
    with open_xz(xz_data) as f:
        f.seek(8000)
        assert f.read(4000) == b"C" * 4000


def test_multi_stream_seek_forward_into_later_stream():
    parts = [b"AAAA", b"BBBB", b"CCCC"]
    xz_data = make_xz_multistream(parts)
    with open_xz(xz_data) as f:
        f.seek(8)
        assert f.read(4) == b"CCCC"


# ---------------------------------------------------------------------------
# Backward seeking
# ---------------------------------------------------------------------------


def test_single_block_seek_backward():
    payload = b"abcdefghij"
    with open_xz(make_xz(payload)) as f:
        f.read()
        f.seek(3)
        assert f.read(4) == b"defg"


@requires_xz_cli
def test_multi_block_seek_backward_to_earlier_block():
    parts = [b"A" * 4000, b"B" * 4000, b"C" * 4000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=4000)
    with open_xz(xz_data) as f:
        f.read()  # read all (builds index)
        f.seek(4000)
        assert f.read(4000) == b"B" * 4000


@requires_xz_cli
def test_backward_seek_uses_index_not_position_zero():
    """After reading all blocks, seeking backward to block 1 jumps directly there."""
    parts = [b"a" * 5000, b"b" * 5000, b"c" * 5000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=5000)
    with open_xz(xz_data) as f:
        f.read()
        assert len(f._seek_points) >= 3  # type: ignore[attr-defined]

        target = len(parts[0])
        f.seek(target)
        assert f._decomp_cursor == target  # type: ignore[attr-defined]
        assert f.read(len(parts[1])) == parts[1]


def test_multi_stream_seek_backward():
    parts = [b"AAAA", b"BBBB", b"CCCC"]
    xz_data = make_xz_multistream(parts)
    with open_xz(xz_data) as f:
        f.read()
        f.seek(0)
        assert f.read(4) == b"AAAA"
        f.seek(4)
        assert f.read(4) == b"BBBB"


# ---------------------------------------------------------------------------
# SEEK_END
# ---------------------------------------------------------------------------


def test_seek_end_returns_correct_size_single_block():
    payload = b"hello world"
    with open_xz(make_xz(payload)) as f:
        size = f.seek(0, io.SEEK_END)
        assert size == len(payload)


def test_seek_end_returns_correct_size_multi_stream():
    parts = [b"hello", b"world", b"!"]
    xz_data = make_xz_multistream(parts)
    with open_xz(xz_data) as f:
        size = f.seek(0, io.SEEK_END)
        assert size == sum(len(p) for p in parts)


@requires_xz_cli
def test_seek_end_returns_correct_size_multi_block():
    payload = b"X" * 12000
    xz_data = make_xz_multiblock(payload, block_size=4000)
    with open_xz(xz_data) as f:
        size = f.seek(0, io.SEEK_END)
        assert size == len(payload)


def test_seek_end_does_not_decompress_single_block():
    """SEEK_END on a single-stream file uses the backwards scan, not decompression."""
    payload = b"data" * 500
    with open_xz(make_xz(payload)) as f:
        f.seek(0, io.SEEK_END)
        assert f._decomp_cursor == 0  # type: ignore[attr-defined]
        assert f._index_built  # type: ignore[attr-defined]


@requires_xz_cli
def test_seek_end_does_not_decompress_multi_block():
    """SEEK_END should use the backwards scan for all blocks."""
    payload = b"Z" * 15000
    xz_data = make_xz_multiblock(payload, block_size=5000)
    with open_xz(xz_data) as f:
        f.seek(0, io.SEEK_END)
        assert f._decomp_cursor == 0  # type: ignore[attr-defined]
        assert f._index_built  # type: ignore[attr-defined]


def test_seek_end_then_read_last_bytes():
    parts = [b"first", b"second", b"last!"]
    xz_data = make_xz_multistream(parts)
    with open_xz(xz_data) as f:
        f.seek(-5, io.SEEK_END)
        assert f.read() == b"last!"


def test_seek_past_eof_then_read_returns_empty():
    payload = b"hello world"
    with open_xz(make_xz(payload)) as f:
        size = f.seek(0, io.SEEK_END)
        f.seek(1, io.SEEK_END)
        assert f.read() == b""
        assert f.seek(0, io.SEEK_END) == size


@requires_xz_cli
def test_seek_end_negative_offset_into_earlier_block():
    parts = [b"A" * 4000, b"B" * 4000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=4000)
    with open_xz(xz_data) as f:
        # Seek 1 byte into the second block from the end
        f.seek(-4001, io.SEEK_END)
        result = f.read(5)
        assert result == b"A" + b"B" * 4


# ---------------------------------------------------------------------------
# Index building
# ---------------------------------------------------------------------------


@requires_xz_cli
def test_seek_points_built_progressively():
    """Reading forward adds seek points for completed blocks."""
    parts = [b"part0" * 1000, b"part1" * 1000, b"part2" * 1000]
    payload = b"".join(parts)
    xz_data = make_xz_multiblock(payload, block_size=len(parts[0]))
    with open_xz(xz_data) as f:
        # Origin point only initially
        assert len(f._seek_points) == 1  # type: ignore[attr-defined]

        # After reading part0 fully, its block seek point is added
        f.read(len(parts[0]))
        # The first block's seek point is at offset 0, same as origin — deduplication
        # means the count may still be 1 after reading part0 only if the origin point
        # already covers block 0's start.  After reading through part1, we get 2.
        f.read(len(parts[1]))
        assert len(f._seek_points) >= 2  # type: ignore[attr-defined]

        f.read()
        assert len(f._seek_points) >= 3  # type: ignore[attr-defined]


def test_backward_scan_multi_stream():
    """_read_xz_index_backwards correctly handles concatenated XZ streams."""
    parts = [b"hello", b"world"]
    xz_data = make_xz_multistream(parts)
    stream = io.BytesIO(xz_data)
    blocks = _read_xz_index_backwards(stream, len(xz_data))
    assert len(blocks) == 2
    assert blocks[0].decompressed_start == 0
    assert blocks[0].decompressed_size == len(parts[0])
    assert blocks[1].decompressed_start == len(parts[0])
    assert blocks[1].decompressed_size == len(parts[1])


@requires_xz_cli
def test_backward_scan_multi_block():
    """_read_xz_index_backwards returns correct block info for multi-block XZ."""
    payload = b"X" * 12000
    xz_data = make_xz_multiblock(payload, block_size=4000)
    stream = io.BytesIO(xz_data)
    blocks = _read_xz_index_backwards(stream, len(xz_data))
    assert len(blocks) == 3
    total = sum(b.decompressed_size for b in blocks)
    assert total == len(payload)
    # Blocks should be in forward order
    for i, block in enumerate(blocks):
        assert block.decompressed_start == i * 4000
    # Consecutive blocks are contiguous in compressed space
    for i in range(len(blocks) - 1):
        end = blocks[i].compressed_start + blocks[i].block_header_size + blocks[i].compressed_data_size + blocks[i].check_size
        # Padded end (rounded to 4 bytes) should equal next block's start
        padded_end = (end + 3) & ~3
        assert padded_end == blocks[i + 1].compressed_start


def test_backward_scan_single_block():
    payload = b"test data"
    xz_data = make_xz(payload)
    stream = io.BytesIO(xz_data)
    blocks = _read_xz_index_backwards(stream, len(xz_data))
    assert len(blocks) == 1
    assert blocks[0].decompressed_size == len(payload)
    assert blocks[0].decompressed_start == 0


# ---------------------------------------------------------------------------
# Non-seekable stream
# ---------------------------------------------------------------------------


def test_non_seekable_stream_sequential_read():
    """Sequential reading must work on a non-seekable input stream."""
    payload = b"hello from non-seekable stream"
    xz_data = make_xz(payload)
    ns_stream = io.BufferedReader(_NonSeekableStream(xz_data))
    with XzDecompressorStream(ns_stream) as f:
        assert not f.seekable()
        assert f.read() == payload


@requires_xz_cli
def test_non_seekable_multi_block_sequential():
    """Multi-block sequential read must work on non-seekable streams."""
    payload = b"M" * 12000
    xz_data = make_xz_multiblock(payload, block_size=4000)
    ns_stream = io.BufferedReader(_NonSeekableStream(xz_data))
    with XzDecompressorStream(ns_stream) as f:
        assert not f.seekable()
        assert f.read() == payload


def test_non_seekable_multi_stream_sequential():
    """Multi-stream sequential read must work on non-seekable streams."""
    parts = [b"stream1", b"stream2", b"stream3"]
    xz_data = make_xz_multistream(parts)
    ns_stream = io.BufferedReader(_NonSeekableStream(xz_data))
    with XzDecompressorStream(ns_stream) as f:
        assert not f.seekable()
        assert f.read() == b"".join(parts)


# ---------------------------------------------------------------------------
# Corruption detection
# ---------------------------------------------------------------------------


def test_invalid_magic_raises_corrupted_error():
    """Data not starting with XZ magic must raise ArchiveCorruptedError."""
    gzip_magic = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03"
    with open_xz(gzip_magic + b"\x00" * 100) as f:
        with pytest.raises(ArchiveCorruptedError):
            f.read()


def test_empty_file_raises_corrupted_error():
    with open_xz(b"") as f:
        with pytest.raises(ArchiveCorruptedError):
            f.read()


def test_truncated_file_raises_eof_error():
    payload = b"truncated data"
    xz_data = make_xz(payload)
    truncated = xz_data[: len(xz_data) - 10]
    with open_xz(truncated) as f:
        with pytest.raises((ArchiveCorruptedError, ArchiveEOFError)):
            f.read()


def test_corrupted_block_data_raises_error():
    """Flipping the first byte of compressed data triggers an LZMA decompression error."""
    payload = b"valid content"
    xz_data = bytearray(make_xz(payload))
    # Byte 24 = first byte of compressed data (after 12-byte stream header + 12-byte block header).
    # Flipping it reliably produces lzma.LZMAError.
    xz_data[24] ^= 0xFF
    with open_xz(bytes(xz_data)) as f:
        with pytest.raises((ArchiveCorruptedError, ArchiveEOFError)):
            f.read()


def test_backward_scan_corrupted_footer_raises_error():
    """_read_xz_index_backwards raises ArchiveCorruptedError on bad footer magic."""
    payload = b"some data"
    xz_data = bytearray(make_xz(payload))
    # Corrupt the "YZ" footer magic (last 2 bytes)
    xz_data[-2] = 0xFF
    xz_data[-1] = 0xFF
    stream = io.BytesIO(bytes(xz_data))
    with pytest.raises(ArchiveCorruptedError):
        _read_xz_index_backwards(stream, len(xz_data))


# ---------------------------------------------------------------------------
# Exception translation
# ---------------------------------------------------------------------------


def test_translate_xz_exception_wraps_lzma_error():
    result = _translate_xz_stream_exception(lzma.LZMAError("bad data"))
    assert isinstance(result, ArchiveCorruptedError)


def test_translate_xz_exception_wraps_eof_error():
    from archivey.exceptions import ArchiveEOFError as _ArchiveEOFError
    result = _translate_xz_stream_exception(EOFError("truncated"))
    assert isinstance(result, _ArchiveEOFError)


def test_translate_xz_exception_returns_none_for_unknown():
    result = _translate_xz_stream_exception(ValueError("something else"))
    assert result is None
