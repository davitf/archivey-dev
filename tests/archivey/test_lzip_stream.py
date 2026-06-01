"""Tests for LzipDecompressorStream: multi-member files and efficient seeking."""

import io
import struct

import pytest

from archivey.exceptions import ArchiveCorruptedError, ArchiveEOFError
from archivey.formats.compressed_streams import LzipDecompressorStream
from archivey.formats.lzip_stream import _MemberBounds, _read_index_backwards
from tests.archivey.create_archives import create_lzip_member

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_multi_member(parts: list[bytes], dict_size_bits: int = 12) -> bytes:
    """Concatenate multiple lzip members into one byte string."""
    return b"".join(create_lzip_member(p, dict_size_bits) for p in parts)


class _CountingStream(io.RawIOBase):
    """Wraps a BytesIO and counts read() calls and total bytes read."""

    def __init__(self, data: bytes) -> None:
        super().__init__()
        self._inner = io.BytesIO(data)
        self.read_calls = 0
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        n = self._inner.readinto(b)
        if n:
            self.read_calls += 1
            self.bytes_read += n
        return n

    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        data = self._inner.read(n)
        if data:
            self.read_calls += 1
            self.bytes_read += len(data)
        return data

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def seekable(self) -> bool:
        return True


class _LimitedReadStream(io.BufferedIOBase):
    """Wraps BytesIO and caps each read() to at most max_read_size bytes.

    Used in tests to prevent DecompressorStream from consuming multiple
    lzip members' compressed data in a single _read_decompressed_chunk() call.
    Since it extends BufferedIOBase, ensure_bufferedio() returns it as-is.
    """

    def __init__(self, data: bytes, max_read_size: int) -> None:
        super().__init__()
        self._inner = io.BytesIO(data)
        self._max = max_read_size

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        if n is None or n < 0:
            return self._inner.read()
        return self._inner.read(min(n, self._max))

    def read1(self, n: int = -1) -> bytes:
        return self.read(n)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()

    def seekable(self) -> bool:
        return True


def open_lzip(data: bytes) -> LzipDecompressorStream:
    return LzipDecompressorStream(io.BytesIO(data))


def open_lzip_counting(data: bytes) -> tuple[LzipDecompressorStream, _CountingStream]:
    cs = _CountingStream(data)
    stream = LzipDecompressorStream(io.BufferedReader(cs))
    return stream, cs


# ---------------------------------------------------------------------------
# Basic multi-member reading
# ---------------------------------------------------------------------------


def test_single_member_read():
    payload = b"hello world"
    data = make_multi_member([payload])
    with open_lzip(data) as f:
        assert f.read() == payload


def test_multi_member_read():
    parts = [b"first member", b"second member", b"third member"]
    data = make_multi_member(parts)
    expected = b"".join(parts)
    with open_lzip(data) as f:
        assert f.read() == expected


def test_multi_member_read_chunked():
    parts = [b"aaa" * 100, b"bbb" * 100, b"ccc" * 100]
    data = make_multi_member(parts)
    expected = b"".join(parts)
    with open_lzip(data) as f:
        result = bytearray()
        while True:
            chunk = f.read(50)
            if not chunk:
                break
            result.extend(chunk)
    assert bytes(result) == expected


# ---------------------------------------------------------------------------
# Forward seeking
# ---------------------------------------------------------------------------


def test_single_member_seek_forward():
    payload = b"abcdefghij"
    data = make_multi_member([payload])
    with open_lzip(data) as f:
        f.seek(3)
        assert f.read(4) == b"defg"


def test_multi_member_seek_forward_into_later_member():
    parts = [b"AAAA", b"BBBB", b"CCCC"]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        # Seek past the first member into the third
        f.seek(8)
        assert f.read(4) == b"CCCC"


def test_forward_seek_skips_intermediate_members():
    """After building the index, seeking forward past indexed members doesn't
    decompress those members — verified by checking completed_members count."""
    parts = [b"member0" * 10, b"member1" * 10, b"member2" * 10]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        # Build the full index via SEEK_END
        f.seek(0, io.SEEK_END)
        assert f._index_complete  # type: ignore[attr-defined]

        # Seek back to start, then forward directly to member 2
        f.seek(0)
        target = len(parts[0]) + len(parts[1])
        f.seek(target)
        # Only member 2 should have been decompressed by the current decompressor
        completed = f._decompressor.completed_members  # type: ignore[attr-defined]
        # We jumped to member 2; members 0 and 1 were skipped
        assert len(completed) == 0  # jumped directly, member 2 not finished yet
        assert f.read() == parts[2]


# ---------------------------------------------------------------------------
# Backward seeking
# ---------------------------------------------------------------------------


def test_single_member_seek_backward():
    payload = b"abcdefghij"
    data = make_multi_member([payload])
    with open_lzip(data) as f:
        f.read()  # advance to EOF
        f.seek(3)
        assert f.read(4) == b"defg"


def test_multi_member_seek_backward_to_earlier_member():
    parts = [b"AAAA", b"BBBB", b"CCCC"]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        f.read()  # read all three members (builds index)
        f.seek(0)
        assert f.read(4) == b"AAAA"
        f.seek(4)
        assert f.read(4) == b"BBBB"


def test_backward_seek_uses_index_not_position_zero():
    """After reading to member 2, seeking backward to member 1 should jump
    directly to member 1's compressed offset, not rewind to compressed byte 0."""
    parts = [b"member0" * 20, b"member1" * 20, b"member2" * 20]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        # Read through all members so the index covers 0..2
        f.read()
        assert len(f._member_index) == 3  # type: ignore[attr-defined]

        # Now seek backward to the start of member 1
        target = len(parts[0])
        f.seek(target)
        # After the jump, the current member should be member 1 (not 0)
        assert f._current_member_idx == 1  # type: ignore[attr-defined]
        assert f.read(len(parts[1])) == parts[1]


# ---------------------------------------------------------------------------
# SEEK_END
# ---------------------------------------------------------------------------


def test_seek_end_returns_correct_size():
    parts = [b"hello", b"world", b"!"]
    data = make_multi_member(parts)
    expected_size = sum(len(p) for p in parts)
    with open_lzip(data) as f:
        size = f.seek(0, io.SEEK_END)
        assert size == expected_size


def test_seek_end_does_not_decompress():
    """SEEK_END should use the backwards trailer scan, not decompress any data."""
    parts = [b"first" * 100, b"second" * 100, b"third" * 100]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        f.seek(0, io.SEEK_END)
        # The backward scan should not have decompressed any members
        assert f._decompressor.completed_members == []  # type: ignore[attr-defined]
        assert f._index_complete  # type: ignore[attr-defined]


def test_seek_end_then_read_last_bytes():
    parts = [b"first", b"second", b"last!"]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        f.seek(-5, io.SEEK_END)
        assert f.read() == b"last!"


def test_seek_end_negative_offset_into_earlier_member():
    parts = [b"AAAA", b"BBBB"]
    data = make_multi_member(parts)
    # Total = 8 bytes. -6 → position 2 (2 bytes into "AAAA")
    with open_lzip(data) as f:
        f.seek(-6, io.SEEK_END)
        assert f.read() == b"AABBBB"


def test_forward_seek_triggers_index_build():
    """Seeking forward past the known index frontier should trigger a backwards scan."""
    parts = [b"part0" * 5, b"part1" * 5, b"part2" * 5]
    member_bytes = [create_lzip_member(p) for p in parts]
    data = b"".join(member_bytes)
    # Limit each compressed read to one member's worth of data so that members
    # are indexed one at a time (the default 65536-byte read would index them all
    # at once for this small test data, making _index_complete True immediately).
    chunk_size = max(len(m) for m in member_bytes)
    with LzipDecompressorStream(_LimitedReadStream(data, chunk_size)) as f:
        # Read only the first member's content
        f.read(len(parts[0]))
        assert not f._index_complete  # type: ignore[attr-defined]
        # Seek past what we've indexed — should trigger backwards scan
        target = len(parts[0]) + len(parts[1])
        f.seek(target)
        assert f._index_complete  # type: ignore[attr-defined]
        assert f.read() == parts[2]


# ---------------------------------------------------------------------------
# Member index building
# ---------------------------------------------------------------------------


def test_member_index_built_progressively():
    parts = [b"part0", b"part1", b"part2"]
    member_bytes = [create_lzip_member(p) for p in parts]
    data = b"".join(member_bytes)
    # Limit compressed reads to one member at a time so the index grows step-by-step.
    chunk_size = max(len(m) for m in member_bytes)
    with LzipDecompressorStream(_LimitedReadStream(data, chunk_size)) as f:
        # No members indexed yet
        assert len(f._member_index) == 0  # type: ignore[attr-defined]

        f.read(len(parts[0]))
        assert len(f._member_index) == 1  # type: ignore[attr-defined]

        f.read(len(parts[1]))
        assert len(f._member_index) == 2  # type: ignore[attr-defined]

        f.read()
        assert len(f._member_index) == 3  # type: ignore[attr-defined]


def test_member_index_bounds():
    parts = [b"aaa", b"bb", b"c"]
    data = make_multi_member(parts)
    with open_lzip(data) as f:
        f.read()
        idx: list[_MemberBounds] = f._member_index  # type: ignore[attr-defined]
        assert idx[0].decompressed_start == 0
        assert idx[0].decompressed_size == 3
        assert idx[1].decompressed_start == 3
        assert idx[1].decompressed_size == 2
        assert idx[2].decompressed_start == 5
        assert idx[2].decompressed_size == 1
        assert idx[2].decompressed_end == 6


def test_read_index_backwards():
    parts = [b"hello", b"world"]
    data = make_multi_member(parts)
    stream = io.BytesIO(data)
    members = _read_index_backwards(stream, len(data))
    assert len(members) == 2
    assert members[0].decompressed_start == 0
    assert members[0].decompressed_size == len(parts[0])
    assert members[1].decompressed_start == len(parts[0])
    assert members[1].decompressed_size == len(parts[1])
    assert (
        members[0].compressed_start + members[0].compressed_size
        == members[1].compressed_start
    )
    assert members[1].compressed_start + members[1].compressed_size == len(data)


# ---------------------------------------------------------------------------
# Corruption detection
# ---------------------------------------------------------------------------


def test_crc_corruption_in_first_member():
    parts = [b"good data", b"also good"]
    data = bytearray(make_multi_member(parts))
    # Flip a byte inside the LZMA stream of member 0 (after the 6-byte header)
    data[10] ^= 0xFF
    with open_lzip(bytes(data)) as f:
        with pytest.raises(ArchiveCorruptedError):
            f.read()


def test_crc_corruption_in_second_member():
    parts = [b"good data", b"corrupt this"]
    data = bytearray(make_multi_member(parts))
    # Corrupt the CRC32 field in member 1's trailer (last 20 bytes, first 4)
    trailer_start = len(data) - 20
    data[trailer_start] ^= 0xFF
    with open_lzip(bytes(data)) as f:
        with pytest.raises(ArchiveCorruptedError):
            f.read()


def test_truncation_mid_member():
    parts = [b"full member", b"truncated here"]
    data = make_multi_member(parts)
    truncated = data[: len(data) - 5]  # chop off last 5 bytes (inside trailer)
    with open_lzip(truncated) as f:
        with pytest.raises(ArchiveEOFError):
            f.read()


def test_invalid_member_size_too_small_triggers_backwards_scan_error():
    """A member_size smaller than header+trailer in a trailer should raise
    ArchiveCorruptedError during the backwards index scan."""
    parts = [b"valid", b"also valid"]
    data = bytearray(make_multi_member(parts))
    # Corrupt member_size in the last member's trailer (last 8 bytes of file)
    struct.pack_into("<Q", data, len(data) - 8, 5)  # absurdly small member_size
    with open_lzip(bytes(data)) as f:
        with pytest.raises(ArchiveCorruptedError):
            f.seek(0, io.SEEK_END)


def test_corrupt_member_size_detected_by_magic_check():
    """A plausible-looking but wrong member_size in the trailer causes the
    backwards scan to jump to an offset where b'LZIP' is not present."""
    parts = [b"first" * 10, b"second" * 10]
    data = bytearray(make_multi_member(parts))
    # Set member_size to a plausible-but-wrong value (off by 3)
    struct.pack_into("<Q", data, len(data) - 8, len(data) - 3)
    with open_lzip(bytes(data)) as f:
        with pytest.raises(ArchiveCorruptedError):
            f.seek(0, io.SEEK_END)
