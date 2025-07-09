import io
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from archivey.internal import io_helpers
from archivey.internal.io_helpers import (
    BinaryIOWrapper,
    LazyOpenIO,
    RewindableNonSeekableStream,
    UncloseableStream,
    ensure_binaryio,
    ensure_bufferedio,
    is_stream,
)
from tests.archivey.test_open_nonseekable import NonSeekableBytesIO


def test_lazy_open_only_on_read():
    open_fn = Mock(return_value=io.BytesIO(b"hello"))
    wrapper = LazyOpenIO(open_fn, seekable=True)
    assert wrapper.seekable() is True
    assert open_fn.call_count == 0
    assert wrapper.read() == b"hello"
    assert open_fn.call_count == 1
    wrapper.close()


def test_lazy_open_not_called_when_unused():
    open_fn = Mock(return_value=io.BytesIO(b"unused"))
    wrapper = LazyOpenIO(open_fn, seekable=True)
    assert wrapper.seekable() is True
    wrapper.close()
    assert open_fn.call_count == 0


def test_lazy_open_closes_inner_stream():
    inner = io.BytesIO(b"data")
    open_fn = Mock(return_value=inner)
    wrapper = LazyOpenIO(open_fn, seekable=True)
    wrapper.read(1)
    wrapper.close()
    assert inner.closed
    with pytest.raises(ValueError):
        wrapper.read()


DATA = b"0123456789abcdef"


def create_stream() -> RewindableNonSeekableStream:
    inner = NonSeekableBytesIO(DATA)
    return RewindableNonSeekableStream(inner)


# RewindableNonSeekableStream tests
class TestRewindableNonSeekableStream:
    def test_basic_read(self):
        """Test basic reading functionality."""
        stream = create_stream()

        assert stream.read(5) == b"01234"
        assert stream.tell() == 5
        assert stream.read(6) == b"56789a"
        assert stream.tell() == 11
        assert stream.read(-1) == b"bcdef"
        assert stream.tell() == 16
        assert stream.read(3) == b""
        assert stream.tell() == 16

    def test_read_all(self):
        """Test reading entire stream."""
        stream = create_stream()

        assert stream.read() == DATA
        assert stream.tell() == len(DATA)

    def test_seek_within_buffer(self):
        """Test seeking within already-read buffer."""
        stream = create_stream()

        # Read some data to populate buffer
        assert stream.read(5) == b"01234"
        assert stream.tell() == 5

        # Seek back to beginning
        assert stream.seek(0) == 0
        assert stream.tell() == 0
        assert stream.read(3) == b"012"
        assert stream.read(7) == b"3456789"

        # Seek to middle
        assert stream.tell() == 10
        assert stream.seek(-8, 1) == 2
        assert stream.read() == DATA[2:]

    def test_seek_forward_beyond_buffer(self):
        """Test seeking forward beyond current buffer."""
        stream = create_stream()

        # Read some data
        assert stream.read(5) == b"01234"

        # Seek forward beyond buffer
        assert stream.seek(8) == 8
        assert stream.read(3) == b"89a"

        assert stream.tell() == 11

        # The stream is in recording mode, so the intermediate data is cached.
        stream.seek(0)
        assert stream.read() == b"0123456789abcdef"

    def test_readinto(self):
        """Test readinto method."""
        stream = create_stream()

        buffer = bytearray(5)
        assert stream.readinto(buffer) == 5
        assert buffer == b"01234"
        assert stream.tell() == 5

    def test_stop_recording(self):
        """Test stop_recording functionality."""
        stream = create_stream()

        # Read some data while recording
        assert stream.read(5) == b"01234"
        assert stream.seekable() is True

        # Stop recording
        stream.stop_recording()
        assert stream.seekable() is False  # We can't seek within the buffer anymore

        # Can still read forward
        assert stream.read(6) == b"56789a"
        current_pos = stream.tell()
        assert current_pos == 11

        # Can re-read the buffered data after stopping recording
        assert stream.seek(0) == 0
        assert stream.read(2) == b"01"
        assert stream.read(2) == b"23"
        assert stream.read(2) == b"4"  # Only one byte left in the buffer
        with pytest.raises(io.UnsupportedOperation):
            stream.read(2)

        # Seeking backwards from the current position to an unbuffered region.
        with pytest.raises(io.UnsupportedOperation):
            stream.seek(10)

        # Seeking forward to an unbuffered region is supported.
        stream.seek(12)
        assert stream.read() == b"cdef"

    def test_seek_to_end_unsupported(self):
        """Test that seeking to end is not supported."""
        stream = create_stream()

        with pytest.raises(io.UnsupportedOperation, match="seek to end"):
            stream.seek(0, io.SEEK_END)

    def test_seek_negative_position(self):
        """Test that seeking to negative position is not supported."""
        stream = create_stream()

        with pytest.raises(io.UnsupportedOperation, match="seek to negative position"):
            stream.seek(-1)

    def test_seek_invalid_whence(self):
        """Test that invalid whence values raise ValueError."""
        stream = create_stream()

        with pytest.raises(ValueError, match="Invalid whence"):
            stream.seek(0, 999)

    def test_properties(self):
        """Test stream properties."""
        stream = create_stream()

        assert stream.readable() is True
        assert stream.writable() is False
        assert stream.seekable() is True

        stream.stop_recording()
        assert stream.seekable() is False

    def test_close(self):
        """Test closing the stream."""
        inner = io.BytesIO(b"hello world")
        stream = RewindableNonSeekableStream(inner)

        stream.close()
        assert stream.closed
        assert inner.closed

    def test_delegate_attributes(self):
        """Test that unknown attributes are delegated to inner stream."""
        inner = Mock(spec=io.BytesIO)
        inner.custom_attr = "test_value"
        stream = RewindableNonSeekableStream(inner)

        assert stream.custom_attr == "test_value"

    def test_empty_stream(self):
        """Test behavior with empty stream."""
        inner = io.BytesIO(b"")
        stream = RewindableNonSeekableStream(inner)

        assert stream.read() == b""
        assert stream.tell() == 0

    def test_large_reads(self):
        """Test reading large amounts of data."""
        data = b"x" * 10000
        inner = io.BytesIO(data)
        stream = RewindableNonSeekableStream(inner)

        # Read in chunks
        chunk1 = stream.read(3000)
        assert len(chunk1) == 3000
        assert stream.tell() == 3000

        # Seek back
        stream.seek(1000)
        assert stream.tell() == 1000

        # Read more
        chunk2 = stream.read(2000)
        assert len(chunk2) == 2000
        assert stream.tell() == 3000

    def test_read_after_close(self):
        """Test reading after closing raises error."""
        stream = create_stream()

        stream.close()

        with pytest.raises(ValueError, match="I/O operation on closed file"):
            stream.read(5)

    def test_wrap_in_buffered_reader(self):
        """Test wrapping in a buffered reader."""
        # This test is tricky because the BufferedReader will read more bytes than
        # the amount we ask for, so it may ask for non-cached bytes from the stream.
        # We should still be able to read all the cached region via the BufferedReader.

        stream = create_stream()
        buffered_stream = io.BufferedReader(stream, buffer_size=8)

        assert (
            buffered_stream.read(5) == b"01234"
        )  # Should actually read 8 bytes from stream
        assert buffered_stream.tell() == 5
        assert stream.tell() == 8

        assert buffered_stream.seekable() is True
        stream.stop_recording()

        assert buffered_stream.seekable() is False
        assert buffered_stream.read(5) == b"56789"
        assert stream.tell() == 16  # Should have read 8 more bytes
        assert buffered_stream.tell() == 10


class OnlyReadStream:
    def __init__(self, data: bytes):
        self._inner = io.BytesIO(data)

    def read(self, size=-1):
        return self._inner.read(size)


def test_ensure_binaryio():
    """Test ensure_binaryio function."""
    stream = io.BytesIO(b"hello")
    assert ensure_binaryio(stream) is stream

    orig = OnlyReadStream(b"hello")
    wrapped = ensure_binaryio(orig)
    assert not wrapped.closed
    assert isinstance(wrapped, BinaryIOWrapper)
    assert wrapped.read(2) == b"he"
    b = bytearray(10)
    assert wrapped.readinto(b) == 3
    assert b[:3] == b"llo"
    assert wrapped.seekable() is False
    assert wrapped.readable() is True
    assert wrapped.writable() is False
    with pytest.raises(io.UnsupportedOperation):
        wrapped.write(b"hello")
    with pytest.raises(io.UnsupportedOperation):
        wrapped.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        wrapped.tell()
    wrapped.close()
    assert wrapped.closed


def test_ensure_bufferedio():
    """Test ensure_bufferedio function."""
    stream = OnlyReadStream(b"hello")
    buffered = ensure_bufferedio(stream)
    assert buffered is not stream
    assert isinstance(buffered, io.BufferedReader)
    assert buffered.read() == b"hello"


def test_is_stream(tmp_path: Path):
    """Test is_stream function with BinaryIO."""
    stream = OnlyReadStream(b"hello")
    assert not is_stream(stream)
    wrapped = ensure_binaryio(stream)
    assert is_stream(wrapped)
    buffered = ensure_bufferedio(wrapped)
    assert isinstance(buffered, io.BufferedReader)  # Just checking for the test
    assert is_stream(buffered)
    uncloseable = io_helpers.ensure_uncloseable(buffered)
    assert isinstance(uncloseable, UncloseableStream)  # Just checking for the test
    assert is_stream(uncloseable)
    assert uncloseable.read() == b"hello"

    assert is_stream(io.BytesIO(b"hello"))

    with open(tmp_path / "test.txt", "wb") as f:
        assert is_stream(f)
        f.write(b"hello")

    with open(tmp_path / "test.txt", "rb") as f:
        assert is_stream(f)
        assert f.read() == b"hello"

    # Check that files are considered streams
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"hello")
        f.seek(0)
        assert is_stream(f)

    assert not is_stream(None)
    assert not is_stream(1)
    assert not is_stream("hello")
    assert not is_stream(b"hello")
