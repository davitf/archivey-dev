import io
from unittest.mock import Mock

import pytest

from archivey.internal.io_helpers import LazyOpenIO, RewindableNonSeekableStream
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
        assert stream.seekable() is False

        # Can still read forward
        assert stream.read(6) == b"56789a"
        current_pos = stream.tell()
        assert current_pos == 11

        # Can re-read the buffered data after stopping recording
        assert stream.seek(0) == 0
        assert stream.read(2) == b"01"
        assert stream.read(2) == b"23"
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
