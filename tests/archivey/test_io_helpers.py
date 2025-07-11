import io
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from archivey.internal.io_helpers import (
    BinaryIOWrapper,
    ConcatenationStream,
    LazyOpenIO,
    RecordableStream,
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


def create_stream() -> RecordableStream:
    inner = NonSeekableBytesIO(DATA)
    return RecordableStream(inner)


# RecordableStream tests
class TestRecordableStream:
    def test_basic_read(self):
        stream = create_stream()
        assert stream.read(5) == b"01234"
        assert stream.tell() == 5
        assert stream.read() == b"56789abcdef"
        assert stream.tell() == len(DATA)
        assert stream.get_all_data() == DATA

    def test_read_all(self):
        """Test reading entire stream."""
        stream = create_stream()
        assert stream.read() == DATA
        assert stream.tell() == len(DATA)

    def test_seek_within_recorded(self):
        stream = create_stream()
        stream.read(6)
        assert stream.seek(0) == 0
        assert stream.read(3) == b"012"
        assert stream.seek(4) == 4
        assert stream.read(2) == b"45"

    def test_seek_outside_recorded(self):
        stream = create_stream()
        stream.seek(5)
        assert stream.tell() == 5
        assert stream.read(2) == DATA[5:7]

    def test_seek_end_unsupported(self):
        stream = create_stream()
        with pytest.raises(io.UnsupportedOperation, match="seek to end"):
            stream.seek(0, io.SEEK_END)

    def test_readinto(self):
        stream = create_stream()
        buf = bytearray(5)
        assert stream.readinto(buf) == 5
        assert bytes(buf) == b"01234"

    def test_properties_and_close(self):
        stream = create_stream()
        assert stream.readable() is True
        assert stream.writable() is False
        assert stream.seekable() is True
        stream.close()
        assert stream.closed

    # def test_delegate_attributes(self):
    #     inner = Mock(spec=io.BytesIO)
    #     inner.custom_attr = "test_value"
    #     stream = RecordableStream(inner)
    #     assert stream.custom_attr == "test_value"

    def test_empty_stream(self):
        inner = io.BytesIO(b"")
        stream = RecordableStream(inner)
        assert stream.read() == b""
        assert stream.tell() == 0

    def test_large_reads(self):
        data = b"x" * 10000
        inner = io.BytesIO(data)
        stream = RecordableStream(inner)
        chunk1 = stream.read(3000)
        assert len(chunk1) == 3000
        assert stream.tell() == 3000
        stream.seek(1000)
        assert stream.read(2000) == data[1000:3000]

    def test_read_after_close(self):
        stream = create_stream()
        stream.close()
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            stream.read(5)


def test_concatenation_stream():
    stream = ConcatenationStream([io.BytesIO(b"abc"), io.BytesIO(b"de")])
    assert not stream.seekable()
    assert stream.read(1) == b"a"
    assert stream.read(4) == b"bc"  # Finish first stream
    assert stream.read() == b"de"
    assert stream.read() == b""

    stream = ConcatenationStream([io.BytesIO(b"abc"), io.BytesIO(b"de")])
    assert stream.read() == b"abcde"


def test_concatenation_stream_with_buffering():
    # Test that the concatenation stream can be wrapped in a buffered reader.
    stream = ConcatenationStream([io.BytesIO(b"abc"), io.BytesIO(b"de")])
    buffered = ensure_bufferedio(stream)
    assert not buffered.seekable()
    assert buffered.read(1) == b"a"
    assert buffered.read(4) == b"bcde"  # Start reading from the second stream
    assert buffered.read() == b""


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
    assert buffered.read() == b"hello"

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
