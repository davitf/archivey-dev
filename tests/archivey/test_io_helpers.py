import io
from unittest.mock import Mock

import pytest

from archivey.internal.io_helpers import (
    ConcatenationStream,
    LazyOpenIO,
    RecordableStream,
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

    def test_delegate_attributes(self):
        inner = Mock(spec=io.BytesIO)
        inner.custom_attr = "test_value"
        stream = RecordableStream(inner)
        assert stream.custom_attr == "test_value"

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
    s1 = io.BytesIO(b"abc")
    s2 = io.BytesIO(b"de")
    stream = ConcatenationStream([s1, s2])
    assert stream.read(4) == b"abcd"
    assert stream.read() == b"e"
