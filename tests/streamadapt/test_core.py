import io

import pytest

from streamadapt import ensure_stream


class NonSeekable(io.BytesIO):
    def seekable(self):
        return False

    def seek(self, *args, **kwargs):  # type: ignore[override]
        raise io.UnsupportedOperation("seek not supported")

    def tell(self):  # type: ignore[override]
        raise io.UnsupportedOperation("tell not supported")


def test_ensure_stream_binary_bytesio():
    bio = io.BytesIO(b"abc")
    s = ensure_stream(bio, mode="binary", seekable=True)
    assert s.read(1) == b"a"
    assert s.seek(0) == 0
    assert s.read() == b"abc"


def test_ensure_stream_text_from_binary():
    bio = io.BytesIO(b"hello")
    s = ensure_stream(bio, mode="text", encoding="utf-8")
    assert s.read() == "hello"


def test_detect_stream_mode():
    sio = io.StringIO("xyz")
    s = ensure_stream(sio)
    assert isinstance(s, io.TextIOBase)
    assert s.read() == "xyz"


def test_seekable_wrapper():
    ns = NonSeekable(b"123")
    s = ensure_stream(ns, mode="binary", seekable=True)
    assert s.read() == b"123"
    assert s.seek(0) == 0
    assert s.read(1) == b"1"
