from archivey.internal.utils import is_stream


class DummyStream:
    def read(self, n: int = -1) -> bytes:
        return b""


class NonCallableRead:
    read = b"not callable"


def test_is_stream_accepts_custom_object():
    assert is_stream(DummyStream()) is True


def test_is_stream_rejects_non_stream():
    class Foo:
        pass

    assert is_stream(Foo()) is False


def test_is_stream_rejects_noncallable_read():
    assert is_stream(NonCallableRead()) is False
