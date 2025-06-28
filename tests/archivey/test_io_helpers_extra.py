import io
import pytest

from archivey.io_helpers import ErrorIOStream, ExceptionTranslatingIO, StatsIO, IOStats
from archivey.exceptions import ArchiveIOError


class DummyStream(io.BytesIO):
    def read(self, n=-1):
        raise ValueError("boom")


def test_error_iostream_raises_provided_exception():
    err = RuntimeError("fail")
    stream = ErrorIOStream(err)
    with pytest.raises(RuntimeError):
        stream.read()
    with pytest.raises(RuntimeError):
        stream.write(b"data")


def test_exception_translating_io_translates_exceptions():
    def translator(exc):
        if isinstance(exc, ValueError):
            return ArchiveIOError("translated")
        return None

    stream = ExceptionTranslatingIO(DummyStream(b""), translator)
    with pytest.raises(ArchiveIOError) as excinfo:
        stream.read()
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_exception_translating_io_callable_raises_on_open():
    def open_fn():
        raise ValueError("bad open")

    def translator(exc):
        if isinstance(exc, ValueError):
            return ArchiveIOError("translated")
        return None

    with pytest.raises(ArchiveIOError):
        ExceptionTranslatingIO(open_fn, translator)


def test_stats_io_records_reads_and_seeks():
    inner = io.BytesIO(b"abcdef")
    stats = IOStats()
    stream = StatsIO(inner, stats)
    assert stream.read(2) == b"ab"
    stream.seek(4)
    assert stream.read() == b"ef"
    assert stats.bytes_read == 4
    assert stats.seek_calls == 1
    assert stats.read_ranges == [[0, 2], [4, 2]]
