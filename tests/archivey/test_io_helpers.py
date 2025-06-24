import io
from unittest.mock import Mock

import pytest

from archivey.readers.io_helpers import LazyOpenIO


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
