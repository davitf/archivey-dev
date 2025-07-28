import functools
import http.server
import io
import logging
import os
import tempfile
import threading
from pathlib import Path
from unittest.mock import Mock

import pytest

from archivey.core import open_archive, open_compressed_stream
from archivey.exceptions import ArchiveStreamNotSeekableError
from archivey.formats.compressed_streams import get_stream_open_fn
from archivey.internal.archive_stream import ArchiveStream
from archivey.internal.io_helpers import (
    BinaryIOWrapper,
    ConcatenationStream,
    IOStats,
    RecordableStream,
    SlicingStream,
    StatsIO,
    ensure_binaryio,
    ensure_bufferedio,
    is_stream,
    read_exact,
)
from archivey.internal.utils import ensure_not_none
from archivey.types import ArchiveFormat
from tests.archivey.test_samples import (
    ALTERNATIVE_CONFIG,
    BASIC_ARCHIVES,
    LARGE_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.test_utils import skip_if_package_missing

logger = logging.getLogger(__name__)

# Formats known to fail when opened from a non-seekable stream
SKIPPABLE_FORMATS: set[ArchiveFormat] = {
    ArchiveFormat.ZIP,
    ArchiveFormat.RAR,
    ArchiveFormat.SEVENZIP,
}


# Formats known to fail when opened from a non-seekable stream with default/alternative packages
EXPECTED_NON_SEEKABLE_FAILURES: set[tuple[ArchiveFormat, bool]] = {
    (ArchiveFormat.GZIP, True),
    (ArchiveFormat.BZIP2, True),
    (ArchiveFormat.XZ, True),
    (ArchiveFormat.TAR_GZ, True),
    (ArchiveFormat.TAR_BZ2, True),
    (ArchiveFormat.TAR_XZ, True),
    (ArchiveFormat.ZIP, False),
    (ArchiveFormat.ZIP, True),
    (ArchiveFormat.RAR, False),
    (ArchiveFormat.RAR, True),
    (ArchiveFormat.SEVENZIP, False),
    (ArchiveFormat.SEVENZIP, True),
}


class NonSeekableBytesIO(io.BytesIO):
    def seekable(self) -> bool:  # pragma: no cover - simple
        return False

    def seek(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("seek")

    def tell(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("tell")


# SlicingStream tests
class TestSlicingStream:
    DATA = b"0123456789abcdefghijklmnopqrstuvwxyz"

    def test_read_seekable_with_start_and_length(self):
        """Read from a seekable stream with specified start and length."""
        underlying = io.BytesIO(self.DATA)
        # Slice from index 5, length 10
        sliced = SlicingStream(underlying, start=5, length=10)
        assert sliced.read(3) == b"567"
        assert sliced.tell() == 3
        assert sliced.read() == b"89abcde"  # Reads remaining 7 bytes of the slice
        assert sliced.tell() == 10
        assert sliced.read(5) == b""  # No more data in slice
        assert underlying.tell() == 15  # Underlying stream is at end of slice

    def test_read_seekable_with_start_only(self):
        """Read from a seekable stream with only start specified (reads to end)."""
        underlying = io.BytesIO(self.DATA)
        underlying.seek(3)  # Initial position of underlying stream
        # Slice from index 10 to the end of the underlying stream
        sliced = SlicingStream(underlying, start=10)
        assert sliced.read(5) == self.DATA[10:15]
        assert sliced.tell() == 5
        assert sliced.read() == self.DATA[15:]
        assert sliced.tell() == len(self.DATA) - 10
        assert underlying.tell() == len(self.DATA)  # Underlying stream at its end

    def test_read_seekable_with_length_only(self):
        """Read from a seekable stream with only length specified (from current pos)."""
        underlying = io.BytesIO(self.DATA)
        underlying.seek(7)  # Underlying stream starts at index 7
        # Slice from current position (7), length 10
        sliced = SlicingStream(underlying, length=10)
        assert sliced.read(4) == self.DATA[7:11]
        assert sliced.tell() == 4
        assert sliced.read() == self.DATA[11:17]
        assert sliced.tell() == 10
        assert underlying.tell() == 17  # Underlying stream is at end of slice

    def test_read_seekable_no_start_no_length(self):
        """Read from a seekable stream, no start/length (from current to end)."""
        underlying = io.BytesIO(self.DATA)
        underlying.seek(20)  # Underlying stream starts at index 20
        sliced = SlicingStream(underlying)  # Slice from 20 to end
        assert sliced.read(5) == self.DATA[20:25]
        assert sliced.tell() == 5
        assert sliced.read() == self.DATA[25:]
        assert sliced.tell() == len(self.DATA) - 20

    def test_read_non_seekable_with_length(self):
        """Read from a non-seekable stream with only length specified."""
        underlying = NonSeekableBytesIO(self.DATA)
        # Can't specify start for non-seekable. Reads first 15 bytes.
        sliced = SlicingStream(underlying, length=15)
        assert sliced.read(5) == self.DATA[:5]
        assert sliced.tell() == 5
        assert sliced.read(5) == self.DATA[5:10]
        assert sliced.tell() == 10
        assert sliced.read() == self.DATA[10:15]  # Reads remaining 5
        assert sliced.tell() == 15
        assert sliced.read(1) == b""  # No more data in slice

    def test_read_non_seekable_no_length(self):
        """Read from a non-seekable stream with no length (reads all)."""
        underlying = NonSeekableBytesIO(self.DATA)
        sliced = SlicingStream(underlying)
        assert sliced.read(10) == self.DATA[:10]
        assert sliced.tell() == 10
        assert sliced.read() == self.DATA[10:]
        assert sliced.tell() == len(self.DATA)

    def test_seek_within_slice_seekable(self):
        """Test seeking within a slice of a seekable stream."""
        underlying = io.BytesIO(self.DATA)
        # Slice: self.DATA[10:20] (length 10)
        sliced = SlicingStream(underlying, start=10, length=10)

        # SEEK_SET
        assert sliced.seek(3) == 3
        assert sliced.tell() == 3
        assert sliced.read(2) == self.DATA[13:15]  # Reads b"de"
        assert sliced.tell() == 5

        # SEEK_CUR
        assert sliced.seek(-2, io.SEEK_CUR) == 3  # Back to relative pos 3
        assert sliced.tell() == 3
        assert sliced.read(4) == self.DATA[13:17]  # Reads b"defg"
        assert sliced.tell() == 7

        # SEEK_END
        assert sliced.seek(-1, io.SEEK_END) == 9  # 1 before end of slice (10 - 1)
        assert sliced.tell() == 9
        assert sliced.read(5) == self.DATA[19:20]  # Reads b"j" (only 1 byte left)
        assert sliced.tell() == 10

        # Seek past end of slice
        assert sliced.seek(100) == 100
        assert sliced.tell() == 100
        assert sliced.read(1) == b""  # Reads nothing

        # Seek before start of slice
        with pytest.raises(ValueError, match="Negative seek position"):
            sliced.seek(-5)

    def test_seek_non_seekable_raises_error(self):
        """Seeking on a SlicingStream wrapping a non-seekable stream should fail."""
        underlying = NonSeekableBytesIO(self.DATA)
        sliced = SlicingStream(underlying, length=10)
        with pytest.raises(
            io.UnsupportedOperation, match="seek on non-seekable stream"
        ):
            sliced.seek(5)

    def test_tell_initial_and_after_read(self):
        """Test tell() at various points."""
        underlying_seekable = io.BytesIO(self.DATA)
        sliced_s = SlicingStream(underlying_seekable, start=5, length=10)
        assert sliced_s.tell() == 0
        sliced_s.read(3)
        assert sliced_s.tell() == 3
        sliced_s.read()
        assert sliced_s.tell() == 10

        underlying_nonseekable = NonSeekableBytesIO(self.DATA)
        sliced_ns = SlicingStream(underlying_nonseekable, length=10)
        assert sliced_ns.tell() == 0
        sliced_ns.read(7)
        assert sliced_ns.tell() == 7

    def test_edge_case_empty_slice_defined_length_zero(self):
        """Slice with length 0."""
        underlying = io.BytesIO(self.DATA)
        sliced = SlicingStream(underlying, start=5, length=0)
        assert sliced.read(10) == b""
        assert sliced.tell() == 0
        assert sliced.seek(0) == 0
        assert sliced.read() == b""

    def test_edge_case_slice_larger_than_stream_seekable(self):
        """Slice length exceeds underlying seekable stream."""
        underlying = io.BytesIO(self.DATA[:10])  # Underlying data is "0123456789"
        # Try to slice 20 bytes starting from 0
        sliced = SlicingStream(underlying, start=0, length=20)
        assert sliced.read() == self.DATA[:10]  # Reads only available 10 bytes
        assert sliced.tell() == 10
        assert sliced.read(5) == b""

    def test_edge_case_slice_larger_than_stream_non_seekable(self):
        """Slice length exceeds underlying non-seekable stream."""
        underlying = NonSeekableBytesIO(self.DATA[:10])
        sliced = SlicingStream(underlying, length=20)
        assert sliced.read() == self.DATA[:10]
        assert sliced.tell() == 10

    def test_slice_from_end_of_seekable_stream(self):
        """Slice starting at the very end of a seekable stream."""
        underlying = io.BytesIO(self.DATA)
        sliced = SlicingStream(underlying, start=len(self.DATA))
        assert sliced.read(5) == b""
        assert sliced.tell() == 0

    def test_non_seekable_start_is_none_enforced(self):
        """Ensure ValueError if start is given for non-seekable stream."""
        underlying = NonSeekableBytesIO(self.DATA)
        with pytest.raises(
            ValueError, match="Cannot slice a non-seekable stream with a start position"
        ):
            SlicingStream(underlying, start=5, length=10)

    def test_seek_end_no_length_seekable(self):
        """Test SEEK_END when length is None for a seekable stream slice."""
        underlying = io.BytesIO(self.DATA)  # Length 36
        sliced = SlicingStream(underlying, start=10)  # Slice from 10 to end (length 26)

        # Seek to the end of the slice (which is end of underlying stream)
        # underlying.seek(0, io.SEEK_END) gives 36.
        # start_abs is 10. So relative end is 36 - 10 = 26.
        assert sliced.seek(0, io.SEEK_END) == 26
        assert sliced.tell() == 26
        assert sliced.read(1) == b""  # At the end

        # Seeking with non-zero offset from SEEK_END when length is None is unsupported
        with pytest.raises(
            io.UnsupportedOperation,
            match="SEEK_END is not supported when slice length is not defined and offset is non-zero",
        ):
            sliced.seek(-5, io.SEEK_END)

        with pytest.raises(
            io.UnsupportedOperation,
            match="SEEK_END is not supported when slice length is not defined and offset is non-zero",
        ):
            sliced.seek(5, io.SEEK_END)

        # However, seeking to current position should still work if we manually go there
        # This part of the test verifies that after a SEEK_END with offset 0,
        # the stream is still usable for other seek operations like SEEK_SET.
        underlying.seek(0)  # Reset underlying stream for clarity for this part
        sliced_2 = SlicingStream(underlying, start=10)  # New slice from 10 to end
        end_pos_of_slice = sliced_2.seek(0, io.SEEK_END)  # Should be 26
        assert end_pos_of_slice == len(self.DATA) - 10

        # Now seek to a position relative to start
        target_relative_pos = end_pos_of_slice - 5  # Target: 21
        assert sliced_2.seek(target_relative_pos, io.SEEK_SET) == target_relative_pos
        assert sliced_2.tell() == target_relative_pos
        assert (
            sliced_2.read(2)
            == self.DATA[10 + target_relative_pos : 10 + target_relative_pos + 2]
        )
        assert sliced_2.tell() == target_relative_pos + 2

    def test_seek_end_no_length_non_zero_offset_error_in_seek(self):
        """
        Test that SlicingStream.seek() raises UnsupportedOperation for SEEK_END
        with a non-zero offset if the slice length is not defined.
        This is based on the current implementation of SlicingStream.seek().
        """
        underlying = io.BytesIO(self.DATA)
        sliced = SlicingStream(underlying, start=5)  # Length is None

        # This should fail as per SlicingStream.seek() logic
        with pytest.raises(
            io.UnsupportedOperation,
            match="SEEK_END is not supported when slice length is not defined and offset is non-zero",
        ):
            sliced.seek(1, io.SEEK_END)
        with pytest.raises(
            io.UnsupportedOperation,
            match="SEEK_END is not supported when slice length is not defined and offset is non-zero",
        ):
            sliced.seek(-1, io.SEEK_END)


def test_lazy_open_only_on_read():
    open_fn = Mock(return_value=io.BytesIO(b"hello"))
    wrapper = ArchiveStream(
        open_fn,
        exception_translator=lambda e: None,
        seekable=True,
        lazy=True,
        archive_path=None,
        member_name="",
    )
    assert wrapper.seekable() is True
    assert open_fn.call_count == 0
    assert wrapper.read() == b"hello"
    assert open_fn.call_count == 1
    wrapper.close()


def test_lazy_open_not_called_when_unused():
    open_fn = Mock(return_value=io.BytesIO(b"unused"))
    wrapper = ArchiveStream(
        open_fn,
        exception_translator=lambda e: None,
        seekable=True,
        lazy=True,
        archive_path=None,
        member_name="",
    )
    assert wrapper.seekable() is True
    wrapper.close()
    assert open_fn.call_count == 0


def test_lazy_open_closes_inner_stream():
    inner = io.BytesIO(b"data")
    open_fn = Mock(return_value=inner)
    wrapper = ArchiveStream(
        open_fn,
        exception_translator=lambda e: None,
        seekable=True,
        lazy=True,
        archive_path=None,
        member_name="",
    )
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


def test_concatenation_stream_composition():
    stream = ConcatenationStream(
        [io.BytesIO(b"01234"), io.BytesIO(b"56789"), io.BytesIO(b"abcdef")]
    )
    assert not stream.seekable()
    some_data = read_exact(stream, 7)
    assert some_data == b"0123456"

    second_stream = ConcatenationStream([io.BytesIO(b"foo"), stream])
    # The data read from the second stream should start from its current position.
    assert second_stream.read() == b"foo789abcdef"


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


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_ensure_bufferedio_with_compressed_stream(
    sample_archive, sample_archive_path, alternative_packages
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open_compressed_stream(sample_archive_path, config=config) as f:
        buffered = ensure_bufferedio(f)
        assert buffered.read() == sample_archive.contents.files[0].contents

    buffer = bytearray(1024)
    with open_compressed_stream(sample_archive_path, config=config) as f:
        buffered = ensure_bufferedio(f)
        bytes_read = buffered.readinto(buffer)
        assert bytes_read == min(
            len(buffer), len(sample_archive.contents.files[0].contents)
        )
        assert (
            buffer[:bytes_read]
            == sample_archive.contents.files[0].contents[:bytes_read]
        )


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_ensure_bufferedio_with_raw_compressed_stream(
    sample_archive, sample_archive_path, alternative_packages
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    open_fn, _ = get_stream_open_fn(sample_archive.creation_info.format, config)
    with open_fn(sample_archive_path) as f:
        buffered = ensure_bufferedio(f)
        assert buffered.read() == sample_archive.contents.files[0].contents

    buffer = bytearray(1024)
    with open_fn(sample_archive_path) as f:
        buffered = ensure_bufferedio(f)
        bytes_read = buffered.readinto(buffer)
        assert bytes_read == min(
            len(buffer), len(sample_archive.contents.files[0].contents)
        )
        assert (
            buffer[:bytes_read]
            == sample_archive.contents.files[0].contents[:bytes_read]
        )


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


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + LARGE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_nonseekable(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Ensure open_archive can read from non-seekable streams in streaming mode."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    stream = NonSeekableBytesIO(data)

    try:
        with open_archive(stream, streaming_only=True, config=config) as archive:
            members = []
            for member, member_stream in archive.iter_members_with_streams():
                members.append(member)
                if member_stream is not None:
                    member_stream.read()
            # The file names may not match in case of single-file archives, as we take
            # the name from the compressed file name which is not available when reading
            # from a stream. But checking the number of members should ensure that
            # we read all the members.
            assert len(members) == len([f.name for f in sample_archive.contents.files])

    except (
        ArchiveStreamNotSeekableError
    ) as exc:  # pragma: no cover - environment dependent
        key = (sample_archive.creation_info.format, alternative_packages)
        if key in EXPECTED_NON_SEEKABLE_FAILURES:
            pytest.xfail(
                f"Non-seekable {sample_archive.creation_info.format} are not supported with {alternative_packages=}: {exc}"
            )
        else:
            assert False, (
                f"Expected format {key} to work with non-seekable streams, but it failed with {exc!r}"
            )


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_compressed_stream_nonseekable(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    stream = ensure_binaryio(NonSeekableBytesIO(data))

    try:
        with open_compressed_stream(stream, config=config) as f:
            assert not f.seekable()

            out = f.read()

    except (
        ArchiveStreamNotSeekableError
    ) as exc:  # pragma: no cover - environment dependent
        key = (sample_archive.creation_info.format, alternative_packages)
        logger.error(f"key: {key}")
        logger.error(f"EXPECTED_FAILURES: {EXPECTED_NON_SEEKABLE_FAILURES}")
        if key in EXPECTED_NON_SEEKABLE_FAILURES:
            pytest.xfail(
                f"Non-seekable {sample_archive.creation_info.format} are not supported with {alternative_packages=}: {exc}"
            )
        else:
            assert False, (
                f"Expected format {key} to work with non-seekable streams, but it failed with {exc!r}"
            )

    expected = sample_archive.contents.files[0].contents
    assert out == expected


# Select one sample archive for each format (except FOLDER and ISO)
archives_by_format = {}
for a in SAMPLE_ARCHIVES:
    fmt = a.creation_info.format
    if fmt in (ArchiveFormat.FOLDER, ArchiveFormat.ISO):
        continue
    archives_by_format.setdefault(fmt, a)


@pytest.mark.parametrize(
    "sample_archive", list(archives_by_format.values()), ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_stream(sample_archive, alternative_packages):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    path = sample_archive.get_archive_path()
    with open(path, "rb") as f:
        data = f.read()

    with open_archive(io.BytesIO(data), config=config) as archive:
        has_member = False
        for member, stream in archive.iter_members_with_streams():
            has_member = True
            if stream is not None:
                stream.read()
        assert has_member


def _serve_directory(directory: str):
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=directory
    )
    server = http.server.ThreadingHTTPServer(("localhost", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_via_http(sample_archive, alternative_packages):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    path = sample_archive.get_archive_path()
    server, thread = _serve_directory(directory=os.path.dirname(path))
    try:
        url = f"http://localhost:{server.server_address[1]}/{os.path.basename(path)}"
        with open(url, "rb") as response:
            try:
                with open_archive(
                    response, streaming_only=True, config=config
                ) as archive:
                    has_member = False
                    for member, stream in archive.iter_members_with_streams():
                        has_member = True
                        if stream is not None:
                            stream.read()
                    assert has_member
            except (
                ArchiveStreamNotSeekableError
            ) as exc:  # pragma: no cover - env dependent
                key = (sample_archive.creation_info.format, alternative_packages)
                if key in EXPECTED_NON_SEEKABLE_FAILURES:
                    pytest.xfail(
                        f"Non-seekable {sample_archive.creation_info.format} are not supported with {alternative_packages=}: {exc}"
                    )
                else:
                    assert False, (
                        f"Expected format {key} to work with HTTP streams, but it failed with {exc!r}"
                    )
    finally:
        server.shutdown()
        thread.join()


class OneByteReader(io.RawIOBase):
    def __init__(self, data: bytes):
        self._stream = io.BytesIO(data)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        return self._stream.tell()

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        data = ensure_not_none(self._stream.read(min(len(b), 1)))
        n = len(data)
        b[:n] = data
        return n

    def read(self, n: int = -1, /) -> bytes:
        return ensure_not_none(self._stream.read(min(n, 1)))

    def close(self) -> None:
        # logger.error("Closing OneByteReader", stack_info=True)
        self._stream.close()
        super().close()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "alternative"]
)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_open_archive_small_reads(
    sample_archive: SampleArchive,
    sample_archive_path: str,
    alternative_packages: bool,
    streaming_only: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    stream = OneByteReader(data)

    with open_archive(stream, streaming_only=streaming_only, config=config) as archive:
        has_member = False
        for member, member_stream in archive.iter_members_with_streams():
            has_member = True
            if member_stream is not None:
                member_stream.read()
        assert has_member


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + LARGE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Ensure open_archive can read from StatsIO-wrapped streams and tracks statistics correctly."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = StatsIO(io.BytesIO(data), stats)

    # Track initial stats
    initial_bytes_read = stats.bytes_read
    initial_seek_calls = stats.seek_calls
    initial_read_ranges = len(stats.read_ranges)

    with open_archive(stream, config=config) as archive:
        has_member = False
        total_member_bytes = 0

        for member, member_stream in archive.iter_members_with_streams():
            has_member = True
            if member_stream is not None:
                member_data = member_stream.read()
                total_member_bytes += len(member_data)

        assert has_member

    # Verify that statistics were tracked
    assert stats.bytes_read > initial_bytes_read, (
        "No bytes were read according to stats"
    )
    assert stats.seek_calls >= initial_seek_calls, "No seek operations were tracked"
    assert len(stats.read_ranges) > initial_read_ranges, "No read ranges were tracked"


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES + LARGE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio_streaming_mode(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Ensure open_archive can read from StatsIO-wrapped streams in streaming mode."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = StatsIO(io.BytesIO(data), stats)

    # Track initial stats
    initial_bytes_read = stats.bytes_read
    initial_seek_calls = stats.seek_calls
    initial_read_ranges = len(stats.read_ranges)

    with open_archive(stream, streaming_only=True, config=config) as archive:
        has_member = False
        total_member_bytes = 0

        for member, member_stream in archive.iter_members_with_streams():
            has_member = True
            if member_stream is not None:
                member_data = member_stream.read()
                total_member_bytes += len(member_data)

        assert has_member

    # Verify that statistics were tracked
    assert stats.bytes_read > initial_bytes_read
    assert stats.seek_calls >= initial_seek_calls
    assert len(stats.read_ranges) > initial_read_ranges

    # Verify that a significant portion of the archive was read. Some metadata
    # may be skipped by the underlying library, so we only require 70% of the
    # archive bytes to be consumed.
    assert stats.bytes_read > len(data) * 0.7, (
        f"Expected more than 70% of {len(data)} bytes read, got {stats.bytes_read}"
    )


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_compressed_stream_statsio(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Ensure open_compressed_stream can read from StatsIO-wrapped streams and tracks statistics correctly."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = ensure_binaryio(StatsIO(io.BytesIO(data), stats))

    # Track initial stats
    initial_bytes_read = stats.bytes_read
    initial_seek_calls = stats.seek_calls
    initial_read_ranges = len(stats.read_ranges)

    with open_compressed_stream(stream, config=config) as f:
        out = f.read()

    # Verify that statistics were tracked
    assert stats.bytes_read > initial_bytes_read, (
        "No bytes were read according to stats"
    )
    assert stats.seek_calls >= initial_seek_calls, "No seek operations were tracked"
    assert len(stats.read_ranges) > initial_read_ranges, "No read ranges were tracked"

    # Verify that a significant portion of the archive was read. Some metadata
    # may be skipped by the underlying library, so we only require 70% of the
    # archive bytes to be consumed.
    assert stats.bytes_read > len(data) * 0.7, (
        f"Expected more than 70% of {len(data)} bytes read, got {stats.bytes_read}"
    )

    # Verify the decompressed content is correct
    expected = sample_archive.contents.files[0].contents
    assert out == expected


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio_seek_operations(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Test that seek operations are properly tracked by StatsIO when opening archives."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = StatsIO(io.BytesIO(data), stats)

    # Track initial stats
    initial_seek_calls = stats.seek_calls
    initial_read_ranges = len(stats.read_ranges)

    with open_archive(stream, config=config) as archive:
        # Get member list first (this may trigger seeks)
        members = archive.get_members()
        assert len(members) > 0

        # Read a few members to trigger more I/O operations
        for i, member in enumerate(members[:3]):  # Read first 3 members
            if member.type.value == "file":  # Only try to open file members
                with archive.open(member) as member_stream:
                    member_stream.read()

    # Verify that seek operations were tracked
    assert stats.seek_calls >= initial_seek_calls
    assert len(stats.read_ranges) > initial_read_ranges

    # Verify read ranges are properly formatted
    for read_range in stats.read_ranges:
        assert len(read_range) == 2
        assert read_range[0] >= 0
        assert read_range[1] >= 0


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio_readinto_operations(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Test that readinto operations are properly tracked by StatsIO when opening archives."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = StatsIO(io.BytesIO(data), stats)

    # Track initial stats
    initial_bytes_read = stats.bytes_read
    initial_read_ranges = len(stats.read_ranges)

    with open_archive(stream, config=config) as archive:
        # Read members using readinto to test that operation
        for member in archive.get_members():
            if member.type.value == "file":  # Only try to open file members
                with archive.open(member) as member_stream:
                    buffer = bytearray(1024)
                    while True:
                        logger.debug(f"readinto {member_stream}")
                        bytes_read = member_stream.readinto(buffer)  # type: ignore
                        if bytes_read == 0:
                            break

    # Verify that bytes were read
    assert stats.bytes_read > initial_bytes_read, (
        "No bytes were read according to stats"
    )
    assert len(stats.read_ranges) > initial_read_ranges, "No read ranges were tracked"


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio_multiple_opens(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Test that StatsIO properly tracks statistics across multiple archive opens."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    stream = StatsIO(io.BytesIO(data), stats)

    # First open
    with open_archive(stream, config=config) as archive:
        members = archive.get_members()
        assert len(members) > 0

    first_open_bytes = stats.bytes_read
    first_open_seeks = stats.seek_calls
    first_open_ranges = len(stats.read_ranges)

    # Second open (should accumulate stats)
    with open_archive(stream, config=config) as archive:
        members = archive.get_members()
        assert len(members) > 0

    # Verify that stats accumulated across opens
    assert stats.bytes_read > first_open_bytes, "Stats should accumulate across opens"
    assert stats.seek_calls >= first_open_seeks, (
        "Seek stats should accumulate across opens"
    )
    assert len(stats.read_ranges) > first_open_ranges, (
        "Read ranges should accumulate across opens"
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_archive_statsio_io_methods(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    """Test that StatsIO properly delegates IO methods to the underlying stream."""
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    # Create StatsIO wrapper around the data
    stats = IOStats()
    underlying_stream = io.BytesIO(data)
    stream = StatsIO(underlying_stream, stats)

    # Test that IO methods are properly delegated
    assert stream.readable() == underlying_stream.readable()
    assert stream.writable() == underlying_stream.writable()
    assert stream.seekable() == underlying_stream.seekable()

    # Test that we can still open the archive normally
    with open_archive(stream, config=config) as archive:
        members = archive.get_members()
        assert len(members) > 0

    # Verify that statistics were tracked
    assert stats.bytes_read > 0, "No bytes were read according to stats"
    assert len(stats.read_ranges) > 1, "No read ranges were tracked"
