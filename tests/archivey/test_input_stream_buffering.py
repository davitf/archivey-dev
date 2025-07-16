import io
import logging

import pytest

from archivey.core import open_archive
from archivey.internal.utils import ensure_not_none
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import (
    ALTERNATIVE_CONFIG,
    BASIC_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing

logger = logging.getLogger(__name__)


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
        for member, member_stream in archive.iter_members_with_io():
            has_member = True
            if member_stream is not None:
                member_stream.read()
        assert has_member
