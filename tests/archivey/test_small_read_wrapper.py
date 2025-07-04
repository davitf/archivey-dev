import io

import pytest

from archivey.core import open_archive
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing


class OneByteReader(io.BytesIO):
    def read(self, n: int = -1) -> bytes:  # type: ignore[override]
        if n == -1:
            return super().read()
        if n == 0:
            return b""
        return super().read(1)

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        BASIC_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format not in (ArchiveFormat.FOLDER,),
    ),
    ids=lambda a: a.filename,
)
def test_open_archive_small_reads(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)
    with open(sample_archive_path, "rb") as f:
        data = f.read()

    stream = OneByteReader(data)
    with open_archive(stream) as archive:
        has_member = False
        for member, member_stream in archive.iter_members_with_io():
            has_member = True
            if member_stream is not None:
                member_stream.read()
        assert has_member
