import io

import pytest

from archivey.api.config import ArchiveyConfig
from archivey.api.core import open_compressed_stream
from tests.archivey.sample_archives import SAMPLE_ARCHIVES, filter_archives
from tests.archivey.testing_utils import skip_if_package_missing

SINGLE_FILE_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
)


class NonSeekableBytesIO(io.BytesIO):
    def seekable(self) -> bool:  # pragma: no cover - simple
        return False

    def seek(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("seek")

    def tell(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("tell")


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
def test_open_compressed_stream_nonseekable(sample_archive, sample_archive_path):
    config = ArchiveyConfig()

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open(sample_archive_path, "rb") as f:
        data = f.read()

    stream = NonSeekableBytesIO(data)

    with open_compressed_stream(stream, config=config) as f:
        out = f.read()

    expected = sample_archive.contents.files[0].contents
    assert out == expected
