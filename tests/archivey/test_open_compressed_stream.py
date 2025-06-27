import logging

import pytest

from archivey.api.core import open_compressed_stream
from archivey.api.config import ArchiveyConfig
from archivey.api.exceptions import ArchiveNotSupportedError
from tests.archivey.sample_archives import SAMPLE_ARCHIVES, filter_archives
from tests.archivey.testing_utils import skip_if_package_missing


# Select single-file archives for testing
SINGLE_FILE_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
)

BASIC_ZIP_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["zip"]
)[0]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename)
@pytest.mark.parametrize("alternative_packages", [False, True], ids=["default", "altlibs"])
def test_open_compressed_stream(sample_archive, sample_archive_path, alternative_packages):
    if alternative_packages:
        config = ArchiveyConfig(
            use_rapidgzip=True,
            use_indexed_bzip2=True,
            use_python_xz=True,
            use_zstandard=True,
        )
    else:
        config = ArchiveyConfig()

    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open_compressed_stream(sample_archive_path, config=config) as f:
        data = f.read()

    expected = sample_archive.contents.files[0].contents
    assert data == expected


def test_open_compressed_stream_wrong_format(tmp_path):
    sample_archive = BASIC_ZIP_ARCHIVE
    skip_if_package_missing(sample_archive.creation_info.format, None)
    path = sample_archive.get_archive_path()
    with pytest.raises(ArchiveNotSupportedError):
        open_compressed_stream(path)
