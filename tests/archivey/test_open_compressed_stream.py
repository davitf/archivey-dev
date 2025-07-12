import io
import logging

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_compressed_stream
from archivey.exceptions import ArchiveNotSupportedError
from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing

BASIC_ZIP_ARCHIVE = filter_archives(BASIC_ARCHIVES, extensions=["zip"])[0]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_compressed_stream_from_file(
    sample_archive, sample_archive_path, alternative_packages
):
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


def test_open_compressed_stream_unsupported_format(tmp_path):
    sample_archive = BASIC_ZIP_ARCHIVE
    skip_if_package_missing(sample_archive.creation_info.format, None)
    path = sample_archive.get_archive_path()
    with pytest.raises(ArchiveNotSupportedError):
        open_compressed_stream(path)


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_compressed_stream_from_stream(
    sample_archive, sample_archive_path, alternative_packages
):
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

    compressed_data = open(sample_archive_path, "rb").read()
    compressed_stream = io.BytesIO(compressed_data)

    with open_compressed_stream(compressed_stream, config=config) as f:
        data = f.read()

    expected = sample_archive.contents.files[0].contents
    assert data == expected


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_compressed_stream_from_stream_with_prefix(
    sample_archive, sample_archive_path, alternative_packages
):
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

    # Add some bad data to the beginning of the stream, to test that the reading is
    # done from the initial position.
    bad_data = b"bad data " * 1000
    compressed_data = bad_data + open(sample_archive_path, "rb").read()
    compressed_stream = io.BytesIO(compressed_data)
    compressed_stream.seek(len(bad_data))

    with open_compressed_stream(compressed_stream, config=config) as f:
        data = f.read()

    expected = sample_archive.contents.files[0].contents
    assert data == expected
