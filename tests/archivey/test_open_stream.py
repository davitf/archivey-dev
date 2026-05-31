import io

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.types import ArchiveFormat
from tests.archivey.sample_archives import SAMPLE_ARCHIVES, SampleArchive
from tests.archivey.testing_utils import skip_if_package_missing

# Select one sample archive for each format (except FOLDER and ISO)
archives_by_format = {}
for a in SAMPLE_ARCHIVES:
    fmt = a.creation_info.format
    if fmt in (ArchiveFormat.FOLDER, ArchiveFormat.ISO):
        continue
    archives_by_format.setdefault(fmt, a)


@pytest.mark.sample_archives(list(archives_by_format.values()))
def test_open_stream(
    sample_archive: SampleArchive,
    archive_config: ArchiveyConfig,
):
    skip_if_package_missing(sample_archive.creation_info.format, archive_config)

    path = sample_archive.get_archive_path()
    with open(path, "rb") as f:
        data = f.read()

    with open_archive(io.BytesIO(data), config=archive_config) as archive:
        has_member = False
        for member, stream in archive.iter_members_with_streams():
            has_member = True
            if stream is not None:
                stream.read()
        assert has_member
