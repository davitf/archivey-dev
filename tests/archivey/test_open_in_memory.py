import io

import pytest

from archivey.api.core import open_archive
from archivey.api.types import ArchiveFormat
from tests.archivey.sample_archives import SAMPLE_ARCHIVES
from tests.archivey.testing_utils import skip_if_package_missing

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
def test_open_from_memory(sample_archive):
    skip_if_package_missing(sample_archive.creation_info.format, None)
    path = sample_archive.get_archive_path()
    with open(path, "rb") as f:
        data = f.read()

    with open_archive(io.BytesIO(data)) as archive:
        has_member = False
        for member, stream in archive.iter_members_with_io():
            has_member = True
            if stream is not None:
                stream.read()
        assert has_member
