import os

import pytest

from tests.archivey.sample_archives import SAMPLE_ARCHIVES, ArchiveInfo, filter_archives
from tests.archivey.test_read_archives import check_iter_members


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["fixture_zip", "fixture_tar"]),
    ids=lambda a: a.filename,
)
def test_fixture_generates_archives(sample_archive: ArchiveInfo, sample_archive_path: str):
    assert os.path.exists(sample_archive_path)
    check_iter_members(sample_archive, archive_path=sample_archive_path)
