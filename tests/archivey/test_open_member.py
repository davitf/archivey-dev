import pytest

from archivey.core import open_archive
from tests.archivey.sample_archives import (
    ALTERNATIVE_CONFIG,
    BASIC_ARCHIVES,
    HARDLINK_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    SYMLINK_ARCHIVES,
    SampleArchive,
)
from tests.archivey.testing_utils import remove_duplicate_files, skip_if_package_missing


@pytest.mark.parametrize(
    "sample_archive",
    BASIC_ARCHIVES + SYMLINK_ARCHIVES + HARDLINK_ARCHIVES,
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_member(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open_archive(sample_archive_path, config=config) as archive:
        # members = archive.get_members()

        for sample_file in remove_duplicate_files(sample_archive.contents.files):
            if sample_file.contents is not None:
                stream = archive.open(sample_file.name)
                data = stream.read()
                assert data == sample_file.contents


@pytest.mark.parametrize(
    "sample_archive", SINGLE_FILE_ARCHIVES, ids=lambda a: a.filename
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_open_member_single_file_archives(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)

    with open_archive(sample_archive_path, config=config) as archive:
        member = archive.get_members()[0]

        stream = archive.open(member)
        data = stream.read()
        assert data == sample_archive.contents.files[0].contents
