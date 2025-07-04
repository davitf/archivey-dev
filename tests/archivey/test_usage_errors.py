import logging

import pytest

from archivey.core import open_archive
from archivey.exceptions import ArchiveMemberCannotBeOpenedError
from archivey.types import ArchiveMember, MemberType
from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    SYMLINK_ARCHIVES,
    SampleArchive,
)
from tests.archivey.testing_utils import skip_if_package_missing

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("sample_archive", BASIC_ARCHIVES, ids=lambda a: a.filename)
def test_get_operations_after_close(
    sample_archive: SampleArchive, sample_archive_path: str
) -> None:
    """Calling get_archive_info after closing should raise an error."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    archive = open_archive(sample_archive_path)
    assert archive.get_archive_info() is not None
    archive.close()

    with pytest.raises(ValueError):
        archive.get_archive_info()

    with pytest.raises(ValueError):
        list(archive.iter_members_with_io())

    with pytest.raises(ValueError):
        list(archive.get_members())

    with pytest.raises(ValueError):
        list(archive.extractall(path="/tmp"))


@pytest.mark.parametrize("sample_archive", BASIC_ARCHIVES, ids=lambda a: a.filename)
def test_open_member_from_another_archive(
    sample_archive: SampleArchive, sample_archive_path: str
) -> None:
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # Open the archive twice
    with (
        open_archive(sample_archive_path) as archive1,
        open_archive(sample_archive_path) as archive2,
    ):
        first_file = archive1.get_member("file1.txt")
        assert first_file.type == MemberType.FILE

        with pytest.raises(ValueError):
            archive2.open(first_file)


@pytest.mark.parametrize("sample_archive", BASIC_ARCHIVES, ids=lambda a: a.filename)
def test_open_dir_member(
    sample_archive: SampleArchive, sample_archive_path: str
) -> None:
    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        first_dir = archive.get_member("subdir/")
        assert first_dir.type == MemberType.DIR

        with pytest.raises(ArchiveMemberCannotBeOpenedError):
            archive.open(first_dir)


@pytest.mark.parametrize("sample_archive", SYMLINK_ARCHIVES, ids=lambda a: a.filename)
def test_resolve_link_non_registered_member(
    sample_archive: SampleArchive, sample_archive_path: str
) -> None:
    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        member = ArchiveMember(
            filename="dangling",
            file_size=None,
            compress_size=None,
            mtime_with_tz=None,
            type=MemberType.SYMLINK,
            link_target="file1.txt",
        )
        with pytest.raises(ValueError):
            archive.resolve_link(member)
