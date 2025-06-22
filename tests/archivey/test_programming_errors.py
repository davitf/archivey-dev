import struct
import zipfile
from datetime import datetime, timezone

import pytest

from archivey.core import open_archive
from archivey.types import ArchiveMember, MemberType, ArchiveFormat
from tests.archivey.sample_archives import BASIC_ARCHIVES, SampleArchive
from tests.archivey.testing_utils import skip_if_package_missing


@pytest.mark.parametrize("sample_archive", BASIC_ARCHIVES, ids=lambda a: a.filename)
def test_get_archive_info_after_close(sample_archive: SampleArchive, sample_archive_path: str) -> None:
    """Calling get_archive_info after closing should raise for most formats."""
    skip_if_package_missing(sample_archive.creation_info.format, None)
    archive = open_archive(sample_archive_path)
    try:
        assert archive.get_archive_info() is not None
        archive.close()
        if sample_archive.creation_info.format == ArchiveFormat.FOLDER:
            archive.get_archive_info()
        else:
            with pytest.raises(Exception):
                archive.get_archive_info()
    finally:
        archive.close()


@pytest.mark.parametrize("sample_archive", [BASIC_ARCHIVES[0]], ids=lambda a: a.filename)
def test_resolve_link_symlink_without_target(sample_archive: SampleArchive, sample_archive_path: str) -> None:
    skip_if_package_missing(sample_archive.creation_info.format, None)
    with open_archive(sample_archive_path) as archive:
        member = ArchiveMember(
            filename="dangling",
            file_size=None,
            compress_size=None,
            mtime_with_tz=None,
            type=MemberType.SYMLINK,
            link_target=None,
        )
        assert archive.resolve_link(member) is member


@pytest.mark.parametrize("sample_archive", [BASIC_ARCHIVES[0]], ids=lambda a: a.filename)
def test_resolve_link_non_registered_member(sample_archive: SampleArchive, sample_archive_path: str) -> None:
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


@pytest.mark.parametrize("sample_archive", [BASIC_ARCHIVES[0]], ids=lambda a: a.filename)
def test_resolve_link_regular_file(sample_archive: SampleArchive, sample_archive_path: str) -> None:
    skip_if_package_missing(sample_archive.creation_info.format, None)
    with open_archive(sample_archive_path) as archive:
        member = archive.get_member("file1.txt")
        assert archive.resolve_link(member) is member


def test_zip_extra_field_before_timestamp(tmp_path) -> None:
    path = tmp_path / "extra.zip"
    modtime = int(datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp())
    zi = zipfile.ZipInfo("file.txt", date_time=(2020, 1, 2, 3, 4, 5))
    zi.extra = struct.pack("<HH4s", 0x1234, 4, b"abcd") + struct.pack("<HHB", 0x5455, 5, 1) + struct.pack("<I", modtime)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(zi, b"data")
    with open_archive(str(path)) as archive:
        info = archive.get_members()[0]
        assert info.mtime_with_tz == datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
