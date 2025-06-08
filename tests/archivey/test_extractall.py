import os
from datetime import datetime
from pathlib import Path

import pytest

from archivey.core import open_archive
from archivey.types import ArchiveFormat, MemberType
from tests.archivey.testing_utils import skip_if_package_missing
from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    DUPLICATE_FILES_ARCHIVES,
    SampleArchive,
)


def _check_file_metadata(path: Path, info, sample):
    stat = path.lstat() if info.type == MemberType.LINK else path.stat()
    features = sample.creation_info.features

    if info.permissions is not None:
        assert (stat.st_mode & 0o777) == info.permissions, path

    if not features.mtime:
        return

    actual = datetime.fromtimestamp(stat.st_mtime)
    if features.rounded_mtime:
        assert abs(actual.timestamp() - info.mtime.timestamp()) <= 1, path
    else:
        assert actual == info.mtime, path


@pytest.mark.parametrize(
    "sample_archive",
    BASIC_ARCHIVES + DUPLICATE_FILES_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_extractall(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        skip_if_package_missing(sample_archive.creation_info.format, None)
    if (
        sample_archive.contents.file_basename == "duplicate_files"
        and "tarcmd" in sample_archive.filename
    ):
        pytest.skip("tar command line duplicates not supported")
    if sample_archive.filename == "duplicate_files__tarfile.tar.gz":
        pytest.skip("tarfile gz duplicate handling not implemented")

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample_archive_path) as archive:
        archive.extractall(dest)

    final_entries = {}
    for entry in sample_archive.contents.files:
        final_entries[entry.name] = entry

    for info in final_entries.values():
        path = dest / info.name.rstrip("/")
        assert path.exists(), f"Missing {path}"
        if info.type == MemberType.DIR:
            assert path.is_dir()
        elif info.type == MemberType.LINK:
            assert path.is_symlink()
            assert os.readlink(path) == info.link_target
        else:
            assert path.is_file()
            with open(path, "rb") as f:
                assert f.read() == (info.contents or b"")

        _check_file_metadata(path, info, sample_archive)

    extracted = {str(p.relative_to(dest)).replace(os.sep, "/") for p in dest.rglob("*")}
    expected = {name.rstrip("/") for name in final_entries}
    assert expected <= extracted


@pytest.mark.parametrize(
    "sample_archive",
    BASIC_ARCHIVES + DUPLICATE_FILES_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_extractall_filter(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        skip_if_package_missing(sample_archive.creation_info.format, None)

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample_archive_path) as archive:
        archive.extractall(dest, filter=lambda m: m.filename.endswith("file2.txt"))

    final_entries = {f.name: f for f in sample_archive.contents.files}
    target = (
        "subdir/file2.txt"
        if "subdir/file2.txt" in final_entries
        else "file2.txt"
    )
    path = dest / Path(target)
    assert path.exists() and path.is_file()
    info = final_entries[target]
    with open(path, "rb") as f:
        assert f.read() == (info.contents or b"")
    _check_file_metadata(path, info, sample_archive)

    assert not (dest / "file1.txt").exists()
    assert not (dest / "implicit_subdir" / "file3.txt").exists()


@pytest.mark.parametrize(
    "sample_archive",
    BASIC_ARCHIVES + DUPLICATE_FILES_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_extractall_members(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        skip_if_package_missing(sample_archive.creation_info.format, None)
    if sample_archive.contents.file_basename == "duplicate_files":
        pytest.skip("extractall_members not reliable with duplicate files")

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample_archive_path) as archive:
        final_entries = {f.name: f for f in sample_archive.contents.files}
        second_name = (
            "subdir/file2.txt" if "subdir/file2.txt" in final_entries else "file2.txt"
        )
        members = archive.get_members()
        file1_members = [m for m in members if m.filename == "file1.txt"]
        member1 = file1_members[-1] if file1_members else "file1.txt"
        archive.extractall(dest, members=[member1, second_name])

    expected_paths = [dest / "file1.txt", dest / Path(second_name)]
    for p in expected_paths:
        assert p.exists() and p.is_file()
        info = final_entries[str(p.relative_to(dest)).replace(os.sep, "/")]
        with open(p, "rb") as f:
            assert f.read() == (info.contents or b"")
        _check_file_metadata(p, info, sample_archive)

    assert not (dest / "implicit_subdir" / "file3.txt").exists()
