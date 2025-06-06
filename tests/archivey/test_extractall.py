import os
from pathlib import Path

import pytest

from archivey.core import open_archive
from datetime import datetime
from archivey.types import ArchiveFormat, MemberType
from tests.archivey.sample_archives import SAMPLE_ARCHIVES


def _get_sample(name: str):
    for a in SAMPLE_ARCHIVES:
        if a.filename == name:
            return a
    raise ValueError(name)


def _check_file_metadata(path: Path, info, sample):
    stat = path.lstat() if info.type == MemberType.LINK else path.stat()
    features = sample.creation_info.features

    if info.permissions is not None:
        assert (stat.st_mode & 0o777) == info.permissions

    if not features.mtime:
        return

    actual = datetime.fromtimestamp(stat.st_mtime)
    if features.rounded_mtime:
        assert abs(actual.timestamp() - info.mtime.timestamp()) <= 1
    else:
        assert actual == info.mtime


@pytest.mark.parametrize(
    "filename",
    ["basic_nonsolid__zipfile.zip", "basic_nonsolid__py7zr.7z"],
)
def test_extractall(tmp_path: Path, filename: str):
    sample = _get_sample(filename)
    if sample.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample.get_archive_path()) as archive:
        archive.extractall(dest)

    for info in sample.contents.files:
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

        _check_file_metadata(path, info, sample)

    extracted = {str(p.relative_to(dest)).replace(os.sep, "/") for p in dest.rglob("*")}
    expected = {f.name.rstrip("/") for f in sample.contents.files}
    assert expected <= extracted


@pytest.mark.parametrize(
    "filename",
    ["basic_nonsolid__zipfile.zip", "basic_nonsolid__py7zr.7z"],
)
def test_extractall_filter(tmp_path: Path, filename: str):
    sample = _get_sample(filename)
    if sample.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample.get_archive_path()) as archive:
        archive.extractall(dest, filter=lambda m: m.filename.endswith("file2.txt"))

    path = dest / "subdir" / "file2.txt"
    assert path.exists() and path.is_file()
    info = next(f for f in sample.contents.files if f.name == "subdir/file2.txt")
    with open(path, "rb") as f:
        assert f.read() == (info.contents or b"")
    _check_file_metadata(path, info, sample)

    assert not (dest / "file1.txt").exists()
    assert not (dest / "implicit_subdir" / "file3.txt").exists()


@pytest.mark.parametrize(
    "filename",
    ["basic_nonsolid__zipfile.zip", "basic_nonsolid__py7zr.7z"],
)
def test_extractall_members(tmp_path: Path, filename: str):
    sample = _get_sample(filename)
    if sample.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")

    dest = tmp_path / "out"
    dest.mkdir()

    with open_archive(sample.get_archive_path()) as archive:
        member_obj = archive.getinfo("file1.txt")
        archive.extractall(dest, members=[member_obj, "subdir/file2.txt"])

    expected_paths = [dest / "file1.txt", dest / "subdir" / "file2.txt"]
    for p in expected_paths:
        assert p.exists() and p.is_file()
        info = next(f for f in sample.contents.files if f.name == str(p.relative_to(dest)).replace(os.sep, "/"))
        with open(p, "rb") as f:
            assert f.read() == (info.contents or b"")
        _check_file_metadata(p, info, sample)

    assert not (dest / "implicit_subdir" / "file3.txt").exists()
