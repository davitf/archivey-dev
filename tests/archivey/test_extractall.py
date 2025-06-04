import os
from pathlib import Path

import pytest
from sample_archives import SAMPLE_ARCHIVES

from archivey.core import open_archive
from archivey.types import ArchiveFormat, MemberType


def _get_sample(name: str):
    for a in SAMPLE_ARCHIVES:
        if a.filename == name:
            return a
    raise ValueError(name)


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

    extracted = {str(p.relative_to(dest)).replace(os.sep, "/") for p in dest.rglob("*")}
    expected = {f.name.rstrip("/") for f in sample.contents.files}
    assert expected <= extracted
