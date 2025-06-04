from __future__ import annotations

from pathlib import Path

import pytest
from sample_archives import BASIC_FILES, ENCODING_FILES, SYMLINK_FILES

from archivey.core import open_archive
from archivey.types import ArchiveFormat, MemberType
from tests.archivey.testing_utils import write_files_to_dir


@pytest.mark.parametrize(
    "files",
    [BASIC_FILES, SYMLINK_FILES, ENCODING_FILES],
    ids=["basic", "symlinks", "encodings"],
)
def test_folder_reader(tmp_path: Path, files: list):
    folder = tmp_path / "folder"
    write_files_to_dir(folder, files)

    with open_archive(folder) as archive:
        assert archive.format == ArchiveFormat.FOLDER
        members = {m.filename: m for m in archive.get_members()}

        expected_names = {f.name.rstrip("/") for f in files}
        assert expected_names.issubset(set(members))

        for file in files:
            member = members[file.name.rstrip("/")]
            assert member.type == file.type
            assert member.mtime == file.mtime

            if file.type == MemberType.LINK:
                assert member.link_target == file.link_target
            elif file.type == MemberType.FILE:
                with archive.open(member) as fh:
                    assert fh.read() == file.contents
