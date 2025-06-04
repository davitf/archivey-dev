import os
import tempfile
import pathlib
import pytest

if not hasattr(pathlib.Path, "walk"):
    def _walk(self, top_down=True, on_error=None, follow_symlinks=False):
        for root, dirs, files in os.walk(
            self, topdown=top_down, onerror=on_error, followlinks=follow_symlinks
        ):
            yield pathlib.Path(root), dirs, files

    pathlib.Path.walk = _walk

from archivey.core import open_archive
from archivey.types import ArchiveFormat, MemberType
from sample_archives import BASIC_FILES, SYMLINK_FILES, ENCODING_FILES
from utils import write_files_to_dir


FILES_SETS = [BASIC_FILES, SYMLINK_FILES, ENCODING_FILES]


def check_folder(dir_path: str, files):
    with open_archive(dir_path) as archive:
        assert archive.format == ArchiveFormat.FOLDER
        members = {m.filename: m for m in archive.get_members()}
        file_map = {f.name.rstrip('/'): f for f in files}

        for name, member in members.items():
            info = file_map.get(name)
            if info is None:
                if member.type == MemberType.DIR:
                    continue
                else:
                    assert False, f"Unexpected file {name}"

            assert member.type == info.type
            if info.type == MemberType.LINK:
                assert member.link_target == info.link_target
            if info.type == MemberType.FILE:
                with archive.open(member) as stream:
                    assert stream.read() == (info.contents or b"")

        assert set(file_map) <= set(members)


@pytest.mark.parametrize("files", FILES_SETS)
def test_folder_reader(files):
    with tempfile.TemporaryDirectory() as tmpdir:
        write_files_to_dir(tmpdir, files)
        check_folder(tmpdir, files)
