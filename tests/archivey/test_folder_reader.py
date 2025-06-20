from __future__ import annotations

from pathlib import Path

import sys # Added for sys.platform
from datetime import datetime # Added for type checking

import pytest

from archivey.core import open_archive
from archivey.types import ArchiveFormat, MemberType, CreateSystem # Added CreateSystem
from tests.archivey.sample_archives import BASIC_FILES, ENCODING_FILES, SYMLINKS_FILES
from tests.archivey.testing_utils import write_files_to_dir


@pytest.mark.parametrize(
    "files",
    [BASIC_FILES, SYMLINKS_FILES, ENCODING_FILES],
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
            assert member.mtime == file.mtime # mtime is already checked

            # Mode check (permissions part)
            if member.mode is not None:
                assert isinstance(member.mode, int)
                # Check if it looks like permission bits (e.g. last 9 bits for basic perms)
                # This is a loose check as exact mode depends on umask during test file creation.
                assert member.mode & 0o777 == member.mode & 0o7777

            # CreateSystem check
            assert isinstance(member.create_system, CreateSystem)
            if sys.platform == "win32":
                assert member.create_system == CreateSystem.NTFS
            elif sys.platform == "darwin":
                assert member.create_system == CreateSystem.MACINTOSH
            elif sys.platform.startswith("linux"):
                assert member.create_system == CreateSystem.UNIX
            # Add other common platform checks if necessary, or leave as UNKNOWN

            # UID and GID check
            assert isinstance(member.uid, int)
            assert member.uid >= 0 # Basic sanity check
            assert isinstance(member.gid, int)
            assert member.gid >= 0 # Basic sanity check

            # Access and Creation time checks
            assert isinstance(member.atime_with_tz, datetime)
            assert member.atime_with_tz.tzinfo is not None
            assert isinstance(member.ctime_with_tz, datetime)
            assert member.ctime_with_tz.tzinfo is not None

            assert isinstance(member.atime, datetime)
            assert member.atime.tzinfo is None
            assert isinstance(member.ctime, datetime)
            assert member.ctime.tzinfo is None


            # User and Group name checks
            if sys.platform == "win32":
                # On Windows, these are often None as pwd/grp modules are not available
                assert member.user_name is None, f"Expected user_name to be None on Windows, got {member.user_name}"
                assert member.group_name is None, f"Expected group_name to be None on Windows, got {member.group_name}"
            else:
                # On Unix-like systems, these should ideally be strings
                # However, in some CI environments or minimal Docker images,
                # the user/group might not resolve, leading to UID/GID being used as string.
                # So, we check if it's a string, or if it's None (less likely but possible if lookup fails)
                assert member.user_name is None or isinstance(member.user_name, str)
                assert member.group_name is None or isinstance(member.group_name, str)


            if file.type == MemberType.SYMLINK:
                assert member.link_target == file.link_target
            elif file.type == MemberType.FILE:
                with archive.open(member) as fh:
                    assert fh.read() == file.contents
