import os
import pathlib
import stat
import tarfile
import zipfile
from datetime import datetime, timezone

import pytest

from archivey import open_archive_reader, open_archive_writer
from archivey.types import ArchiveFormat, MemberType

# Helper function to create a temporary file path
def temp_archive_path(tmp_path: pathlib.Path, filename: str) -> str:
    return str(tmp_path / filename)

# Helper function to verify archive contents
def verify_archive_contents(
    archive_path: str,
    expected_members: list[tuple[str, MemberType, str | None, bytes | None]],
    archive_format_str: str # e.g. "zip", "tar" - used for format specific checks
):
    # Convert archive_format_str to ArchiveFormat enum if needed, or adjust checks
    # For now, assume it's mainly for zipfile vs tarfile specific checks like trailing slashes

    with open_archive_reader(archive_path) as reader:
        members = reader.get_members()
        # Adjust for tar potentially not listing parent dirs explicitly if they only contain other dirs
        # or if they are implicitly created. For simplicity, we'll expect explicit dir entries for now.
        assert len(members) == len(expected_members)

        member_map = {m.filename: m for m in members}

        for name, type, link_target, content_bytes in expected_members:
            # Normalize directory names for comparison (tar might not have trailing /)
            # Zipfile often has it for directories.
            # The `open_archive_reader` should ideally normalize this,
            # but we can be a bit flexible here for the purpose of the write test.

            # Check based on type
            if type == MemberType.DIR:
                # For ZIP, directory names in member_map often end with '/'
                # For TAR, they typically don't.
                # We will ensure our writer is consistent, or reader normalizes.
                # For now, let's assume writer adds them as specified (e.g. "subdir/" for zip, "subdir" for tar)
                # and reader presents them consistently.
                # The sample_archives.py uses "dirname/" for dirs.

                # Attempt to find the member, trying with and without trailing slash
                # if archive_format_str == "tar" and name.endswith("/"):
                #     found_member = member_map.get(name.rstrip('/'))
                # elif archive_format_str == "zip" and not name.endswith("/"):
                #     found_member = member_map.get(name + "/")
                # else:
                #     found_member = member_map.get(name)

                # For now, assume the name in expected_members is what we expect from the reader
                assert name in member_map, f"Directory {name} not found in archive."
                found_member = member_map[name]
                assert found_member.is_dir, f"{name} is not a directory."
                # Permissions could be checked if set consistently
                # assert (found_member.mode & 0o777) == (expected_permissions or 0o755)
            elif type == MemberType.FILE:
                assert name in member_map, f"File {name} not found in archive."
                found_member = member_map[name]
                assert found_member.is_file, f"{name} is not a file."
                if content_bytes is not None:
                    with reader.open(found_member) as f:
                        assert f.read() == content_bytes
                # assert (found_member.mode & 0o777) == (expected_permissions or 0o644)
            elif type == MemberType.SYMLINK:
                assert name in member_map, f"Symlink {name} not found in archive."
                found_member = member_map[name]
                assert found_member.is_link, f"{name} is not a symlink."
                assert found_member.link_target == link_target
                # Symlink permissions are tricky; often 0o777 or not stored/relevant in some formats
                # assert (found_member.mode & 0o777) == (expected_permissions or 0o777)

# Test cases for ZIP
@pytest.mark.parametrize("filename", ["test_write.zip"])
def test_create_zip_archive(tmp_path: pathlib.Path, filename: str):
    archive_path = temp_archive_path(tmp_path, filename)

    # mtime for consistency
    fixed_mtime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    expected_members_data = [
        ("file1.txt", MemberType.FILE, None, b"content1", 0o644, fixed_mtime),
        ("emptydir/", MemberType.DIR, None, None, 0o755, fixed_mtime),
        ("dir1/", MemberType.DIR, None, None, 0o755, fixed_mtime),
        ("dir1/file2.txt", MemberType.FILE, None, b"content2", 0o644, fixed_mtime),
        ("link_to_file1.txt", MemberType.SYMLINK, "file1.txt", None, 0o777, fixed_mtime),
    ]

    with open_archive_writer(archive_path) as writer:
        # Add file
        with writer.open("file1.txt") as outfile:
            outfile.write(b"content1")
        # TODO: Set mtime and mode if ArchiveMember in add_member takes them
        # For now, assuming add_member in specific writers will handle ArchiveMember fields

        # Add empty directory
        writer.add("emptydir/", MemberType.DIR)

        # Add directory and file within it
        writer.add("dir1/", MemberType.DIR)
        with writer.open("dir1/file2.txt") as outfile:
            outfile.write(b"content2")

        # Add symlink
        writer.add("link_to_file1.txt", MemberType.SYMLINK, link_target="file1.txt")

    # Verify basic zip structure
    assert os.path.exists(archive_path)
    with zipfile.ZipFile(archive_path, "r") as zf:
        # Basic checks, detailed verification by open_archive_reader later
        assert "file1.txt" in zf.namelist()
        assert "emptydir/" in zf.namelist() # zipfile includes trailing slash for dirs
        assert "dir1/file2.txt" in zf.namelist()
        # Symlink name might vary based on how reader interprets it; ensure consistency

    # Verify with our reader
    verify_archive_contents(archive_path, [
        (name, type, lt, content) for name, type, lt, content, _, _ in expected_members_data
    ], "zip")


# Test cases for TAR
@pytest.mark.parametrize("tar_format_enum, extension", [
    (ArchiveFormat.TAR, ".tar"),
    (ArchiveFormat.TAR_GZ, ".tar.gz"),
    (ArchiveFormat.TAR_BZ2, ".tar.bz2"),
    (ArchiveFormat.TAR_XZ, ".tar.xz"),
    # ZSTD and LZ4 might fail if system libs are not available for tarfile
    # (ArchiveFormat.TAR_ZSTD, ".tar.zst"),
    # (ArchiveFormat.TAR_LZ4, ".tar.lz4"),
])
def test_create_tar_archive(tmp_path: pathlib.Path, tar_format_enum: ArchiveFormat, extension: str):
    archive_path = temp_archive_path(tmp_path, f"test_write{extension}")

    fixed_mtime = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Note: Tar typically doesn't store trailing slashes for directory names in its listing
    # but our ArchiveMember.filename for a dir might include it for consistency.
    # Let's assume the writer handles this (e.g. TarArchiveWriter strips it for TarInfo.name)
    # and the reader presents it consistently (e.g. adds it back if that's our convention).
    # For this test, expected names for TAR will not have trailing slashes for dirs,
    # matching common tar utility output.
    expected_members_data = [
        ("file1.txt", MemberType.FILE, None, b"content1 tar", fixed_mtime),
        ("emptydir", MemberType.DIR, None, None, fixed_mtime), # No trailing slash for tar
        ("dir1", MemberType.DIR, None, None, fixed_mtime),    # No trailing slash
        ("dir1/file2.txt", MemberType.FILE, None, b"content2 tar", fixed_mtime),
        ("link_to_file1.txt", MemberType.SYMLINK, "file1.txt", None, fixed_mtime),
        # ("hardlink_to_file1.txt", MemberType.HARDLINK, "file1.txt", None, fixed_mtime), # Hardlinks need target to exist first
    ]

    with open_archive_writer(archive_path) as writer:
        # Add file
        with writer.open("file1.txt") as outfile:
            outfile.write(b"content1 tar")
        # TODO: Set mtime and mode on ArchiveMember if writer uses it.

        # Add empty directory
        writer.add("emptydir", MemberType.DIR) # No trailing slash needed for tar writer

        # Add directory and file within it
        writer.add("dir1", MemberType.DIR)
        with writer.open("dir1/file2.txt") as outfile:
            outfile.write(b"content2 tar")

        # Add symlink
        writer.add("link_to_file1.txt", MemberType.SYMLINK, link_target="file1.txt")

        # Add hardlink (requires file1.txt to be "known" to the tar writer by now)
        # The TarArchiveWriter's TarFileStream closes and adds the file immediately,
        # so file1.txt should be in the archive at this point.
        # writer.add("hardlink_to_file1.txt", MemberType.HARDLINK, link_target="file1.txt")


    # Verify basic tar structure
    assert os.path.exists(archive_path)
    # Tarfile read mode will depend on the compression
    read_mode = "r:"
    if extension.endswith(".gz"): read_mode += "gz"
    elif extension.endswith(".bz2"): read_mode += "bz2"
    elif extension.endswith(".xz"): read_mode += "xz"
    elif extension.endswith(".zst"): read_mode += "zst"
    elif extension.endswith(".lz4"): read_mode += "lz4"
    elif extension.endswith(".tar"): read_mode = "r"


    with tarfile.open(archive_path, read_mode) as tf:
        tar_names = tf.getnames()
        assert "file1.txt" in tar_names
        assert "emptydir" in tar_names # Tar names usually don't have trailing / for dirs
        assert "dir1/file2.txt" in tar_names
        # Symlink and hardlink verification
        # symlink_member = tf.getmember("link_to_file1.txt")
        # assert symlink_member.issym()
        # assert symlink_member.linkname == "file1.txt"
        # if "hardlink_to_file1.txt" in tar_names: # if hardlink test is enabled
        #     hardlink_member = tf.getmember("hardlink_to_file1.txt")
        #     assert hardlink_member.islnk() # islnk() is for hard links
        #     assert hardlink_member.linkname == "file1.txt"


    # Verify with our reader
    verify_archive_contents(archive_path, [
         (name, type, lt, content) for name, type, lt, content, _ in expected_members_data
    ], "tar")

# TODO:
# - Test mtime and mode setting more explicitly if ArchiveMember fields are used by writers.
# - Test archives with no members.
# - Test archives with unicode filenames.
# - Test error handling (e.g., writing to a closed archive, adding unsupported member type).
# - Test symlinks pointing to directories.
# - Test symlinks with absolute paths (behavior might vary).
# - Test hardlinks (ensure target is added first, verify link).
# - Test writers directly for more granular control if needed (e.g. ZipArchiveWriter, TarArchiveWriter).
# - Consider if `verify_archive_contents` needs to be more robust regarding path normalization
#   (e.g. slashes at the end of directory names) between zip/tar and our reader's output.
#   For now, tests for tar expect no trailing slashes for dirs in `expected_members_data`
#   and zip tests expect them. This should align with how `ZipArchiveWriter` and `TarArchiveWriter`
#   are implemented and how `open_archive_reader` presents them.
