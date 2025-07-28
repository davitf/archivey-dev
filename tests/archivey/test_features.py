import logging
import os
import struct
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from archivey import open_archive
from archivey.config import ExtractionFilter
from archivey.core import open_archive
from archivey.exceptions import (
    ArchiveEncryptedError,
    ArchiveError,
    ArchiveFilterError,
    ArchiveMemberCannotBeOpenedError,
    ArchiveMemberNotFoundError,
    PackageNotInstalledError,
)
from archivey.filters import (
    create_filter,
    fully_trusted,
    tar_filter,
)
from archivey.internal.dependency_checker import get_dependency_versions
from archivey.types import ArchiveFormat, ArchiveMember, MemberType
from tests.archivey.test_samples import (
    ALTERNATIVE_CONFIG,
    SAMPLE_ARCHIVES,
    SANITIZE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.test_reading import check_iter_members
from tests.archivey.test_utils import skip_if_package_missing

# Select encrypted sample archives that use a single password and no header password
ENCRYPTED_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES,
    prefixes=[
        "encryption",
        "encryption_with_plain",
        "encryption_solid",
        "encryption_with_symlinks",
    ],
    extensions=["zip", "rar", "7z"],
    custom_filter=lambda a: not a.contents.has_multiple_passwords()
    and a.contents.header_password is None,
)


def _archive_password(sample: SampleArchive) -> str:
    for f in sample.contents.files:
        if f.password is not None:
            return f.password
    raise ValueError("sample archive has no password")


def _first_encrypted_file(sample: SampleArchive):
    for f in sample.contents.files:
        if f.password is not None and f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no encrypted files")


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_open_archive(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path, pwd=pwd) as archive:
        encrypted = _first_encrypted_file(sample_archive)
        with archive.open(encrypted.name) as fh:
            assert fh.read() == encrypted.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_iter_members(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip(
                "py7zr does not support password parameter for iter_members_with_streams"
            )
        contents = {}
        for m, stream in archive.iter_members_with_streams(pwd=pwd):
            if m.is_file:
                assert stream is not None
                contents[m.filename] = stream.read()
        for f in sample_archive.contents.files:
            if f.type == MemberType.FILE:
                assert contents[f.name] == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_open(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path) as archive:
        for f in sample_archive.contents.files:
            if f.type == MemberType.FILE:
                with archive.open(f.name, pwd=pwd) as fh:
                    assert fh.read() == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_open(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    encrypted = _first_encrypted_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            with archive.open(encrypted.name, pwd=wrong) as f:
                f.read()


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_iter_members_read(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.skip(
            "py7zr does not support password parameter for iter_members_with_streams"
        )

    wrong = "wrong_password"
    with open_archive(sample_archive_path) as archive:
        for m, stream in archive.iter_members_with_streams(pwd=wrong):
            assert stream is not None
            if m.is_file:
                if m.encrypted:
                    with pytest.raises((ArchiveEncryptedError, ArchiveError)):
                        stream.read()
                else:
                    stream.read()


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_iter_members_no_read(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip(
                "py7zr does not support password parameter for iter_members_with_streams"
            )
        for _m, _stream in archive.iter_members_with_streams(pwd=wrong):
            pass


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extract_with_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    dest = tmp_path / "out"
    dest.mkdir()
    encrypted = _first_encrypted_file(sample_archive)
    # config = get_default_config()
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip("py7zr extract password support incomplete")
        # archive.config.overwrite_mode = OverwriteMode.OVERWRITE
        path = archive.extract(encrypted.name, dest, pwd=pwd)
    extracted_path = Path(path or dest / encrypted.name)
    with open(extracted_path, "rb") as f:
        assert f.read() == encrypted.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extractall_with_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
    #     pytest.skip("py7zr extractall password support incomplete")

    pwd = _archive_password(sample_archive)
    dest = tmp_path / "all"
    dest.mkdir()
    with open_archive(sample_archive_path) as archive:
        archive.extractall(dest, pwd=pwd)

    for f in sample_archive.contents.files:
        if f.type == MemberType.FILE:
            path = dest / f.name
            assert path.exists()
            with open(path, "rb") as fh:
                assert fh.read() == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extract_wrong_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    dest = tmp_path / "out"
    dest.mkdir()
    encrypted = _first_encrypted_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip("py7zr extract password support incomplete")
        # archive.config.overwrite_mode = OverwriteMode.OVERWRITE
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            archive.extract(encrypted.name, dest, pwd=wrong)


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extractall_wrong_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
    #     pytest.skip("py7zr extractall password support incomplete")

    wrong = "wrong_password"
    dest = tmp_path / "all"
    dest.mkdir()
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            archive.extractall(dest, pwd=wrong)


# @pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_no_password(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path) as archive:
        for member, stream in archive.iter_members_with_streams():
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_password_in_open_archive(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path, pwd="pwd") as archive:
        for member, stream in archive.iter_members_with_streams():
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_password_in_iterator(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path) as archive:
        for member, stream in archive.iter_members_with_streams(pwd="pwd"):
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
        extensions=["rar", "7z"],
    ),
    ids=lambda a: a.filename,
)
def test_open_encrypted_symlink(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    sample_files = {f.name: f for f in sample_archive.contents.files}

    files_to_test = [
        ("encrypted_link_to_secret.txt", "pwd"),
        ("encrypted_link_to_not_secret.txt", "longpwd"),
        ("plain_link_to_secret.txt", "pwd"),
    ]
    with open_archive(sample_archive_path) as archive:
        for filename, pwd in files_to_test:
            data = archive.open(filename, pwd=pwd).read()
            assert data == sample_files[filename].contents

            # After reading the file, the link target should have been set
            member = archive.get_member(filename)
            assert member.link_target == sample_files[filename].link_target


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
        extensions=["rar", "7z"],
    ),
    ids=lambda a: a.filename,
)
def test_open_encrypted_symlink_wrong_password(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    symlink_name = "encrypted_link_to_secret.txt"

    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            with archive.open(symlink_name, pwd="wrong") as fh:
                fh.read()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
        extensions=["rar", "7z"],
    ),
    ids=lambda a: a.filename,
)
def test_open_encrypted_symlink_target_wrong_password(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    symlink_name = "encrypted_link_to_very_secret.txt"

    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            with archive.open(symlink_name, pwd="pwd") as fh:
                fh.read()


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_fully_trusted_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the fully_trusted filter allows everything."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_streams(filter=fully_trusted))

        # Should get all members without any filtering
        assert len(members) > 0

        # Check that problematic files are still present
        filenames = {m.filename for m, _ in members if m.type != MemberType.DIR}
        expected_filenames = {
            f.name for f in sample_archive.contents.files if f.type != MemberType.DIR
        }
        features = sample_archive.creation_info.features
        if features.replace_backslash_with_slash:
            expected_filenames = {
                name.replace("\\", "/") for name in expected_filenames
            }
        assert filenames == expected_filenames


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_tar_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the tar_filter raises errors on unsafe content."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            ArchiveFilterError,
            match="(Absolute path not allowed|Path outside archive root|Symlink target outside archive root)",
        ):
            list(archive.iter_members_with_streams(filter=tar_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_data_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test the data_filter raises errors on unsafe content."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            ArchiveFilterError,
            match="(Absolute path not allowed|Path outside archive root|Symlink target outside archive root)",
        ):
            list(archive.iter_members_with_streams(filter=ExtractionFilter.DATA))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_with_raise_on_error_false(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that logs warnings instead of raising errors."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=False,
    )

    with open_archive(sample_archive_path) as archive:
        # Should not raise an error, but should filter out problematic members
        members = list(archive.iter_members_with_streams(filter=custom_filter))

        # Should get some members (the safe ones)
        assert len(members) > 0

        # Check that problematic files are filtered out
        filenames = {m.filename for m, _ in members}
        assert "/absfile.txt" not in filenames
        assert "../outside.txt" not in filenames


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_name_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize names."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=False,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        # Should still raise error due to link target sanitization
        with pytest.raises(
            ArchiveFilterError, match="Symlink target outside archive root"
        ):
            list(archive.iter_members_with_streams(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_link_target_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize link targets."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=False,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        name_issues = any(
            f.name.startswith("/") or f.name.startswith("../") or "/../" in f.name
            for f in sample_archive.contents.files
        )
        if name_issues:
            with pytest.raises(ArchiveFilterError):
                list(archive.iter_members_with_streams(filter=custom_filter))
        else:
            list(archive.iter_members_with_streams(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_without_permission_sanitization(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test filter that doesn't sanitize permissions."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=False,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        # Should still raise error due to name/link sanitization
        with pytest.raises(ArchiveFilterError):
            list(archive.iter_members_with_streams(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_data_filter_with_permission_changes(
    sample_archive: SampleArchive, sample_archive_path: str
):
    """Test data filter that changes permissions for files."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    data_filter_custom = create_filter(
        for_data=True,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=False,  # Don't raise to see permission changes
    )

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_streams(filter=data_filter_custom))

        # Check that executable files have permissions changed
        for member, _ in members:
            if member.is_file and "exec.sh" in member.filename:
                # The filter removes executable bits but keeps owner permissions as 0o644
                # Original mode is 493 (0o755), should become 420 (0o644)
                expected_mode = 0o644  # 420
                actual_mode = member.mode if member.mode is not None else "None"
                assert member.mode == expected_mode, (
                    f"Expected {oct(expected_mode)}, got {oct(actual_mode) if actual_mode != 'None' else 'None'}"
                )


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_combinations(sample_archive: SampleArchive, sample_archive_path: str):
    # Test minimal filtering
    skip_if_package_missing(sample_archive.creation_info.format, None)

    minimal_filter = create_filter(
        for_data=False,
        sanitize_names=False,
        sanitize_link_targets=False,
        sanitize_permissions=False,
        raise_on_error=False,
    )

    with open_archive(sample_archive_path) as archive:
        members = list(archive.iter_members_with_streams(filter=minimal_filter))
        # Should get all members since no filtering is done
        assert len(members) > 0

        # Check that problematic files are still present
        filenames = [m.filename for m, _ in members]
        expected_names = [f.name for f in sample_archive.contents.files]
        features = sample_archive.creation_info.features
        if features.replace_backslash_with_slash:
            expected_names = [n.replace("\\", "/") for n in expected_names]

        if "/absfile.txt" in expected_names:
            assert any("/absfile.txt" in f for f in filenames)
        if "../outside.txt" in expected_names:
            assert any("../outside.txt" in f for f in filenames)


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_error_messages(sample_archive: SampleArchive, sample_archive_path: str):
    """Test that filter errors have meaningful messages."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(ArchiveFilterError) as exc_info:
            list(archive.iter_members_with_streams(filter=tar_filter))

        error_msg = str(exc_info.value)
        assert (
            "Absolute path not allowed" in error_msg
            or "Path outside archive root" in error_msg
            or "Symlink target outside archive root" in error_msg
        )


ERROR_CASES = [
    ("../outside.txt", "Path outside archive root"),
    ("link_outside", "Symlink target outside archive root"),
    ("hardlink_outside", "Hardlink target outside archive root"),
]


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize(
    ("member_name", "pattern"),
    ERROR_CASES,
    ids=[c[0] for c in ERROR_CASES],
)
def test_tar_filter_individual_errors(
    sample_archive: SampleArchive,
    sample_archive_path: str,
    member_name: str,
    pattern: str,
):
    """Ensure tar_filter raises the correct error for each problematic member."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    if member_name not in {f.name for f in sample_archive.contents.files}:
        pytest.skip(f"{member_name} not present in {sample_archive.filename}")

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(ArchiveFilterError, match=pattern):
            list(
                archive.iter_members_with_streams(
                    members=[member_name], filter=tar_filter
                )
            )


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES,
    ids=lambda x: x.filename,
)
def test_filter_with_dest_path(sample_archive: SampleArchive, sample_archive_path: str):
    """Test filter behavior with destination path specified."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    custom_filter = create_filter(
        for_data=False,
        sanitize_names=True,
        sanitize_link_targets=True,
        sanitize_permissions=True,
        raise_on_error=True,
    )

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(ArchiveFilterError):
            list(archive.iter_members_with_streams(filter=custom_filter))


@pytest.mark.parametrize(
    "sample_archive",
    SANITIZE_ARCHIVES[:1],
    ids=lambda x: x.filename,
)
def test_broken_filter(sample_archive: SampleArchive, sample_archive_path: str):
    """Test that a broken filter raises an error."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    first_member: ArchiveMember | None = None

    def broken_filter(member: ArchiveMember) -> ArchiveMember | None:
        # A filter that caches and always returns the first member. The code should
        # notice that the returned member is different from the input member.
        nonlocal first_member
        if first_member is None:
            first_member = member

        return first_member.replace()  # Create a copy

    with open_archive(sample_archive_path) as archive:
        with pytest.raises(
            ValueError, match="Filter returned a member with a different internal ID"
        ):
            list(archive.iter_members_with_streams(filter=broken_filter))


logger = logging.getLogger(__name__)

# Tests for LibraryNotInstalledError
BASIC_RAR_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["rar"]
)[0]

HEADER_ENCRYPTED_RAR_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["encrypted_header"], extensions=["rar"]
)[0]

NORMAL_ENCRYPTED_RAR_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["encryption"], extensions=["rar"]
)[0]

BASIC_7Z_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["7z"]
)[0]

# BASIC_ISO_ARCHIVE = filter_archives(
#     SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["iso"]
# )[0]

BASIC_ZSTD_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["zst"]
)[0]

BASIC_LZ4_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["lz4"]
)[0]

BASIC_GZIP_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["gz"]
)[0]

BASIC_BZIP2_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["bz2"]
)[0]

BASIC_XZ_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["xz"]
)[0]


@pytest.mark.parametrize(
    ["library_name", "sample_archive", "alternative_packages"],
    [
        # ("pycdlib", BASIC_ISO_ARCHIVE.get_archive_path(), None),
        ("rarfile", BASIC_RAR_ARCHIVE, False),
        ("py7zr", BASIC_7Z_ARCHIVE, False),
        ("rapidgzip", BASIC_GZIP_ARCHIVE, True),
        ("indexed_bzip2", BASIC_BZIP2_ARCHIVE, True),
        ("python-xz", BASIC_XZ_ARCHIVE, True),
        ("pyzstd", BASIC_ZSTD_ARCHIVE, False),
        ("zstandard", BASIC_ZSTD_ARCHIVE, True),
        ("lz4", BASIC_LZ4_ARCHIVE, False),
    ],
    ids=lambda x: os.path.basename(x) if isinstance(x, str) else x,
)
def test_missing_package_raises_exception(
    library_name: str, sample_archive: SampleArchive, alternative_packages: bool
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    archive_path = sample_archive.get_archive_path()
    dependencies = get_dependency_versions()
    library_version = getattr(dependencies, f"{library_name.replace('-', '_')}_version")

    # Check if we're in a no-libs test environment
    if os.environ.get("ARCHIVEY_TEST_NO_LIBS"):
        if library_version is not None:
            pytest.fail(
                f"{library_name} should not be installed in nolibs environment, but found version {library_version}"
            )
    else:
        # Original behavior: skip if library is installed
        if library_version is not None:
            pytest.skip(f"{library_name} is installed with version {library_version}")

    if library_version is not None:
        pytest.skip(
            f"{library_name} is installed with version {getattr(dependencies, f'{library_name}_version')}"
        )

    with pytest.raises(PackageNotInstalledError) as excinfo:
        open_archive(archive_path, config=config)

    assert f"{library_name} package is not installed" in str(excinfo.value)


@pytest.mark.skipif(
    get_dependency_versions().rarfile_version is None, reason="rarfile is not installed"
)
def test_rarfile_missing_cryptography_raises_exception():
    """Test that LibraryNotInstalledError is raised for header-encrypted .rar when cryptography is not installed."""
    with patch("archivey.formats.rar_reader.rarfile._have_crypto", 0):
        with open_archive(
            NORMAL_ENCRYPTED_RAR_ARCHIVE.get_archive_path(),
            pwd=NORMAL_ENCRYPTED_RAR_ARCHIVE.contents.header_password,
        ) as archive:
            assert {m.filename for m in archive.get_members()} == {
                "secret.txt",
                "also_secret.txt",
            }


@pytest.mark.skipif(
    get_dependency_versions().rarfile_version is None, reason="rarfile is not installed"
)
def test_rarfile_missing_cryptography_does_not_raise_exception_for_other_files():
    """Test that LibraryNotInstalledError is NOT raised for non-header-encrypted .rar when cryptography is not installed."""
    with patch("archivey.formats.rar_reader.rarfile._have_crypto", 0):
        with open_archive(
            NORMAL_ENCRYPTED_RAR_ARCHIVE.get_archive_path(),
            pwd=NORMAL_ENCRYPTED_RAR_ARCHIVE.contents.header_password,
        ) as archive:
            assert {m.filename for m in archive.get_members()} == {
                "secret.txt",
                "also_secret.txt",
            }


SANITIZE_ALL_ARCHIVES = filter_archives(SAMPLE_ARCHIVES, prefixes=["sanitize"])


@pytest.mark.parametrize(
    "sample_archive", SANITIZE_ALL_ARCHIVES, ids=lambda a: a.filename
)
def test_open_symlink_outside(sample_archive: SampleArchive, sample_archive_path: str):
    """Opening a symlink that points outside the archive should fail."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # For folder archives ensure the link target exists so failures are due to
    # the path check, not a missing file.
    if sample_archive.creation_info.format == ArchiveFormat.FOLDER:
        folder_root = Path(sample_archive_path)
        (folder_root.parent / "escape.txt").write_text("outside")

    with open_archive(sample_archive_path) as archive:
        members = {m.filename: m for m in archive.get_members()}
        member = members.get("link_outside")
        assert member is not None, "test archive missing link_outside"
        with pytest.raises(
            (ArchiveMemberCannotBeOpenedError, ArchiveMemberNotFoundError)
        ):
            archive.open(member)


def test_zip_extra_field_before_timestamp(tmp_path) -> None:
    path = tmp_path / "extra.zip"
    modtime = int(datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp())
    zi = zipfile.ZipInfo("file.txt", date_time=(2020, 1, 2, 3, 4, 5))
    zi.extra = (
        struct.pack("<HH4s", 0x1234, 4, b"abcd")
        + struct.pack("<HHB", 0x5455, 5, 1)
        + struct.pack("<I", modtime)
    )
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(zi, b"data")
    with open_archive(str(path)) as archive:
        info = archive.get_members()[0]
        assert info.mtime_with_tz == datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlinks", "symlinks_solid"]),
    ids=lambda x: x.filename,
)
def test_read_symlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["hardlinks_nonsolid", "hardlinks_solid"]
    ),
    ids=lambda x: x.filename,
)
def test_read_hardlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)
