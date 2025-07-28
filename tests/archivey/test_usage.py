import logging
import os
from unittest.mock import patch

import pytest

from archivey.core import open_archive
from archivey.exceptions import (
    PackageNotInstalledError,
    ArchiveMemberCannotBeOpenedError,
)
from archivey.internal.dependency_checker import get_dependency_versions
from archivey.types import ArchiveMember, MemberType
from tests.archivey.test_archive_creation import (
    ALTERNATIVE_CONFIG,
    SAMPLE_ARCHIVES,
    SampleArchive,
    filter_archives,
    BASIC_ARCHIVES,
    SYMLINK_ARCHIVES,
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
        list(archive.iter_members_with_streams())

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
