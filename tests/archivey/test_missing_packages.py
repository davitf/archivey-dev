import logging
import os
from unittest.mock import patch

import pytest

from archivey.core import open_archive
from archivey.dependency_checker import get_dependency_versions
from archivey.exceptions import (
    PackageNotInstalledError,
)
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    filter_archives,
)

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

BASIC_ISO_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["iso"]
)[0]

BASIC_ZSTD_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["zst"]
)[0]

BASIC_LZ4_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["lz4"]
)[0]

BASIC_ZSTD_ARCHIVE = filter_archives(
    SAMPLE_ARCHIVES, prefixes=["single_file"], extensions=["zst"]
)[0]


@pytest.mark.parametrize(
    ["library_name", "archive_path"],
    [
        ("pycdlib", BASIC_ISO_ARCHIVE.get_archive_path()),
        ("rarfile", BASIC_RAR_ARCHIVE.get_archive_path()),
        ("py7zr", BASIC_7Z_ARCHIVE.get_archive_path()),
        ("pyzstd", BASIC_ZSTD_ARCHIVE.get_archive_path()),
        ("lz4", BASIC_LZ4_ARCHIVE.get_archive_path()),
    ],
    ids=lambda x: os.path.basename(x),
)
def test_missing_package_raises_exception(library_name: str, archive_path: str):
    dependencies = get_dependency_versions()
    if getattr(dependencies, f"{library_name}_version") is not None:
        pytest.skip(
            f"{library_name} is installed with version {getattr(dependencies, f'{library_name}_version')}"
        )

    with pytest.raises(PackageNotInstalledError) as excinfo:
        open_archive(archive_path)

    assert f"{library_name} package is not installed" in str(excinfo.value)


@pytest.mark.skipif(
    get_dependency_versions().rarfile_version is None, reason="rarfile is not installed"
)
def test_rarfile_missing_cryptography_raises_exception():
    """Test that LibraryNotInstalledError is raised for header-encrypted .rar when cryptography is not installed."""
    with patch("archivey.rar_reader.rarfile._have_crypto", 0):
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
    with patch("archivey.rar_reader.rarfile._have_crypto", 0):
        with open_archive(
            NORMAL_ENCRYPTED_RAR_ARCHIVE.get_archive_path(),
            pwd=NORMAL_ENCRYPTED_RAR_ARCHIVE.contents.header_password,
        ) as archive:
            assert {m.filename for m in archive.get_members()} == {
                "secret.txt",
                "also_secret.txt",
            }
