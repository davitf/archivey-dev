import os
import shutil

import pytest

from archivey.core import open_archive, open_compressed_stream
from archivey.types import ArchiveFormat

from tests.archivey.sample_archives import (
    BASIC_ARCHIVES,
    ALTERNATIVE_CONFIG,
    filter_archives,
    ArchiveContents,
    File,
)
from tests.archivey.create_archives import (
    SINGLE_FILE_LIBRARY_OPENERS,
    create_zip_archive_with_zipfile,
    create_rar_archive_with_command_line,
    create_7z_archive_with_py7zr,
)
from tests.archivey.testing_utils import skip_if_package_missing
from archivey.exceptions import PackageNotInstalledError

# Select a representative archive for each inner format
INNER_ARCHIVES = {
    ArchiveFormat.ZIP: filter_archives(BASIC_ARCHIVES, extensions=["zip"])[0],
    ArchiveFormat.RAR: filter_archives(BASIC_ARCHIVES, extensions=["rar"])[0],
    ArchiveFormat.SEVENZIP: filter_archives(BASIC_ARCHIVES, extensions=["7z"])[0],
    ArchiveFormat.TAR: filter_archives(BASIC_ARCHIVES, extensions=["tar"])[0],
}

# Map compression format to output extension
COMPRESSION_EXT = {
    ArchiveFormat.GZIP: ".gz",
    ArchiveFormat.BZIP2: ".bz2",
    ArchiveFormat.XZ: ".xz",
    ArchiveFormat.ZSTD: ".zst",
    ArchiveFormat.LZ4: ".lz4",
}

# Extension for archive containers
ARCHIVE_EXT = {
    ArchiveFormat.ZIP: ".zip",
    ArchiveFormat.RAR: ".rar",
    ArchiveFormat.SEVENZIP: ".7z",
}

# Pair outer compression formats with inner archives. This ensures all formats
# appear at least once as outer and inner archives.
TEST_PAIRS = [
    (ArchiveFormat.GZIP, INNER_ARCHIVES[ArchiveFormat.ZIP]),
    (ArchiveFormat.BZIP2, INNER_ARCHIVES[ArchiveFormat.RAR]),
    (ArchiveFormat.XZ, INNER_ARCHIVES[ArchiveFormat.SEVENZIP]),
    (ArchiveFormat.ZSTD, INNER_ARCHIVES[ArchiveFormat.TAR]),
    (ArchiveFormat.LZ4, INNER_ARCHIVES[ArchiveFormat.ZIP]),
]

PAIR_IDS = [f"{outer.name}_of_{inner.filename}" for outer, inner in TEST_PAIRS]

# Pairs of archive container formats with compressed tar members
TAR_MEMBER_PAIRS = [
    (ArchiveFormat.ZIP, filter_archives(BASIC_ARCHIVES, extensions=["tar.gz"])[0]),
    (ArchiveFormat.RAR, filter_archives(BASIC_ARCHIVES, extensions=["tar.bz2"])[0]),
    (ArchiveFormat.SEVENZIP, filter_archives(BASIC_ARCHIVES, extensions=["tar.xz"])[0]),
]

TAR_MEMBER_IDS = [
    "zip_tar_gz",
    "rar_tar_bz2",
    "7z_tar_xz",
]


def compress_file(src: str, dst: str, fmt: ArchiveFormat) -> str:
    opener = SINGLE_FILE_LIBRARY_OPENERS.get(fmt)
    if opener is None:
        pytest.skip(f"Required library for {fmt.name} is not installed")
    with open(src, "rb") as f_in, opener(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return dst


def create_archive_with_member(
    outer_format: ArchiveFormat, inner_path: str, dst: str
) -> str:
    data = open(inner_path, "rb").read()
    contents = ArchiveContents(file_basename="outer", files=[File(os.path.basename(inner_path), 1, data)])
    if outer_format == ArchiveFormat.ZIP:
        create_zip_archive_with_zipfile(dst, contents, ArchiveFormat.ZIP)
    elif outer_format == ArchiveFormat.RAR:
        create_rar_archive_with_command_line(dst, contents, ArchiveFormat.RAR)
    elif outer_format == ArchiveFormat.SEVENZIP:
        create_7z_archive_with_py7zr(dst, contents, ArchiveFormat.SEVENZIP)
    else:
        raise AssertionError(f"Unsupported outer format {outer_format}")
    return dst


@pytest.mark.parametrize(
    "outer_format, inner_archive",
    TEST_PAIRS,
    ids=PAIR_IDS,
)
@pytest.mark.parametrize("alternative_packages", [False, True], ids=["default", "altlibs"])
def test_open_archive_from_compressed_stream(
    outer_format: ArchiveFormat,
    inner_archive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    if outer_format == ArchiveFormat.XZ and alternative_packages:
        pytest.xfail("python_xz does not support readinto")

    inner_path = inner_archive.get_archive_path()
    compressed_path = os.path.join(
        tmp_path, os.path.basename(inner_path) + COMPRESSION_EXT[outer_format]
    )
    compress_file(inner_path, compressed_path, outer_format)

    with open_compressed_stream(compressed_path, config=config) as stream:
        with open_archive(stream, config=config, streaming_only=True) as archive:
            assert archive.format == inner_archive.creation_info.format
            has_member = False
            for _, member_stream in archive.iter_members_with_streams():
                has_member = True
                if member_stream is not None:
                    member_stream.read()
            assert has_member


@pytest.mark.parametrize(
    "outer_format, inner_archive",
    TAR_MEMBER_PAIRS,
    ids=TAR_MEMBER_IDS,
)
@pytest.mark.parametrize("alternative_packages", [False, True], ids=["default", "altlibs"])
def test_open_archive_from_member(
    outer_format: ArchiveFormat,
    inner_archive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    if inner_archive.creation_info.format == ArchiveFormat.TAR_XZ and alternative_packages:
        pytest.xfail("python_xz does not support readinto")
    if inner_archive.creation_info.format == ArchiveFormat.TAR_GZ and alternative_packages:
        pytest.xfail("rapidgzip does not support non-seekable streams")

    inner_path = inner_archive.get_archive_path()
    outer_path = os.path.join(tmp_path, "outer" + ARCHIVE_EXT[outer_format])
    try:
        create_archive_with_member(outer_format, inner_path, outer_path)
    except PackageNotInstalledError as exc:
        pytest.skip(str(exc))

    with open_archive(outer_path, config=config, streaming_only=True) as outer:
        for member, stream in outer.iter_members_with_streams():
            assert member.filename.endswith(os.path.basename(inner_path))
            with open_archive(stream, config=config, streaming_only=True) as archive:
                assert archive.format == inner_archive.creation_info.format
                for _ in archive.iter_members_with_streams():
                    break
            break
