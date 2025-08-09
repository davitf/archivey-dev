import logging
import os
import shutil

import pytest

from archivey.core import open_archive, open_compressed_stream
from archivey.exceptions import PackageNotInstalledError
from archivey.types import ArchiveFormat
from tests.archivey.create_archives import (
    SINGLE_FILE_LIBRARY_OPENERS,
    create_7z_archive_with_py7zr,
    create_rar_archive_with_command_line,
    create_tar_archive_with_tarfile,
    create_zip_archive_with_zipfile,
)
from tests.archivey.sample_archives import (
    ALTERNATIVE_CONFIG,
    BASIC_ARCHIVES,
    SINGLE_FILE_ARCHIVES,
    ArchiveContents,
    File,
    SampleArchive,
    filter_archives,
)
from tests.archivey.test_open_nonseekable import EXPECTED_NON_SEEKABLE_FAILURES
from tests.archivey.testing_utils import skip_if_package_missing


def compress_file(src: str, dst: str, fmt: ArchiveFormat) -> str:
    opener = SINGLE_FILE_LIBRARY_OPENERS.get(fmt)
    if opener is None:
        pytest.skip(f"Required library for {fmt.file_extension()} is not installed")
    with open(src, "rb") as f_in, opener(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    return dst


def create_archive_with_member(
    outer_format: ArchiveFormat, inner_path: str, dst: str
) -> str:
    data = open(inner_path, "rb").read()
    contents = ArchiveContents(
        file_basename="outer", files=[File(os.path.basename(inner_path), 1, data)]
    )

    if outer_format == ArchiveFormat.ZIP:
        create_zip_archive_with_zipfile(dst, contents, ArchiveFormat.ZIP)
    elif outer_format == ArchiveFormat.RAR:
        create_rar_archive_with_command_line(dst, contents, ArchiveFormat.RAR)
    elif outer_format == ArchiveFormat.SEVENZIP:
        create_7z_archive_with_py7zr(dst, contents, ArchiveFormat.SEVENZIP)
    elif outer_format in [
        ArchiveFormat.TAR_GZ,
        ArchiveFormat.TAR_BZ2,
        ArchiveFormat.TAR_XZ,
        ArchiveFormat.TAR_ZSTD,
        ArchiveFormat.TAR_LZ4,
        ArchiveFormat.TAR,
    ]:
        create_tar_archive_with_tarfile(dst, contents, outer_format)
    else:
        raise AssertionError(f"Unsupported outer format {outer_format}")
    return dst


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "outer_format",
    SINGLE_FILE_LIBRARY_OPENERS.keys(),
)
@pytest.mark.parametrize(
    "inner_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    # TAR_MEMBER_PAIRS,
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_archive_from_compressed_stream(
    outer_format: ArchiveFormat,
    inner_archive: SampleArchive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    if (
        alternative_packages
        and outer_format == ArchiveFormat.BZIP2
        and inner_archive.filename.endswith(".bz2")
    ):
        pytest.xfail("prevent segfault")

    logger.info(
        f"alternative_packages: {alternative_packages}, outer_format: {outer_format}, inner_archive.filename: {inner_archive.filename}"
    )

    inner_path = inner_archive.get_archive_path()
    compressed_path = os.path.join(
        tmp_path, os.path.basename(inner_path) + "." + outer_format.file_extension()
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
    "outer_format",
    [
        ArchiveFormat.TAR_GZ,
        ArchiveFormat.TAR_BZ2,
        ArchiveFormat.TAR_XZ,
        ArchiveFormat.TAR_ZSTD,
        ArchiveFormat.TAR_LZ4,
        ArchiveFormat.TAR,
        ArchiveFormat.ZIP,
        ArchiveFormat.RAR,
        ArchiveFormat.SEVENZIP,
    ],
    # ids=TAR_MEMBER_IDS,
)
@pytest.mark.parametrize(
    "inner_archive",
    filter_archives(
        BASIC_ARCHIVES + SINGLE_FILE_ARCHIVES,
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.FOLDER,
    ),
    # TAR_MEMBER_PAIRS,
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["default", "altlibs"]
)
def test_open_archive_from_member(
    outer_format: ArchiveFormat,
    inner_archive: SampleArchive,
    tmp_path,
    alternative_packages: bool,
):
    config = ALTERNATIVE_CONFIG if alternative_packages else None

    skip_if_package_missing(outer_format, config)
    skip_if_package_missing(inner_archive.creation_info.format, config)

    inner_path = inner_archive.get_archive_path()
    outer_path = os.path.join(tmp_path, "outer." + outer_format.file_extension())
    try:
        create_archive_with_member(outer_format, inner_path, outer_path)
    except PackageNotInstalledError as exc:
        pytest.skip(str(exc))

    with open_archive(outer_path, config=config, streaming_only=True) as outer:
        for member, stream in outer.iter_members_with_streams():
            assert member.filename.endswith(os.path.basename(inner_path))
            assert stream is not None

            if (
                not stream.seekable()
                and (inner_archive.creation_info.format, alternative_packages)
                in EXPECTED_NON_SEEKABLE_FAILURES
            ):
                pytest.xfail("Non-seekable stream not supported")

            with open_archive(stream, config=config, streaming_only=True) as archive:
                assert archive.format == inner_archive.creation_info.format
                for _ in archive.iter_members_with_streams():
                    break
            break
