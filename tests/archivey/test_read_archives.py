from dataclasses import dataclass
import logging
import os
import zlib
import pytest
from datetime import datetime

from archivey.archive_stream import ArchiveStream
from archivey.types import MemberType
from sample_archives import (
    MARKER_MTIME_BASED_ON_ARCHIVE_NAME,
    SAMPLE_ARCHIVES,
    ArchiveInfo,
    GenerationMethod,
    filter_archives,
)
from archivey.types import ArchiveFormat


def normalize_newlines(s: str | None) -> str | None:
    return s.replace("\r\n", "\n") if s else None


def full_path_to_archive(archive_filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), "../test_archives", archive_filename)


@dataclass
class ArchiveFormatFeatures:
    dir_entries: bool = True
    file_comments: bool = True


FORMAT_FEATURES = {
    ArchiveFormat.RAR: ArchiveFormatFeatures(dir_entries=False, file_comments=False),
    ArchiveFormat.SEVENZIP: ArchiveFormatFeatures(
        dir_entries=False, file_comments=False
    ),
}

DEFAULT_FORMAT_FEATURES = ArchiveFormatFeatures()


def get_crc32(data: bytes) -> int:
    """
    Compute CRC32 checksum for a file within an archive.
    Returns a hex string.
    """
    crc32_value: int = 0

    # Read the file in chunks
    crc32_value = zlib.crc32(data, crc32_value)
    return crc32_value & 0xFFFFFFFF


def check_read_archive(
    sample_archive: ArchiveInfo,
    features: ArchiveFormatFeatures,
    use_rar_stream: bool = False,
):
    # print("STARTING TEST", sample_archive.filename)
    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    archive_base_dir = os.path.join(os.path.dirname(__file__), "..")

    files_by_name = {file.name: file for file in sample_archive.contents.files}
    allow_timestamp_rounding_error = (
        sample_archive.format_info.generation_method == GenerationMethod.ZIPFILE
    )

    with ArchiveStream(
        sample_archive.get_archive_path(archive_base_dir),
        pwd=sample_archive.contents.header_password,
        use_rar_stream=use_rar_stream,
        use_single_file_stored_metadata=True,
    ) as archive:
        assert archive.get_format() == sample_archive.format_info.format
        format_info = archive.get_archive_info()
        assert normalize_newlines(format_info.comment) == normalize_newlines(
            sample_archive.contents.archive_comment
        )
        actual_filenames: list[str] = []

        for member in archive.info_iter():
            sample_file = files_by_name.get(member.filename, None)

            if member.is_file and sample_file is not None and member.crc32 is not None:
                # print(f"Checking CRC32 for {member.filename}, size={member.size}, crc32={member.crc32}")

                sample_crc32 = get_crc32(sample_file.contents or b"")
                assert member.crc32 == sample_crc32, (
                    f"CRC32 mismatch for {member.filename}: got {member.crc32}, expected {sample_crc32}"
                )

            if sample_file is None:
                if member.type == MemberType.DIR:
                    logging.warning(
                        f"Archive {sample_archive.filename} contains unexpected dir {member.filename}"
                    )
                    continue
                else:
                    pytest.fail(
                        f"Archive {sample_archive.filename} contains unexpected file {member.filename}"
                    )

            actual_filenames.append(member.filename)

            if sample_file.compression_method is not None:
                assert member.compression_method == sample_file.compression_method

            if features.file_comments:
                assert member.comment == sample_file.comment
            else:
                assert member.comment is None

            if member.is_file and member.size is not None:
                assert member.size == len(sample_file.contents or b"")

            # Check permissions
            if sample_file.permissions is not None:
                if sample_archive.format_info.format in [
                    ArchiveFormat.TAR,
                    ArchiveFormat.TAR_GZ,
                    ArchiveFormat.TAR_BZ2,
                    ArchiveFormat.TAR_XZ,
                    ArchiveFormat.ZIP,
                ]:
                    assert member.permissions is not None, (
                        f"Permissions not set for {member.filename} in {sample_archive.filename} "
                        f"(expected {oct(sample_file.permissions)})"
                    )
                    assert member.permissions == sample_file.permissions, (
                        f"Permission mismatch for {member.filename} in {sample_archive.filename}: "
                        f"got {oct(member.permissions) if member.permissions is not None else 'None'}, "
                        f"expected {oct(sample_file.permissions)}"
                    )
                elif member.permissions is not None:
                    # For other formats, if permissions happen to be set by the library
                    # and we have an expected mode, check it.
                    assert member.permissions == sample_file.permissions, (
                        f"Permission mismatch for {member.filename} in {sample_archive.filename} (optional check): "
                        f"got {oct(member.permissions)}, expected {oct(sample_file.permissions)}"
                    )

            assert member.encrypted == (
                sample_file.password is not None
                or (
                    member.is_file
                    and sample_archive.contents.header_password is not None
                )
            ), (
                f"Encrypted mismatch for {member.filename}: got {member.encrypted}, expected {sample_file.password is not None}"
            )

            assert member.mtime is not None
            if sample_file.mtime == MARKER_MTIME_BASED_ON_ARCHIVE_NAME:
                archive_file_mtime = datetime.fromtimestamp(
                    os.path.getmtime(sample_archive.get_archive_path(archive_base_dir))
                )
                assert member.mtime == archive_file_mtime, (
                    f"Timestamp mismatch for {member.filename} (special check): "
                    f"member mtime {member.mtime} vs archive mtime {archive_file_mtime}"
                )
            elif allow_timestamp_rounding_error:
                assert (
                    abs(member.mtime.timestamp() - sample_file.mtime.timestamp()) <= 1
                ), (
                    f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
                )
            else:
                assert member.mtime == sample_file.mtime, (
                    f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
                )

            if sample_file.type == MemberType.FILE:
                with archive.open(
                    member,
                    pwd=sample_file.password
                    if sample_archive.contents.header_password is None
                    else None,
                ) as f:
                    contents = f.read()
                    assert contents == sample_file.contents

        expected_filenames = set(
            file.name
            for file in sample_archive.contents.files
            if features.dir_entries or file.type != MemberType.DIR
        )

        missing_files = expected_filenames - set(actual_filenames)
        extra_files = set(actual_filenames) - expected_filenames

        assert not missing_files, f"Missing files: {missing_files}"
        assert not extra_files, f"Extra files: {extra_files}"


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["zip"]),
    ids=lambda x: x.filename,
)
def test_read_zip_archives(sample_archive: ArchiveInfo):
    features = FORMAT_FEATURES.get(
        sample_archive.format_info.format, DEFAULT_FORMAT_FEATURES
    )
    check_read_archive(sample_archive, features)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst", "tar.lz4"],
    ),
    ids=lambda x: x.filename,
)
def test_read_tar_archives(sample_archive: ArchiveInfo):
    features = FORMAT_FEATURES.get(
        sample_archive.format_info.format, DEFAULT_FORMAT_FEATURES
    )
    check_read_archive(sample_archive, features)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["rar"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_rar_stream", [True, False])
def test_read_rar_archives(sample_archive: ArchiveInfo, use_rar_stream: bool):
    features = FORMAT_FEATURES.get(
        sample_archive.format_info.format, DEFAULT_FORMAT_FEATURES
    )
    check_read_archive(sample_archive, features, use_rar_stream=use_rar_stream)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["7z"]),
    ids=lambda x: x.filename,
)
def test_read_sevenzip_py7zr_archives(sample_archive: ArchiveInfo):
    features = FORMAT_FEATURES.get(
        sample_archive.format_info.format, DEFAULT_FORMAT_FEATURES
    )
    check_read_archive(sample_archive, features)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
    ),
    ids=lambda x: x.filename,
)
def test_read_single_file_compressed_archives(sample_archive: ArchiveInfo):
    features = FORMAT_FEATURES.get(
        sample_archive.format_info.format, DEFAULT_FORMAT_FEATURES
    )
    check_read_archive(sample_archive, features)
