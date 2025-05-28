from dataclasses import dataclass
import logging
import os
import pytest
from datetime import datetime

from archivey.archive_stream import ArchiveStream
from archivey.types import MemberType
from sample_archives import SAMPLE_ARCHIVES, ArchiveInfo, GenerationMethod
from archivey.types import ArchiveFormat

# Special mtime value to indicate that the member's mtime should be compared
# against the archive file's mtime.
SPECIAL_MTIME_FOR_ARCHIVE_MTIME_CHECK = datetime(1970, 1, 1, 0, 0, 0)


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


def check_read_archive(sample_archive: ArchiveInfo, features: ArchiveFormatFeatures):
    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    archive_base_dir = os.path.join(os.path.dirname(__file__), "..")

    files_by_name = {file.name: file for file in sample_archive.files}
    allow_timestamp_rounding_error = (
        sample_archive.generation_method == GenerationMethod.ZIPFILE
    )

    with ArchiveStream(
        sample_archive.get_archive_path(archive_base_dir),
        pwd=sample_archive.header_password,
    ) as archive:
        assert archive.get_format() == sample_archive.format
        format_info = archive.get_archive_info()
        assert normalize_newlines(format_info.comment) == normalize_newlines(
            sample_archive.archive_comment
        )
        actual_filenames: list[str] = []

        for member in archive.info_iter():
            sample_file = files_by_name.get(member.filename, None)
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

            assert member.mtime is not None
            if member.is_file:
                assert member.size == len(sample_file.contents or b"")

            assert member.encrypted == (
                sample_file.password is not None
                or (member.is_file and sample_archive.header_password is not None)
            ), (
                f"Encrypted mismatch for {member.filename}: got {member.encrypted}, expected {sample_file.password is not None}"
            )

            if sample_file.mtime == SPECIAL_MTIME_FOR_ARCHIVE_MTIME_CHECK:
                archive_file_mtime_ts = os.path.getmtime(sample_archive.get_archive_path(archive_base_dir))
                # Convert member.mtime to timestamp for comparison, allowing for a small tolerance
                # This is primarily for GZip, BZip2, XZ where member mtime should be file's original mtime,
                # and the archive file's mtime is also set to this.
                assert abs(member.mtime.timestamp() - archive_file_mtime_ts) <= 2, (
                    f"Timestamp mismatch for {member.filename} (special check): "
                    f"member mtime {member.mtime.timestamp()} vs archive mtime {archive_file_mtime_ts}"
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
                    if sample_archive.header_password is None
                    else None,
                ) as f:
                    contents = f.read()
                    assert contents == sample_file.contents

        expected_filenames = set(
            file.name
            for file in sample_archive.files
            if features.dir_entries or file.type != MemberType.DIR
        )

        missing_files = expected_filenames - set(actual_filenames)
        extra_files = set(actual_filenames) - expected_filenames

        assert not missing_files, f"Missing files: {missing_files}"
        assert not extra_files, f"Extra files: {extra_files}"


@pytest.mark.parametrize("sample_archive", SAMPLE_ARCHIVES, ids=lambda x: x.filename)
def test_read_archive(sample_archive: ArchiveInfo):
    features = FORMAT_FEATURES.get(sample_archive.format, DEFAULT_FORMAT_FEATURES)
    check_read_archive(sample_archive, features)
