import logging
import os
import pytest

from archivey.archive_stream import ArchiveStream
from archivey.types import MemberType
from sample_archives import SAMPLE_ARCHIVES, ArchiveInfo, GenerationMethod


def normalize_newlines(s: str | None) -> str | None:
    return s.replace("\r\n", "\n") if s else None


def full_path_to_archive(archive_filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), "../test_archives", archive_filename)


@pytest.mark.parametrize("sample_archive", SAMPLE_ARCHIVES, ids=lambda x: x.filename)
def test_read_archive(sample_archive: ArchiveInfo):
    files_by_name = {file.name: file for file in sample_archive.files}
    allow_timestamp_rounding_error = (
        sample_archive.generation_method == GenerationMethod.ZIPFILE
    )

    with ArchiveStream(full_path_to_archive(sample_archive.filename)) as archive:
        assert archive.get_format() == sample_archive.format
        format_info = archive.get_archive_info()
        assert normalize_newlines(format_info.comment) == normalize_newlines(
            sample_archive.archive_comment
        )
        member_names: list[str] = []

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

            member_names.append(member.filename)

            if sample_file.compression_method is not None:
                assert member.compression_method == sample_file.compression_method

            assert member.comment == sample_file.comment
            assert member.mtime is not None

            if allow_timestamp_rounding_error:
                assert (
                    abs(member.mtime.timestamp() - sample_file.mtime.timestamp()) <= 1
                ), (
                    f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
                )
            else:
                assert member.mtime == sample_file.mtime, (
                    f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
                )

            assert member.encrypted == (sample_file.password is not None)
            password = (
                (sample_file.password or "").encode("utf-8")
                if member.encrypted
                else None
            )

            if sample_file.type == MemberType.FILE:
                with archive.open(member, pwd=password) as f:
                    contents = f.read()
                    assert contents == sample_file.contents

        assert set(member_names) == set(files_by_name.keys())
