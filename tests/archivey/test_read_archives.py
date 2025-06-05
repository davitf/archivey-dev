import glob
import logging
import os
import pathlib
import zlib
from datetime import datetime

import pytest
from tests.archivey.sample_archives import (
    MARKER_MTIME_BASED_ON_ARCHIVE_NAME,
    SAMPLE_ARCHIVES,
    ArchiveInfo,
    FileInfo,
    filter_archives,
)

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.dependency_checker import get_dependency_versions
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
)
from archivey.types import ArchiveFormat, ArchiveMember, CreateSystem, MemberType


def normalize_newlines(s: str | None) -> str | None:
    return s.replace("\r\n", "\n") if s else None


def get_crc32(data: bytes) -> int:
    """
    Compute CRC32 checksum for a file within an archive.
    Returns a hex string.
    """
    crc32_value: int = 0

    # Read the file in chunks
    crc32_value = zlib.crc32(data, crc32_value)
    return crc32_value & 0xFFFFFFFF


def check_member_metadata(
    member: ArchiveMember,
    sample_file: FileInfo | None,
    sample_archive: ArchiveInfo,
    archive_path: str | None = None,
):
    if sample_file is None:
        return

    features = sample_archive.creation_info.features

    if member.is_file and member.crc32 is not None:
        sample_crc32 = get_crc32(sample_file.contents or b"")
        assert member.crc32 == sample_crc32, (
            f"CRC32 mismatch for {member.filename}: got {member.crc32}, expected {sample_crc32}"
        )

    if sample_file.compression_method is not None:
        assert member.compression_method == sample_file.compression_method

    if features.file_comments:
        assert member.comment == sample_file.comment
    else:
        assert member.comment is None

    if member.is_file:
        if features.file_size:
            assert member.file_size == len(sample_file.contents or b"")
        else:
            assert member.file_size is None

    # Check permissions
    if sample_file.permissions is not None:
        assert member.mode is not None, (
            f"Permissions not set for {member.filename} in {sample_archive.filename} "
            f"(expected {oct(sample_file.permissions)})"
        )
        assert member.mode == sample_file.permissions, (
            f"Permission mismatch for {member.filename} in {sample_archive.filename}: "
            f"got {oct(member.mode) if member.mode is not None else 'None'}, "
            f"expected {oct(sample_file.permissions)}"
        )

    assert member.encrypted == (
        sample_file.password is not None
        or (member.is_file and sample_archive.contents.header_password is not None)
    ), (
        f"Encrypted mismatch for {member.filename}: got {member.encrypted}, expected {sample_file.password is not None}"
    )

    if not features.mtime:
        assert member.mtime is None
    elif sample_file.mtime == MARKER_MTIME_BASED_ON_ARCHIVE_NAME:
        archive_file_mtime = datetime.fromtimestamp(
            os.path.getmtime(archive_path or sample_archive.get_archive_path())
        )
        assert member.mtime == archive_file_mtime, (
            f"Timestamp mismatch for {member.filename} (special check): "
            f"member mtime {member.mtime} vs archive mtime {archive_file_mtime}"
        )
    elif features.rounded_mtime:
        assert member.mtime is not None
        assert abs(member.mtime.timestamp() - sample_file.mtime.timestamp()) <= 1, (
            f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
        )
    else:  # Expect exact match
        assert member.mtime == sample_file.mtime, (
            f"Timestamp mismatch for {member.filename}: {member.mtime} != {sample_file.mtime}"
        )

    # TODO: set feature
    if member.create_system is not None:
        assert member.create_system in {
            CreateSystem.UNIX,
            CreateSystem.UNKNOWN,
        }


def check_iter_members(
    sample_archive: ArchiveInfo,
    archive_path: str | None = None,
    use_rar_stream: bool = False,
    set_file_password_in_constructor: bool = True,
    skip_member_contents: bool = False,
    *,
    use_rapidgzip: bool = False,
    use_indexed_bzip2: bool = False,
    use_python_xz: bool = False,
):
    if sample_archive.creation_info.format == ArchiveFormat.ISO:
        pytest.importorskip("pycdlib")
    elif sample_archive.creation_info.format == ArchiveFormat.RAR:
        pytest.importorskip("rarfile")
    elif sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    elif sample_archive.creation_info.format == ArchiveFormat.ZSTD:
        pytest.importorskip("zstandard")
    elif sample_archive.creation_info.format == ArchiveFormat.LZ4:
        pytest.importorskip("lz4")

    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    if sample_archive.contents.has_multiple_passwords():
        pytest.skip(
            f"Skipping test for {sample_archive.filename} as it has multiple passwords"
        )

    features = sample_archive.creation_info.features

    files_by_name = {file.name: file for file in sample_archive.contents.files}

    constructor_password = sample_archive.contents.header_password

    if (
        set_file_password_in_constructor
        and sample_archive.contents.has_password_in_files()
    ):
        assert constructor_password is None, (
            "Can't set file password in constructor if header password is already set"
        )
        assert not sample_archive.contents.has_multiple_passwords(), (
            "Can't set file password in constructor if there are multiple passwords"
        )
        constructor_password = next(
            iter(
                f.password
                for f in sample_archive.contents.files
                if f.password is not None
            )
        )

    config = ArchiveyConfig(
        use_rar_stream=use_rar_stream,
        use_single_file_stored_metadata=True,
        use_rapidgzip=use_rapidgzip,
        use_indexed_bzip2=use_indexed_bzip2,
        use_python_xz=use_python_xz,
    )
    archive_path_resolved = archive_path or sample_archive.get_archive_path()
    with open_archive(
        archive_path_resolved,
        pwd=constructor_password,
        config=config,
    ) as archive:
        assert archive.format == sample_archive.creation_info.format
        format_info = archive.get_archive_info()
        assert normalize_newlines(format_info.comment) == normalize_newlines(
            sample_archive.contents.archive_comment
        )
        actual_filenames: list[str] = []

        members_iter = (
            ((m, None) for m in archive.get_members())
            if skip_member_contents
            else archive.iter_members_with_io()
        )

        for member, stream in members_iter:
            sample_file = files_by_name.get(member.filename, None)

            check_member_metadata(
                member,
                sample_file,
                sample_archive,
                archive_path=archive_path_resolved,
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

            if sample_file.type == MemberType.FILE and not skip_member_contents:
                assert stream is not None
                contents = stream.read()
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
def test_read_zip_archives(sample_archive: ArchiveInfo, sample_archive_path: str):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, custom_filter=lambda a: a.generate_corrupted_variants),
    ids=lambda a: a.filename,
)
def test_read_truncated_archives(sample_archive: ArchiveInfo, truncated_archive_path: str):
    """Test that reading truncated archives raises ArchiveEOFError."""
    archive_path = pathlib.Path(truncated_archive_path)
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        pytest.xfail("RAR library handles truncated archives without error")

    with pytest.raises((ArchiveEOFError, ArchiveCorruptedError, EOFError)):
        with open_archive(archive_path) as archive:
            for member, stream in archive.iter_members_with_io():
                if stream is not None:
                    stream.read()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, custom_filter=lambda a: a.generate_corrupted_variants),
    ids=lambda a: a.filename,
)
def test_read_corrupted_archives_general(sample_archive: ArchiveInfo, corrupted_archive_path: str):
    """Test that reading generally corrupted archives raises ArchiveCorruptedError."""
    archive_path = pathlib.Path(corrupted_archive_path)
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        pytest.xfail("RAR library handles corrupted archives without error")

    with pytest.raises((ArchiveCorruptedError, zlib.error)):
        # For many corrupted archives, error might be raised on open or during iteration
        with open_archive(str(archive_path)) as archive:
            for member, stream in archive.iter_members_with_io():
                if stream is not None:
                    stream.read()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst", "tar.lz4"],
    ),
    ids=lambda x: x.filename,
)
def test_read_tar_archives(sample_archive: ArchiveInfo, sample_archive_path: str):
    archive_path = pathlib.Path(sample_archive_path)
    if not archive_path.exists():
        pytest.skip("TAR archive not available")
    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        skip_member_contents=True,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["iso"]),
    ids=lambda x: x.filename,
)
def test_read_iso_archives(sample_archive: ArchiveInfo, sample_archive_path: str):
    if not pathlib.Path(sample_archive_path).exists():
        pytest.skip("ISO archive not available")
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["rar"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_rar_stream", [True, False])
def test_read_rar_archives(sample_archive: ArchiveInfo, sample_archive_path: str, use_rar_stream: bool):
    deps = get_dependency_versions()
    if (
        sample_archive.contents.header_password is not None
        and deps.cryptography_version is None
    ):
        pytest.skip("Cryptography is not installed, skipping RAR encrypted-header test")

    if use_rar_stream and deps.unrar_version is None:
        pytest.skip("unrar not installed, skipping RarStreamReader test")

    has_password = sample_archive.contents.has_password()
    has_multiple_passwords = sample_archive.contents.has_multiple_passwords()
    first_file_has_password = sample_archive.contents.files[0].password is not None

    expect_failure = use_rar_stream and (
        has_multiple_passwords
        or (
            has_password
            and not first_file_has_password
            and not sample_archive.contents.header_password
        )
    )

    if expect_failure:
        with pytest.raises(ValueError):
            check_iter_members(sample_archive, archive_path=sample_archive_path, use_rar_stream=use_rar_stream)
    else:
        check_iter_members(
            sample_archive,
            archive_path=sample_archive_path,
            use_rar_stream=use_rar_stream,
            skip_member_contents=deps.unrar_version is None,
        )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["rar"],
        custom_filter=lambda x: x.contents.has_password()
        and not x.contents.has_multiple_passwords()
        and x.contents.header_password is None,
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_rar_stream", [True, False])
def test_read_rar_archives_with_password_in_constructor(
    sample_archive: ArchiveInfo, sample_archive_path: str, use_rar_stream: bool
):
    deps = get_dependency_versions()
    if use_rar_stream and deps.unrar_version is None:
        pytest.skip("unrar not installed, skipping RarStreamReader test")

    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        use_rar_stream=use_rar_stream,
        set_file_password_in_constructor=True,
        skip_member_contents=deps.unrar_version is None,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["zip", "7z"],
        custom_filter=lambda x: x.contents.has_password()
        and not x.contents.has_multiple_passwords()
        and x.contents.header_password is None,
    ),
    ids=lambda x: x.filename,
)
def test_read_zip_and_7z_archives_with_password_in_constructor(
    sample_archive: ArchiveInfo,
    sample_archive_path: str,
):
    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        set_file_password_in_constructor=True,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["7z"]),
    ids=lambda x: x.filename,
)
def test_read_sevenzip_py7zr_archives(sample_archive: ArchiveInfo, sample_archive_path: str):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
    ),
    ids=lambda x: x.filename,
)
def test_read_single_file_compressed_archives(
    sample_archive: ArchiveInfo, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)
