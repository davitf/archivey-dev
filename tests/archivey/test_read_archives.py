import logging
import os
import pathlib
from datetime import datetime
from typing import Optional

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive, ArchiveFormat
from archivey.dependency_checker import get_dependency_versions
from archivey.types import ArchiveMember, CreateSystem, MemberType, ArchiveFormat
from tests.archivey.sample_archives import (
    MARKER_MTIME_BASED_ON_ARCHIVE_NAME,
    SAMPLE_ARCHIVES,
    FileInfo,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import (
    get_crc32,
    normalize_newlines,
    skip_if_package_missing,
)


def check_member_metadata(
    member: ArchiveMember,
    sample_file: FileInfo | None,
    sample_archive: SampleArchive,
    archive_path: str | None = None,
):
    if sample_file is None:
        return

    features = sample_archive.creation_info.features

    if member.is_file:
        if features.file_size:
            assert member.file_size == len(sample_file.contents or b"")
        else:
            assert member.file_size is None

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
    sample_archive: SampleArchive,
    archive_path: str | None = None,
    use_rar_stream: bool = False,
    set_file_password_in_constructor: bool = True,
    skip_member_contents: bool = False,
    config: Optional[ArchiveyConfig] = None,
):
    skip_if_package_missing(sample_archive.creation_info.format, config)

    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    if sample_archive.contents.has_multiple_passwords():
        pytest.skip(
            f"Skipping test for {sample_archive.filename} as it has multiple passwords"
        )

    features = sample_archive.creation_info.features

    # Build a map of expected files for quick lookup.
    expected_files_map: dict[str, FileInfo] = {}

    TAR_FORMATS_FAMILY = {
        ArchiveFormat.TAR,
        ArchiveFormat.TAR_GZ,
        ArchiveFormat.TAR_BZ2,
        ArchiveFormat.TAR_XZ,
        ArchiveFormat.TAR_ZSTD,
        ArchiveFormat.TAR_LZ4,
    }

    if sample_archive.contents and sample_archive.contents.files:
        # Conditional population based on format and duplicate support
        if sample_archive.creation_info.features.duplicate_files and \
           sample_archive.creation_info.format == ArchiveFormat.RAR:
            # RAR: last occurrence wins, no _DUPE suffixes needed in map keys
            for file_info in sample_archive.contents.files:
                expected_files_map[file_info.name] = file_info
        elif sample_archive.creation_info.features.duplicate_files and \
             sample_archive.creation_info.format in TAR_FORMATS_FAMILY:
            # TAR: duplicates are present, map them in reverse order of appearance in sample_archive.contents.files
            # to match observed reader behavior (last added to archive is often first read for same name).

            # First, group FileInfo objects by their base name
            grouped_by_name: dict[str, list[FileInfo]] = {}
            for file_info in sample_archive.contents.files:
                grouped_by_name.setdefault(file_info.name, []).append(file_info)

            for name, infos_for_name in grouped_by_name.items():
                # Iterate in reverse of how they appear in sample_archive.contents.files list for this specific name
                # The first one (i=0) gets the base name, subsequent ones get _DUPE, _DUPE_DUPE etc.
                # This means expected_files_map["file.txt"] will map to the *last* FileInfo object with name "file.txt"
                # in sample_archive.contents.files, and "file.txt_DUPE" to the second to last, etc.
                for i, file_info_to_map in enumerate(reversed(infos_for_name)):
                    filekey = name
                    if i > 0:
                        filekey += "_DUPE" * i  # Append 1x_DUPE, 2x_DUPE etc.
                    expected_files_map[filekey] = file_info_to_map
        elif sample_archive.creation_info.features.duplicate_files: # Handles ZIP and any other future formats with duplicates
            # Original logic for ZIP: first occurrence gets base name, next gets _DUPE etc.
            processed_counts: dict[str, int] = {}
            for file_info in sample_archive.contents.files:
                base_name = file_info.name
                count = processed_counts.get(base_name, 0)
                processed_counts[base_name] = count + 1

                filekey = base_name
                if count > 0:
                    filekey += "_DUPE" * count
                expected_files_map[filekey] = file_info
        else: # No duplicate_files support, or duplicate_files is False
            # Each file name is unique in the map, last one wins if sample_archive.contents.files has duplicates by name
            for file_info in sample_archive.contents.files:
                expected_files_map[file_info.name] = file_info

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

        processed_member_names_count: dict[str, int] = {}
        processed_keys_from_expected_map: set[str] = set()

        for member, stream in members_iter:
            actual_member_filename = member.filename
            occurrence_count = processed_member_names_count.get(actual_member_filename, 0) + 1
            processed_member_names_count[actual_member_filename] = occurrence_count

            key_for_sample_lookup = actual_member_filename
            is_rar_and_handles_duplicates_by_last_wins = (
                sample_archive.creation_info.features.duplicate_files
                and sample_archive.creation_info.format == ArchiveFormat.RAR
            )

            if not is_rar_and_handles_duplicates_by_last_wins and \
               sample_archive.creation_info.features.duplicate_files and \
               occurrence_count > 1:
                key_for_sample_lookup = actual_member_filename + "_DUPE" * (occurrence_count - 1)
            elif is_rar_and_handles_duplicates_by_last_wins and occurrence_count > 1:
                pytest.fail(f"RAR reader yielded duplicate member name '{actual_member_filename}' when not expected (expected only last instance).")
            elif not sample_archive.creation_info.features.duplicate_files and occurrence_count > 1:
                pytest.fail(f"Reader yielded duplicate member name '{actual_member_filename}' for format '{sample_archive.creation_info.format}' not supporting duplicates.")

            sample_file = expected_files_map.get(key_for_sample_lookup)
            if sample_file is not None:
                processed_keys_from_expected_map.add(key_for_sample_lookup)

            check_member_metadata( # This function call seems to be outside the original diff, but it's part of the same logical block.
                member, # The diff tool might complain if this is not part of the search block.
                sample_file,
                sample_archive,
                archive_path=archive_path_resolved,
            )

            if sample_file is None:
                # Handle cases where the archive might contain directory entries not explicitly listed
                # in sample_archive.contents.files (e.g., auto-created parent dirs).
                if member.is_dir: # Assuming ArchiveMember has an is_dir attribute or similar
                    # If it's a directory and not in our explicit list, we might ignore it
                    # if the format creates directory entries automatically.
                    logger.debug( # Assuming logger is defined
                        f"Ignoring directory member not in sample_archive: {member.filename}"
                    )
                    if stream:  # pragma: no cover
                        stream.close()
                    continue
                else:
                    # If it's a file and not in our list, that's a problem.
                    if stream:  # pragma: no cover
                        stream.close()
                    pytest.fail(
                        f"Archive member '{actual_member_filename}' (key: '{key_for_sample_lookup}') not found in expected_files_map"
                    )

            # actual_filenames.append(member.filename) # This is replaced by processed_keys_from_expected_map

            if sample_file.type == MemberType.FILE and not skip_member_contents: # Assuming sample_file is not None here
                assert stream is not None
                contents = stream.read()
                assert contents == sample_file.contents # Assuming sample_file.contents exists

        # Adjust final missing/extra files check:
        expected_keys = set(expected_files_map.keys())
        missing_files = expected_keys - processed_keys_from_expected_map
        assert not missing_files, f"Missing files that were expected but not found in archive iteration: {missing_files}"

        # This check is for internal consistency:
        # All keys that were processed should have been derived from expected_keys.
        # If extra_keys_that_were_somehow_processed is not empty, it implies a logic error
        # in how key_for_sample_lookup was generated or how processed_keys_from_expected_map was populated.
        extra_keys_that_were_somehow_processed = processed_keys_from_expected_map - expected_keys
        assert not extra_keys_that_were_somehow_processed, (
            f"Internal test logic error: Processed keys contain entries not derivable from the expected map. "
            f"Extra processed keys: {extra_keys_that_were_somehow_processed}. "
            f"Expected keys: {expected_keys}. "
            f"Processed member names count: {processed_member_names_count}."
        )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["zip"]),
    ids=lambda x: x.filename,
)
def test_read_zip_archives(sample_archive: SampleArchive, sample_archive_path: str):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst", "tar.lz4"],
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("alternative_packages", [False, True])
def test_read_tar_archives(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    # logger.info(
    #     f"Testing {sample_archive.filename} with format {sample_archive.creation_info.format}"
    # ) # Removed logger

    if alternative_packages:
        config = ArchiveyConfig(
            use_rapidgzip=True,
            use_indexed_bzip2=True,
            use_python_xz=True,
            use_zstandard=True,
        )
    else:
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        skip_member_contents=True,
        config=config,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["iso"]),
    ids=lambda x: x.filename,
)
def test_read_iso_archives(sample_archive: SampleArchive, sample_archive_path: str):
    if not pathlib.Path(sample_archive_path).exists():
        pytest.skip("ISO archive not available")
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["rar"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_rar_stream", [True, False])
def test_read_rar_archives(
    sample_archive: SampleArchive, sample_archive_path: str, use_rar_stream: bool
):
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
            check_iter_members(
                sample_archive,
                archive_path=sample_archive_path,
                use_rar_stream=use_rar_stream,
            )
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
    sample_archive: SampleArchive, sample_archive_path: str, use_rar_stream: bool
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
    sample_archive: SampleArchive,
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
def test_read_sevenzip_py7zr_archives(
    sample_archive: SampleArchive, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("alternative_packages", [False, True])
def test_read_single_file_compressed_archives(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool
):
    if alternative_packages:
        config = ArchiveyConfig(
            use_rapidgzip=True,
            use_indexed_bzip2=True,
            use_python_xz=True,
            use_zstandard=True,
            use_single_file_stored_metadata=True,
        )
    else:
        config = ArchiveyConfig(use_single_file_stored_metadata=True)

    check_iter_members(sample_archive, archive_path=sample_archive_path, config=config)
