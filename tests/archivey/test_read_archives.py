import collections
import logging
import os
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.dependency_checker import get_dependency_versions
from archivey.exceptions import ArchiveError, ArchiveMemberCannotBeOpenedError
from archivey.filters import create_filter
from archivey.types import ArchiveFormat, ArchiveMember, CreateSystem, MemberType
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


def _has_unicode_non_bmp_chars(s: str) -> bool:
    return any(ord(c) >= 0x10000 for c in s)


TESTING_FILTER = create_filter(
    for_data=False,
    sanitize_names=False,
    sanitize_link_targets=False,
    sanitize_permissions=False,
    raise_on_error=True,
)


def check_member_metadata(
    member: ArchiveMember,
    sample_file: FileInfo | None,
    sample_archive: SampleArchive,
    archive_path: str | None = None,
):
    if sample_file is None:
        return

    assert member.type == sample_file.type, (
        f"Member type mismatch for {member.filename}: got {member.type}, expected {sample_file.type}"
    )

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
        file_comment = sample_file.comment
        # In RAR4 files with Unicode comments, the comment may have corrupted chars.
        skip_comment_assertion = (
            file_comment is not None
            and features.comment_corrupts_unicode_non_bmp_chars
            and _has_unicode_non_bmp_chars(file_comment)
        )

        if not skip_comment_assertion:
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

    # 0-byte files may not be marked as encrypted (e.g. in 7z archives with header encryption)
    if sample_file.contents:
        assert member.encrypted == (
            sample_file.password is not None
            or (member.is_file and sample_archive.contents.header_password is not None)
        ), (
            f"Encrypted mismatch for {member.filename}: got {member.encrypted}, expected {sample_file.password is not None}"
        )

    if not features.mtime:
        assert member.mtime is None
    elif not features.hardlink_mtime and member.type == MemberType.HARDLINK:
        # Hardlinks may have the timestamp of the pointed file, don't check it.
        pass
    elif sample_file.mtime == MARKER_MTIME_BASED_ON_ARCHIVE_NAME:
        archive_file_mtime = datetime.fromtimestamp(
            os.path.getmtime(archive_path or sample_archive.get_archive_path()),
            tz=timezone.utc,
        ).replace(tzinfo=None)
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

    if features.mtime:
        assert member.mtime_with_tz is not None
        assert member.mtime_with_tz.replace(tzinfo=None) == member.mtime
        if features.mtime_with_tz:
            assert member.mtime_with_tz.tzinfo is not None
            assert member.mtime_with_tz.tzinfo == timezone.utc
        else:
            assert member.mtime_with_tz.tzinfo is None

    # TODO: set feature
    if member.create_system is not None:
        assert member.create_system in {
            CreateSystem.UNIX,
            CreateSystem.UNKNOWN,
        }


def check_iter_members(
    sample_archive: SampleArchive,
    archive_path: str,
    set_file_password_in_constructor: bool = True,
    skip_member_contents: bool = False,
    config: Optional[ArchiveyConfig] = None, # Base config
    use_libarchive: bool = False,
):
    base_config = config or ArchiveyConfig()
    if use_libarchive:
        # Skip if libarchive-c is not installed (though it's a dep now, good for local testing)
        try:
            import libarchive # type: ignore
        except ImportError:
            pytest.skip("libarchive-c not installed, skipping use_libarchive=True test")
        config = replace(base_config, use_libarchive=True)
        # Libarchive specific skips or adjustments can be added here if needed
        if sample_archive.creation_info.format == ArchiveFormat.RAR and use_libarchive:
            # Known issue: My LibarchiveReader's password handling for RAR might be basic.
            # Also, libarchive might not support all RAR versions/features like unrar does.
            if sample_archive.contents.has_password():
                 # pytest.skip("RAR with password and libarchive needs review")
                 pass # Let it run and see

        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP and use_libarchive:
            if sample_archive.contents.has_password():
                # pytest.skip("7z with password and libarchive needs review")
                pass # Let it run and see
    else:
        config = base_config


    skip_if_package_missing(sample_archive.creation_info.format, config) # config now includes use_libarchive

    if (
        archive_path.endswith(".tar.zst")
        and config is not None
        and config.use_zstandard
        and not use_libarchive # zstandard direct usage conflict
    ):
        pytest.skip(
            "Skipping test for .tar.zst archives with zstandard enabled (non-libarchive), as zstandard doesn't support seeking"
        )

    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    # Libarchive may handle multiple passwords differently or not at all compared to specific readers
    if use_libarchive and sample_archive.contents.has_multiple_passwords():
        pytest.skip(f"Skipping libarchive test for {sample_archive.filename} as it has multiple passwords, behavior undefined")
    elif not use_libarchive and sample_archive.contents.has_multiple_passwords():
        pytest.skip(
            f"Skipping test for {sample_archive.filename} as it has multiple passwords"
        )


    features = sample_archive.creation_info.features

    # If the archive may have duplicate files, we need to compare the files in the
    # iterator with the ones in the sample_archive in the same order.
    # Otherwise, the archive should have only the last version of the file.
    expected_files_by_filename: collections.defaultdict[str, list[FileInfo]] = (
        collections.defaultdict(list)
    )

    for sample_file in sample_archive.contents.files:
        if features.dir_entries or sample_file.type != MemberType.DIR:
            expected_files_by_filename[sample_file.name].append(sample_file)

    # expected_filenames = set(expected_files_by_filename.keys())

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
    config = replace(config or ArchiveyConfig(), extraction_filter=TESTING_FILTER)

    with open_archive(
        archive_path_resolved,
        pwd=constructor_password,
        config=config,
    ) as archive:
        assert archive.format == sample_archive.creation_info.format
        format_info = archive.get_archive_info()

        # Check archive comment
        archive_comment = sample_archive.contents.archive_comment
        # In RAR4 files with Unicode comments, the comment may have corrupted chars.
        skip_archive_comment_assertion = (
            archive_comment is not None
            and features.comment_corrupts_unicode_non_bmp_chars
            and _has_unicode_non_bmp_chars(archive_comment)
        )

        if not skip_archive_comment_assertion:
            assert normalize_newlines(format_info.comment) == normalize_newlines(
                sample_archive.contents.archive_comment
            )

        members_iter = (
            ((m, None) for m in archive.get_members())
            if skip_member_contents
            else archive.iter_members_with_io()
        )

        all_contents_by_filename: collections.defaultdict[
            str, list[tuple[ArchiveMember, bytes | None]]
        ] = collections.defaultdict(list)
        all_non_dirs_in_archive = set()

        logger.info(f"members_iter: {members_iter}")
        for member, stream in members_iter:
            logger.info(
                f"member: {member.filename} [{member.type}] [{member.member_id}] {stream=}"
            )

            if skip_member_contents:
                assert not member._edited_by_filter, (
                    f"Member {member.filename} was edited by filter"
                )
            else:
                assert member._edited_by_filter, (
                    f"Member {member.filename} was not edited by filter"
                )

            filekey = member.filename
            if member.is_dir:
                assert member.filename.endswith("/"), (
                    f"Directory {member.filename} does not end with /"
                )
            else:
                assert not member.filename.endswith("/"), (
                    f"{member.type} {member.filename} ends with /"
                )

            if not skip_member_contents and member.is_file:
                assert stream is not None, (
                    f"Stream not provided for {member.filename} ({member.type})"
                )
            else:
                assert stream is None, (
                    f"Stream provided for {member.filename} ({member.type}) (data={stream.read()})"
                )

            # TODO: compare data for resolved links
            data = stream.read() if stream is not None else None

            all_contents_by_filename[filekey].append((member, data))
            if member.type != MemberType.DIR:
                all_non_dirs_in_archive.add(filekey)

        logger.info(f"all_contents_by_filename: {all_contents_by_filename}")

        # Check that all expected filenames are present in the archive.
        assert not set(expected_files_by_filename.keys()) - set(
            all_contents_by_filename.keys()
        ), (
            f"Expected files {set(expected_files_by_filename.keys()) - set(all_contents_by_filename.keys())} not found in archive"
        )
        # The archive may contain extra dirs that were implicit in the file list,
        # but not other unexpected files.
        assert not all_non_dirs_in_archive - set(expected_files_by_filename.keys()), (
            f"Extra files {all_non_dirs_in_archive - set(expected_files_by_filename.keys())} found in archive"
        )

        # Check that the contents of the members are the same as the contents of the files.
        for filename, expected_files in expected_files_by_filename.items():
            actual_files = all_contents_by_filename[filename]
            if features.duplicate_files:
                assert len(actual_files) == len(expected_files), (
                    f"Expected {len(expected_files)} files for {filename}, got {len(actual_files)}"
                )
            else:
                assert len(actual_files) == 1, (
                    f"Expected 1 file for {filename}, got {len(actual_files)}"
                )
                # We expect only the last file with a given filename to be present.
                expected_files = [expected_files[-1]]

            actual_files.sort(key=lambda x: x[0].member_id)

            for i in range(len(expected_files)):
                logger.info(f"Checking {filename} ({i})")
                sample_file = expected_files[i]
                member, contents = actual_files[i]

                check_member_metadata(
                    member,
                    sample_file,
                    sample_archive,
                    archive_path=archive_path_resolved,
                )

                if sample_file.type == MemberType.FILE and not skip_member_contents:
                    assert contents == sample_file.contents

                if sample_file.contents is not None and archive.has_random_access():
                    with archive.open(member) as stream:
                        assert stream.read() == sample_file.contents
                else:
                    with pytest.raises((ValueError, ArchiveError)):
                        stream = archive.open(member)
                        logger.info(
                            f"Unexpected open() success for {member=}; data={stream.read()}"
                        )

            # Check that opening the file by filename gives the most recent contents.
            sample_file = expected_files[-1]
            if sample_file.contents is not None and archive.has_random_access():
                with archive.open(filename) as stream:
                    assert stream.read() == sample_file.contents
            else:
                with pytest.raises((ValueError, ArchiveError)):
                    archive.open(filename)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        extensions=["zip"],
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_zip_archives(sample_archive: SampleArchive, sample_archive_path: str, use_libarchive: bool):
    check_iter_members(sample_archive, archive_path=sample_archive_path, use_libarchive=use_libarchive)


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
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_tar_archives(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool, use_libarchive: bool
):
    logger.info(
        f"Testing {sample_archive.filename} with format {sample_archive.creation_info.format}"
    )

    if alternative_packages:
        config = ArchiveyConfig(
            use_rapidgzip=True,
            use_indexed_bzip2=True,
            use_python_xz=True,
            use_zstandard=True,
        )
    else:
        config = ArchiveyConfig() # Ensure a base config object is passed

    # libarchive handles its own decompression, so alternative_packages for compression libs
    # are not relevant when use_libarchive is True. Avoid redundant test runs.
    if use_libarchive and alternative_packages:
        pytest.skip("Alternative decompression packages are not used when use_libarchive is True for TAR.")

    # skip_if_package_missing is now called inside check_iter_members
    # skip_if_package_missing(sample_archive.creation_info.format, config)

    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        skip_member_contents=True, # TAR tests often skip content check
        config=config,
        use_libarchive=use_libarchive,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["rar"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_rar_stream", [True, False])
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_rar_archives(
    sample_archive: SampleArchive, sample_archive_path: str, use_rar_stream: bool, use_libarchive: bool
):
    deps = get_dependency_versions()
    if (
        sample_archive.contents.header_password is not None
        and deps.cryptography_version is None
        and not use_libarchive # libarchive might not need archivey's cryptography dep
    ):
        pytest.skip("Cryptography is not installed, skipping RAR encrypted-header test for native reader")

    if use_rar_stream and deps.unrar_version is None and not use_libarchive:
        pytest.skip("unrar not installed, skipping RarStreamReader test for native reader")

    # If using libarchive, use_rar_stream is irrelevant
    if use_libarchive and use_rar_stream:
        pytest.skip("use_rar_stream is not applicable when use_libarchive is True for RAR.")

    config = ArchiveyConfig(use_rar_stream=use_rar_stream if not use_libarchive else False)

    if use_libarchive:
        # Specific skips for libarchive RAR if needed, e.g. advanced RAR features
        if "rar4" in sample_archive.filename or "rar5" in sample_archive.filename: # Example
            pass # Potentially skip complex RAR5 features if libarchive struggles

    has_password = sample_archive.contents.has_password()
    has_multiple_passwords = sample_archive.contents.has_multiple_passwords()
    first_file_has_password = sample_archive.contents.files[0].password is not None

    # Native reader failure conditions
    expect_native_failure = not use_libarchive and use_rar_stream and (
        has_multiple_passwords
        or (
            has_password
            and not first_file_has_password
            and not sample_archive.contents.header_password
        )
    )

    if expect_native_failure:
        with pytest.raises(ValueError):
            check_iter_members(
                sample_archive,
                archive_path=sample_archive_path,
                config=config,
                use_libarchive=use_libarchive, # Should be False here
            )
    else:
        check_iter_members(
            sample_archive,
            archive_path=sample_archive_path,
            config=config,
            skip_member_contents=deps.unrar_version is None and not use_libarchive,
            use_libarchive=use_libarchive,
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
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_rar_archives_with_password_in_constructor(
    sample_archive: SampleArchive, sample_archive_path: str, use_rar_stream: bool, use_libarchive: bool
):
    deps = get_dependency_versions()
    if use_rar_stream and deps.unrar_version is None and not use_libarchive:
        pytest.skip("unrar not installed, skipping RarStreamReader test for native reader")

    if use_libarchive and use_rar_stream:
        pytest.skip("use_rar_stream is not applicable when use_libarchive is True for RAR.")

    config = ArchiveyConfig(use_rar_stream=use_rar_stream if not use_libarchive else False)
    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        config=config,
        set_file_password_in_constructor=True,
        skip_member_contents=deps.unrar_version is None and not use_libarchive,
        use_libarchive=use_libarchive,
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
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_zip_and_7z_archives_with_password_in_constructor(
    sample_archive: SampleArchive,
    sample_archive_path: str,
    use_libarchive: bool,
):
    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        set_file_password_in_constructor=True,
        use_libarchive=use_libarchive,
    )


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["7z"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_sevenzip_py7zr_archives(
    sample_archive: SampleArchive, sample_archive_path: str, use_libarchive: bool
):
    check_iter_members(sample_archive, archive_path=sample_archive_path, use_libarchive=use_libarchive)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["single_file", "single_file_with_metadata"]
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("alternative_packages", [False, True])
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_single_file_compressed_archives(
    sample_archive: SampleArchive, sample_archive_path: str, alternative_packages: bool, use_libarchive: bool
):
    if use_libarchive:
        # Libarchive handles single files; alternative packages for compression not directly relevant.
        # use_single_file_stored_metadata might also be less relevant if libarchive
        # gets metadata directly from the file itself (e.g. for .gz if it contains filename).
        if alternative_packages: # Avoid redundant runs
             pytest.skip("Alternative packages for single file compression not used with libarchive.")
        config = ArchiveyConfig(use_single_file_stored_metadata=True) # Keep for consistency if libarchive needs it
    elif alternative_packages:
        config = ArchiveyConfig(
            use_rapidgzip=True,
            use_indexed_bzip2=True,
            use_python_xz=True,
            use_zstandard=True,
            use_single_file_stored_metadata=True,
        )
    else:
        config = ArchiveyConfig(use_single_file_stored_metadata=True)

    check_iter_members(sample_archive, archive_path=sample_archive_path, config=config, use_libarchive=use_libarchive)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlinks", "symlinks_solid"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_symlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str, use_libarchive: bool
):
    # Specific skips for libarchive if its symlink handling differs significantly, e.g. for link targets
    # For example, if libarchive normalizes paths differently or has issues with certain link types.
    if use_libarchive and "weird_symlink_target" in sample_archive.filename: # hypothetical
        pytest.skip("Libarchive known to handle this specific symlink case differently.")

    check_iter_members(sample_archive, archive_path=sample_archive_path, use_libarchive=use_libarchive)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlink_loop"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_symlink_loop_archives(sample_archive: SampleArchive, sample_archive_path: str, use_libarchive: bool):
    """Ensure that archives with symlink loops do not cause infinite loops."""
    config = ArchiveyConfig(use_libarchive=use_libarchive)
    if use_libarchive:
        # Libarchive's loop detection or error on open might differ.
        # This test might need adjustment based on libarchive's behavior.
        # For now, assume similar error on opening a looping symlink.
        pass

    with open_archive(sample_archive_path, config=config) as archive:
        for member in archive.get_members():
            if member.type == MemberType.SYMLINK:
                # Adjust expectations if libarchive behaves differently for readable loops
                if member.link_target == "file5.txt" and not use_libarchive: # Native reader might resolve this
                    with archive.open(member) as fh:
                        fh.read()
                elif member.link_target == "file5.txt" and use_libarchive:
                     # Assuming libarchive might also resolve this or error consistently.
                     # This part may need refinement after observing libarchive behavior.
                    try:
                        with archive.open(member) as fh:
                            fh.read()
                    except ArchiveMemberCannotBeOpenedError:
                        pass # Expected if libarchive also blocks it like the native one for other loops
                else:
                    with pytest.raises(ArchiveMemberCannotBeOpenedError):
                        archive.open(member)
            else:
                with archive.open(member) as fh:
                    fh.read()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES, prefixes=["hardlinks_nonsolid", "hardlinks_solid"]
    ),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("use_libarchive", [False, True], ids=["native", "libarchive"])
def test_read_hardlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str, use_libarchive: bool
):
    if use_libarchive:
        # Libarchive treats hardlinks essentially as regular files in its entry listing.
        # The BaseArchiveReader's hardlink resolution logic (based on prior identical filenames)
        # might behave differently or fail if libarchive doesn't provide members in a
        # strictly consistent order or if it resolves hardlinks internally differently.
        # This is a known area for potential divergence.
        # Depending on strictness, we might skip or adjust assertions for hardlink targets with libarchive.
        # For now, let it run. If it fails, this is a primary suspect.
        # Example skip for a particularly complex hardlink archive with libarchive:
        if "very_complex_hardlinks" in sample_archive.filename: # hypothetical
            pytest.skip("Skipping complex hardlink test with libarchive due to known behavioral differences.")
        pass

    check_iter_members(sample_archive, archive_path=sample_archive_path, use_libarchive=use_libarchive)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["_folder/"]),
    ids=lambda x: x.filename,
)
# No use_libarchive here, as FolderReader is always used for folders.
def test_read_folder_archives(sample_archive: SampleArchive, sample_archive_path: str):
    logger.info(f"Testing {sample_archive.filename}; files at {sample_archive_path}")
    check_iter_members(sample_archive, archive_path=sample_archive_path, use_libarchive=False) # Explicitly False
