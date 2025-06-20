import collections
import logging
import os
import io # Added for MockCorruptStreamReader
from datetime import datetime, timezone
from typing import Optional, Union, Iterator, BinaryIO # Added Union, Iterator, BinaryIO

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.dependency_checker import get_dependency_versions
from archivey.exceptions import (
    ArchiveError,
    ArchiveMemberCannotBeOpenedError,
    ArchiveCorruptedError, # Added
    ArchiveEncryptedError # Added
)
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, CreateSystem, MemberType # Added ArchiveFormat, ArchiveInfo
from archivey.base_reader import BaseArchiveReader # Added for Mock
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
        assert isinstance(member.create_system, CreateSystem)
        # Specific system checks might be too dependent on how test archives were created.
        # For instance, a TAR file created on Windows might still be CreateSystem.UNIX
        # if the tar program defaults to that. For now, just check type.
        # Example:
        # if features.create_system_is_unix_like:
        #     assert member.create_system == CreateSystem.UNIX
        # else:
        #     assert member.create_system in {CreateSystem.FAT, CreateSystem.NTFS, CreateSystem.UNKNOWN}


    # New metadata fields (type checking primarily, unless specific features are known)
    if features.supports_uid_gid: # Assuming a new feature flag
        assert isinstance(member.uid, int) or member.uid is None
        assert isinstance(member.gid, int) or member.gid is None
    else:
        # For formats not expected to have uid/gid, they should be None
        assert member.uid is None, f"UID should be None for {member.filename} in {sample_archive.filename}"
        assert member.gid is None, f"GID should be None for {member.filename} in {sample_archive.filename}"

    if features.supports_user_group_names: # Assuming a new feature flag
        assert isinstance(member.user_name, str) or member.user_name is None
        assert isinstance(member.group_name, str) or member.group_name is None
    else:
        assert member.user_name is None, f"user_name should be None for {member.filename} in {sample_archive.filename}"
        assert member.group_name is None, f"group_name should be None for {member.filename} in {sample_archive.filename}"


    if features.supports_atime: # Assuming a new feature flag
        assert isinstance(member.atime_with_tz, datetime) or member.atime_with_tz is None
        if member.atime_with_tz:
            assert member.atime_with_tz.tzinfo is not None
            assert isinstance(member.atime, datetime)
            assert member.atime.tzinfo is None
    else:
        assert member.atime_with_tz is None
        assert member.atime is None

    if features.supports_ctime: # Assuming a new feature flag
        assert isinstance(member.ctime_with_tz, datetime) or member.ctime_with_tz is None
        if member.ctime_with_tz:
            assert member.ctime_with_tz.tzinfo is not None
            assert isinstance(member.ctime, datetime)
            assert member.ctime.tzinfo is None
    else:
        assert member.ctime_with_tz is None
        assert member.ctime is None


def check_iter_members(
    sample_archive: SampleArchive,
    archive_path: str,
    set_file_password_in_constructor: bool = True,
    skip_member_contents: bool = False,
    config: Optional[ArchiveyConfig] = None,
):
    skip_if_package_missing(sample_archive.creation_info.format, config)

    if (
        archive_path.endswith(".tar.zst")
        and config is not None
        and config.use_zstandard
    ):
        pytest.skip(
            "Skipping test for .tar.zst archives with zstandard enabled, as zstandard doesn't support seeking"
        )

    if sample_archive.skip_test:
        pytest.skip(f"Skipping test for {sample_archive.filename} as skip_test is True")

    if sample_archive.contents.has_multiple_passwords():
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

        # logger.info(f"files_by_name: {expected_files_by_filename}")
        # logger.info(f"skip_member_contents: {skip_member_contents}")
        # logger.info(f"members_iter: {members_iter}")

        all_contents_by_filename: collections.defaultdict[
            str, list[tuple[ArchiveMember, bytes | None]]
        ] = collections.defaultdict(list)
        all_non_dirs_in_archive = set()

        logger.info(f"members_iter: {members_iter}")
        for member, stream in members_iter:
            logger.info(
                f"member: {member.filename} [{member.type}] [{member.member_id}] {stream=}"
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
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        skip_member_contents=True,
        config=config,
    )


# @pytest.mark.parametrize(
#     "sample_archive",
#     filter_archives(SAMPLE_ARCHIVES, extensions=["iso"]),
#     ids=lambda x: x.filename,
# )
# def test_read_iso_archives(sample_archive: SampleArchive, sample_archive_path: str):
#     if not pathlib.Path(sample_archive_path).exists():
#         pytest.skip("ISO archive not available")
#     check_iter_members(sample_archive, archive_path=sample_archive_path)


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

    config = ArchiveyConfig(use_rar_stream=use_rar_stream)

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
                config=config,
            )
    else:
        check_iter_members(
            sample_archive,
            archive_path=sample_archive_path,
            config=config,
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

    config = ArchiveyConfig(use_rar_stream=use_rar_stream)
    check_iter_members(
        sample_archive,
        archive_path=sample_archive_path,
        config=config,
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


# Tests for the test_member() method
@pytest.mark.parametrize(
    "archive_filename, member_name, is_valid, test_pwd, expected_exception",
    [
        # ZipReader Tests
        ("zip_basic.zip", "file1.txt", True, None, None),
        ("zip_basic.zip", "empty_file.txt", True, None, None),
        ("zip_basic.zip", "directory/", True, None, None), # Non-file members
        # ("zip_corrupt_member.zip", "corrupt_file.txt", False, None, None), # Needs specific sample
        ("zip_encrypted_aes256.zip", "file1.txt", True, "password", None),
        ("zip_encrypted_aes256.zip", "file1.txt", False, "wrongpassword", ArchiveEncryptedError),
        ("zip_encrypted_aes256.zip", "file1.txt", False, None, ArchiveEncryptedError),

        # RarReader Tests
        ("rar4_basic.rar", "file1.txt", True, None, None),
        # ("rar_corrupt_member.rar", "corrupt_file.txt", False, None, None), # Needs specific sample
        ("rar5_encrypted_aes256_headers_off.rar", "file1.txt", True, "password", None),
        ("rar5_encrypted_aes256_headers_off.rar", "file1.txt", False, "wrongpassword", ArchiveEncryptedError),
        ("rar5_encrypted_aes256_headers_off.rar", "file1.txt", False, None, ArchiveEncryptedError),

        # TarReader (uses BaseArchiveReader.test_member)
        ("tar_basic.tar", "file1.txt", True, None, None),
        ("tar_basic.tar", "directory/", True, None, None),

        # SingleFileReader (e.g., GZip, also uses BaseArchiveReader.test_member)
        ("single_file.gz", "single_file.txt", True, None, None), # Name inside GZ might be different
    ],
)
def test_archive_member_test_method(
    archive_filename: str,
    member_name: str,
    is_valid: bool,
    test_pwd: Optional[str],
    expected_exception: Optional[type[Exception]],
    tmp_path, # For creating dummy files if needed, though most rely on SAMPLE_ARCHIVES
):
    sample_archive = next((sa for sa in SAMPLE_ARCHIVES if sa.filename == archive_filename), None)
    if not sample_archive:
        if archive_filename == "single_file.gz": # Create a dummy GZ for base test
            gz_file_path = tmp_path / "single_file.txt.gz"
            import gzip
            with gzip.open(gz_file_path, "wb") as f:
                f.write(b"test content")
            archive_path_to_test = gz_file_path
            # Adjust member_name if SingleFileReader changes it based on filename
            # For this dummy, assume SingleFileReader will make it 'single_file.txt'
            # This part might need adjustment based on SingleFileReader's internal logic for member names.
        else:
            pytest.skip(f"Sample archive {archive_filename} not found for test_member.")
            return
    else:
        archive_path_to_test = sample_archive.get_archive_path()
        # Ensure the member actually exists in the sample for valid tests
        if is_valid and not any(f.name.rstrip('/') == member_name.rstrip('/') for f in sample_archive.contents.files):
             pytest.skip(f"Member {member_name} not found in {archive_filename} for valid test.")
             return


    # Skip if underlying package for the format is missing
    skip_if_package_missing(sample_archive.creation_info.format if sample_archive else ArchiveFormat.GZIP, None)
    if "rar" in archive_filename and get_dependency_versions().unrar_version is None and get_dependency_versions().rarfile_version is None:
        pytest.skip("Skipping RAR test_member as rarfile/unrar is not available.")


    with open_archive(archive_path_to_test, pwd=sample_archive.contents.header_password if sample_archive else None) as archive:
        if expected_exception:
            with pytest.raises(expected_exception):
                archive.test_member(member_name, pwd=test_pwd)
        else:
            try:
                assert archive.test_member(member_name, pwd=test_pwd) == is_valid
            except ArchiveMemberCannotBeOpenedError: # Can happen for links if target is bad
                if is_valid: # If we expected it to be valid, this is a failure
                    raise
                # If we expected it to be invalid, this is an acceptable way for it to be invalid
                assert not is_valid


# Mocking for BaseArchiveReader specific test_member scenario (corrupted stream)
class MockCorruptStreamReader(BaseArchiveReader):
    def __init__(self, archive_path, format_type):
        super().__init__(format_type, archive_path, True, True)
        self._member = ArchiveMember("corrupt_file.txt", 100, 100, datetime.now(), MemberType.FILE)

    def iter_members_for_registration(self) -> Iterator[ArchiveMember]:
        yield self._member

    def open(self, member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[bytes | str] = None) -> BinaryIO:
        # Simulate a corruption error when trying to read this specific member
        if (isinstance(member_or_filename, ArchiveMember) and member_or_filename.filename == "corrupt_file.txt") or \
           (isinstance(member_or_filename, str) and member_or_filename == "corrupt_file.txt"):
            raise ArchiveCorruptedError("Simulated stream corruption")
        # Fallback for other members, though this mock only has one
        bio = io.BytesIO(b"other content")
        return bio

    def close(self) -> None: pass
    def get_archive_info(self) -> ArchiveInfo: return ArchiveInfo(self.format)
    def extract(self, member_or_filename, path=None, pwd=None): pass
    def extractall(self, path=None, members=None, pwd=None, filter=None): return {}
    def resolve_link(self, member): return None


def test_base_reader_test_member_corrupted(tmp_path):
    mock_archive_path = tmp_path / "mock_archive.mock"
    mock_archive_path.touch() # Create a dummy file

    reader = MockCorruptStreamReader(mock_archive_path, ArchiveFormat.UNKNOWN)
    assert reader.test_member("corrupt_file.txt") is False


# Specific test for SingleFileReader (Gzip) metadata
def test_gzip_reader_specific_metadata(tmp_path):
    import gzip
    import time

    # Create a dummy GZ file with a comment
    gz_content = b"This is some test content for GZip."
    gz_comment = "This is a test comment."
    gz_file_path = tmp_path / "test_comment.txt.gz"

    # Record timestamps before file creation for comparison
    # Ensure there's a slight delay to make ctime/atime/mtime distinct if possible
    time_before_creation = datetime.now(timezone.utc)
    time.sleep(0.01)


    with gzip.open(gz_file_path, "wb", compresslevel=9) as f:
        # GzipFile doesn't directly support writing comments in its constructor or write method easily
        # We need to access the underlying GzipFile object if possible, or create header manually
        # For simplicity in testing, we'll assume the library correctly reads comments if present.
        # To properly test writing comment, we'd need to manipulate the Gzip header.
        # Instead, we will focus on testing *reading* a comment if archivey's Gzip reader supports it.
        # The sample_archives.py would be the place to generate such a file.
        # For now, we'll test atime/ctime and trust comment parsing if a sample had it.
        f.write(gz_content)

    time.sleep(0.01) # Ensure mtime is after creation
    time_after_creation = datetime.now(timezone.utc)


    # Test atime/ctime for SingleFileReader
    with open_archive(gz_file_path) as archive:
        assert archive.format == ArchiveFormat.GZIP
        members = archive.get_members()
        assert len(members) == 1
        member = members[0]

        assert member.filename == "test_comment.txt" # Check extension removal

        # Check atime and ctime (these come from the OS for single file archives)
        # These assertions can be a bit flaky due to OS/filesystem timing precision
        # We check if they are datetime objects and fall within a reasonable range
        assert isinstance(member.atime_with_tz, datetime)
        assert member.atime_with_tz.tzinfo == timezone.utc
        # Assert that atime is between time_before_creation and time_after_creation (or slightly after due to access)
        # This is tricky because accessing the file for open_archive updates atime.
        # We can only assert it's a valid datetime for now.
        # A more robust check might involve statting the file *before* open_archive
        # and comparing, but that's outside this direct test.

        assert isinstance(member.ctime_with_tz, datetime)
        assert member.ctime_with_tz.tzinfo == timezone.utc
        # ctime should be between before and after creation
        assert time_before_creation <= member.ctime_with_tz <= time_after_creation + datetime.timedelta(seconds=1) # Add buffer

        # For GZip comment, we'd ideally have a sample from sample_archives.py
        # that is known to have a comment.
        # e.g. sample_gz_with_comment = next(a for a in SAMPLE_ARCHIVES if a.filename == "gzip_with_comment.gz")
        # with open_archive(sample_gz_with_comment.get_archive_path()) as archive_with_comment:
        #    member_with_comment = archive_with_comment.get_members()[0]
        #    assert member_with_comment.comment == "Expected Gzip Comment"
        pass # Placeholder for actual comment test if sample exists


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


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlinks", "symlinks_solid"]),
    ids=lambda x: x.filename,
)
def test_read_symlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, prefixes=["symlink_loop"]),
    ids=lambda x: x.filename,
)
def test_symlink_loop_archives(sample_archive: SampleArchive, sample_archive_path: str):
    """Ensure that archives with symlink loops do not cause infinite loops."""
    with open_archive(sample_archive_path) as archive:
        for member in archive.get_members():
            if member.type == MemberType.SYMLINK:
                if member.link_target == "file5.txt":
                    with archive.open(member) as fh:
                        fh.read()
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
def test_read_hardlinks_archives(
    sample_archive: SampleArchive, sample_archive_path: str
):
    check_iter_members(sample_archive, archive_path=sample_archive_path)
