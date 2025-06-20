from pathlib import Path

import pytest

from archivey.config import OverwriteMode
from archivey.core import open_archive
from archivey.exceptions import ArchiveEncryptedError, ArchiveError
from archivey.types import ArchiveFormat, MemberType
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing

# Select encrypted sample archives that use a single password and no header password
ENCRYPTED_ARCHIVES = filter_archives(
    SAMPLE_ARCHIVES,
    prefixes=[
        "encryption",
        "encryption_with_plain",
        "encryption_solid",
        "encryption_with_symlinks",
    ],
    extensions=["zip", "rar", "7z"],
    custom_filter=lambda a: not a.contents.has_multiple_passwords()
    and a.contents.header_password is None,
)


def _archive_password(sample: SampleArchive) -> str:
    for f in sample.contents.files:
        if f.password is not None:
            return f.password
    raise ValueError("sample archive has no password")


def _first_encrypted_file(sample: SampleArchive):
    for f in sample.contents.files:
        if f.password is not None and f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no encrypted files")


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_open_archive(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path, pwd=pwd) as archive:
        encrypted = _first_encrypted_file(sample_archive)
        with archive.open(encrypted.name) as fh:
            assert fh.read() == encrypted.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_iter_members(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip(
                "py7zr does not support password parameter for iter_members_with_io"
            )
        contents = {}
        for m, stream in archive.iter_members_with_io(pwd=pwd):
            if m.is_file:
                assert stream is not None
                contents[m.filename] = stream.read()
        for f in sample_archive.contents.files:
            if f.type == MemberType.FILE:
                assert contents[f.name] == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_password_in_open(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    with open_archive(sample_archive_path) as archive:
        for f in sample_archive.contents.files:
            if f.type == MemberType.FILE:
                with archive.open(f.name, pwd=pwd) as fh:
                    assert fh.read() == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_open(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    encrypted = _first_encrypted_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            with archive.open(encrypted.name, pwd=wrong) as f:
                f.read()


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_iter_members_read(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.skip(
            "py7zr does not support password parameter for iter_members_with_io"
        )

    wrong = "wrong_password"
    with open_archive(sample_archive_path) as archive:
        for m, stream in archive.iter_members_with_io(pwd=wrong):
            assert stream is not None
            if m.is_file:
                if m.encrypted:
                    with pytest.raises((ArchiveEncryptedError, ArchiveError)):
                        stream.read()
                else:
                    stream.read()


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_wrong_password_iter_members_no_read(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip(
                "py7zr does not support password parameter for iter_members_with_io"
            )
        for _m, _stream in archive.iter_members_with_io(pwd=wrong):
            pass


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extract_with_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    pwd = _archive_password(sample_archive)
    dest = tmp_path / "out"
    dest.mkdir()
    encrypted = _first_encrypted_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip("py7zr extract password support incomplete")
        archive.config.overwrite_mode = OverwriteMode.OVERWRITE
        path = archive.extract(encrypted.name, dest, pwd=pwd)
    extracted_path = Path(path or dest / encrypted.name)
    with open(extracted_path, "rb") as f:
        assert f.read() == encrypted.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extractall_with_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
    #     pytest.skip("py7zr extractall password support incomplete")

    pwd = _archive_password(sample_archive)
    dest = tmp_path / "all"
    dest.mkdir()
    with open_archive(sample_archive_path) as archive:
        archive.extractall(dest, pwd=pwd)

    for f in sample_archive.contents.files:
        if f.type == MemberType.FILE:
            path = dest / f.name
            assert path.exists()
            with open(path, "rb") as fh:
                assert fh.read() == f.contents


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extract_wrong_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    wrong = "wrong_password"
    dest = tmp_path / "out"
    dest.mkdir()
    encrypted = _first_encrypted_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
            pytest.skip("py7zr extract password support incomplete")
        archive.config.overwrite_mode = OverwriteMode.OVERWRITE
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            archive.extract(encrypted.name, dest, pwd=wrong)


@pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
def test_extractall_wrong_password(
    tmp_path: Path, sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
    #     pytest.skip("py7zr extractall password support incomplete")

    wrong = "wrong_password"
    dest = tmp_path / "all"
    dest.mkdir()
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            archive.extractall(dest, pwd=wrong)


# @pytest.mark.parametrize("sample_archive", ENCRYPTED_ARCHIVES, ids=lambda a: a.filename)
@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_no_password(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path) as archive:
        for member, stream in archive.iter_members_with_io():
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
        extensions=["rar", "7z"]
    ),
    ids=lambda a: a.filename,
)
def test_open_encrypted_symlink(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    # Scenario 1: Correct password for symlink targeting encrypted file.
    # Symlink: "encrypted_link_to_secret.txt" (links to "secret.txt", pwd="pwd", content="Secret")
    # The symlink data itself might be encrypted with "pwd" (RAR), or just the target is.
    with open_archive(sample_archive_path) as archive:
        with archive.open("encrypted_link_to_secret.txt", pwd="pwd") as f:
            assert f.read() == b"Secret"

    # Scenario 2: Wrong password for symlink targeting encrypted file.
    # Symlink: "encrypted_link_to_secret.txt"
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            # Password "wrong_password" should fail to open the symlink data (RAR)
            # or fail to open the target "secret.txt" (7z, after successfully reading plain symlink data).
            with archive.open("encrypted_link_to_secret.txt", pwd="wrong_password") as f:
                f.read()

    # Scenario 3: Correct password for symlink, but target has different encryption.
    # Symlink: "encrypted_link_to_very_secret.txt" (links to "very_secret.txt", content="Very secret", target_pwd="longpwd")
    # The symlink data itself might be protected by "pwd" (RAR).
    with open_archive(sample_archive_path) as archive:
        with pytest.raises((ArchiveEncryptedError, ArchiveError)):
            # Password "pwd" might open the symlink object itself (if it's encrypted, e.g. RAR with pwd="pwd" for link data)
            # but then opening the target "very_secret.txt" (encrypted with "longpwd") should fail.
            with archive.open("encrypted_link_to_very_secret.txt", pwd="pwd") as f:
                f.read()

    # Scenario 4: Symlink (plain) to an encrypted file, password provided for the target.
    # Symlink: "plain_link_to_secret.txt" (links to "secret.txt", content="Secret", target_pwd="pwd")
    # The symlink itself is "plain" (not encrypted).
    with open_archive(sample_archive_path) as archive:
        # Password "pwd" is for the target "secret.txt".
        with archive.open("plain_link_to_secret.txt", pwd="pwd") as f:
            assert f.read() == b"Secret"

    # Scenario 5: Symlink (encrypted) to a plain file, correct password for symlink.
    # Symlink: "encrypted_link_to_not_secret.txt" (links to "not_secret.txt", content="Not secret", link_pwd="longpwd")
    # Target "not_secret.txt" is plain.
    with open_archive(sample_archive_path) as archive:
        with archive.open("encrypted_link_to_not_secret.txt", pwd="longpwd") as f:
            assert f.read() == b"Not secret"

    # Scenario 6: Symlink (encrypted) to a plain file, wrong password for symlink.
    # Symlink: "encrypted_link_to_not_secret.txt" (links to "not_secret.txt", content="Not secret", link_pwd="longpwd")
    # Target "not_secret.txt" is plain.
    # This test is more relevant for formats that encrypt symlink data itself (like RAR).
    # For 7z, if the symlink data is plain, a wrong password here might be ignored for the link
    # and then it would read the plain target successfully. However, the current 7z implementation
    # passes the password to iter_members_with_io, which might try to use it.
    # The current implementation for 7z in open() calls iter_members_with_io,
    # which would use the password. If the symlink target data is fetched and it's plain,
    # it should succeed. If the symlink itself (the object containing target path) is plain,
    # py7zr might not use the password for it.
    # RAR encrypts the symlink *target path* if a password is set for the symlink member.
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        with open_archive(sample_archive_path) as archive:
            with pytest.raises((ArchiveEncryptedError, ArchiveError)):
                # "encrypted_link_to_not_secret.txt" symlink data is encrypted with "longpwd" in RAR.
                # Using "wrong_password" should fail to decrypt/read this symlink data.
                with archive.open("encrypted_link_to_not_secret.txt", pwd="wrong_password") as f:
                    f.read()
    elif sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        # For 7z, symlinks' target paths are typically stored plainly.
        # If "encrypted_link_to_not_secret.txt" symlink object is plain, and target "not_secret.txt" is plain:
        # Providing a "wrong_password" to open() -> iter_members_with_io() might still succeed if py7zr
        # doesn't apply passwords to reading plain symlink definitions or plain targets.
        # The current archive definition for "encryption_with_symlinks" for 7z has:
        # Symlink("encrypted_link_to_not_secret.txt", ..., password="longpwd")
        # This 'password' field in FileInfo for 7z means the *target data* is encrypted *if* the target is a file.
        # But for symlinks, py7zr stores the target path. If that storage itself is encrypted, this applies.
        # The `iter_members_with_io` in sevenzip_reader passes pwd.
        # Let's assume for 7z, if the symlink object itself is not what's encrypted by "longpwd",
        # but rather the conceptual "reading of the link" if it were data:
        # If "encrypted_link_to_not_secret.txt" means the symlink *entry* is tied to "longpwd":
        with open_archive(sample_archive_path) as archive:
            with pytest.raises((ArchiveEncryptedError, ArchiveError)): # Expecting failure due to "wrong_password"
                with archive.open("encrypted_link_to_not_secret.txt", pwd="wrong_password") as f:
                    f.read() # Should fail if "wrong_password" prevents access to the symlink or its target resolution


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_password_in_open_archive(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path, pwd="pwd") as archive:
        for member, stream in archive.iter_members_with_io():
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["encryption_with_symlinks"],
    ),
    ids=lambda a: a.filename,
)
def test_iterator_encryption_with_symlinks_password_in_iterator(
    sample_archive: SampleArchive, sample_archive_path: str
):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    members_by_name = {}
    with open_archive(sample_archive_path) as archive:
        for member, stream in archive.iter_members_with_io(pwd="pwd"):
            members_by_name[member.filename] = stream

    assert set(members_by_name.keys()) == {
        f.name for f in sample_archive.contents.files
    }
