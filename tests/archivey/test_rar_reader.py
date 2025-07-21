import pytest
from unittest.mock import Mock, patch
from archivey.formats.rar_reader import (
    RarReader,
    get_non_corrupted_filename,
    is_rar_info_hardlink,
    get_encryption_info,
    PasswordCheckResult,
    verify_rar5_password,
    _rar_hash_key,
    convert_crc_to_encrypted,
    check_rarinfo_crc,
    RarStreamMemberFile,
    RarStreamReader,
    RAR_ENCDATA_FLAG_HAS_PASSWORD_CHECK_DATA,
    RAR_ENCDATA_FLAG_TWEAKED_CHECKSUMS
)
from archivey.types import ArchiveMember, MemberType
from rarfile import RarInfo, Rar3Info, Rar5Info
import rarfile

# Mocking rarfile.RarInfo for cleaner tests
class MockRarInfo:
    def __init__(self, filename, file_size=0, compress_size=0, mtime=None, type=MemberType.FILE, mode=0o644, crc32=0, compression_method=None, comment=None, encrypted=False, host_os=3, link_target=None):
        self.filename = filename
        self.file_size = file_size
        self.compress_size = compress_size
        self.mtime = mtime
        self.date_time = None
        self._type = type
        self.mode = mode
        self.CRC = crc32
        self.compress_type = compression_method
        self.comment = comment
        self._encrypted = encrypted
        self.host_os = host_os
        self._link_target = link_target
        self.orig_filename = filename.encode('utf-8', 'surrogatepass') if isinstance(filename, str) else filename
        self.flags = 0
        self.file_encryption = None

    def is_dir(self):
        return self._type == MemberType.DIR

    def is_file(self):
        return self._type == MemberType.FILE

    def is_symlink(self):
        return self._type == MemberType.SYMLINK

    def needs_password(self):
        return self._encrypted

    @property
    def file_redir(self):
        if self._type == MemberType.HARDLINK:
            return (rarfile.RAR5_XREDIR_HARD_LINK, 0, self._link_target)
        return None


class MockRar3Info(MockRarInfo, Rar3Info):
    pass

class MockRar5Info(MockRarInfo, Rar5Info):
    pass


def test_get_non_corrupted_filename():
    # Test with a regular filename
    rar_info = MockRar3Info(filename="test.txt")
    assert get_non_corrupted_filename(rar_info) == "test.txt"

    # Test with a corrupted filename
    rar_info_corrupted = MockRar3Info(filename="test\udce4.txt")
    rar_info_corrupted.orig_filename = 'test😄.txt'.encode('utf-8')
    rar_info_corrupted.flags = rarfile.RAR_FILE_UNICODE
    assert get_non_corrupted_filename(rar_info_corrupted) == "test😄.txt"

def test_is_rar_info_hardlink():
    # Test with a non-hardlink
    rar_info = MockRar5Info(filename="test.txt")
    assert not is_rar_info_hardlink(rar_info)

    # Test with a hardlink
    rar_info_hardlink = MockRar5Info(filename="hardlink", type=MemberType.HARDLINK, link_target="target")
    assert is_rar_info_hardlink(rar_info_hardlink)

def test_get_encryption_info():
    # Test with no encryption
    rar_info = MockRar5Info(filename="test.txt")
    assert get_encryption_info(rar_info) is None

    # Test with encryption
    rar_info_encrypted = MockRar5Info(filename="encrypted.txt", encrypted=True)
    rar_info_encrypted.file_encryption = ("algo", "flags", "kdf_count", "salt", "iv", "check_value")
    assert get_encryption_info(rar_info_encrypted) is not None

def test_verify_rar5_password():
    # Test with a correct password
    rar_info = MockRar5Info(filename="encrypted.txt", encrypted=True)
    rar_info.file_encryption = (0, RAR_ENCDATA_FLAG_HAS_PASSWORD_CHECK_DATA, 10, b'salt'*4, b'iv'*4, b'\x00'*12)
    with patch('archivey.formats.rar_reader._verify_rar5_password_internal', return_value=PasswordCheckResult.CORRECT) as mock_verify:
        assert verify_rar5_password(b"password", rar_info) == PasswordCheckResult.CORRECT
        mock_verify.assert_called_once_with(b"password", b'salt'*4, 10, b'\x00'*12)

    # Test with an incorrect password
    with patch('archivey.formats.rar_reader._verify_rar5_password_internal', return_value=PasswordCheckResult.INCORRECT) as mock_verify:
        assert verify_rar5_password(b"wrong_password", rar_info) == PasswordCheckResult.INCORRECT
        mock_verify.assert_called_once_with(b"wrong_password", b'salt'*4, 10, b'\x00'*12)

    # Test with no password
    assert verify_rar5_password(None, rar_info) == PasswordCheckResult.INCORRECT

    # Test with no password needed
    rar_info_no_pwd = MockRar5Info(filename="test.txt", encrypted=False)
    assert verify_rar5_password(b"password", rar_info_no_pwd) == PasswordCheckResult.CORRECT

def test_check_rarinfo_crc():
    # Test with a correct CRC
    rar_info = MockRar5Info(filename="test.txt", crc32=12345)
    assert check_rarinfo_crc(rar_info, None, 12345) is True

    # Test with an incorrect CRC
    assert check_rarinfo_crc(rar_info, None, 54321) is False

    # Test with encrypted CRC
    rar_info_encrypted = MockRar5Info(filename="encrypted.txt", encrypted=True, crc32=12345)
    rar_info_encrypted.file_encryption = (0, RAR_ENCDATA_FLAG_TWEAKED_CHECKSUMS, 10, b'salt'*4, b'iv'*4, b'\x00'*12)
    with patch('archivey.formats.rar_reader.convert_crc_to_encrypted', return_value=12345) as mock_convert:
        assert check_rarinfo_crc(rar_info_encrypted, b"password", 54321) is True
        mock_convert.assert_called_once_with(54321, b"password", b'salt'*4, 10)
