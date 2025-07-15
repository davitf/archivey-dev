import pytest
from unittest.mock import patch, MagicMock
from archivey.internal.cli import main, build_arg_parser, format_mode, get_member_checksums, build_pattern_filter
from archivey.types import MemberType, ArchiveMember
import io
from datetime import datetime

def create_member(**kwargs):
    defaults = {
        "filename": "test",
        "file_size": 0,
        "compress_size": 0,
        "mtime_with_tz": datetime.fromtimestamp(1678886400),
        "type": MemberType.FILE,
        "mode": 0o644,
        "link_target": None,
        "crc32": 0,
        "comment": None,
        "encrypted": False,
    }
    defaults.update(kwargs)
    return ArchiveMember(**defaults)

def test_format_mode():
    assert format_mode(MemberType.FILE, 0o644) == "-rw-r--r--"
    assert format_mode(MemberType.DIR, 0o755) == "drwxr-xr-x"
    assert format_mode(MemberType.SYMLINK, 0o777) == "lrwxrwxrwx"
    assert format_mode(MemberType.HARDLINK, 0o600) == "hrw-------"
    assert format_mode(MemberType.OTHER, 0o600) == "-rw-------"


def test_get_member_checksums():
    data = b"test data"
    crc32, sha256 = get_member_checksums(io.BytesIO(data))
    assert crc32 == 3540561586
    assert sha256 == "916f0027a575074ce72a331777c3478d6513f786a591bd892da1a577bf2335f9"

def test_build_pattern_filter():
    member1 = create_member(filename="test.txt")
    member2 = create_member(filename="image.jpg")

    # Test with a single pattern
    filter_fn = build_pattern_filter(["*.txt"])
    assert filter_fn(member1) is True
    assert filter_fn(member2) is False

    # Test with multiple patterns
    filter_fn = build_pattern_filter(["*.txt", "*.jpg"])
    assert filter_fn(member1) is True
    assert filter_fn(member2) is True

    # Test with no patterns
    filter_fn = build_pattern_filter([])
    assert filter_fn is None

def test_main_list(capsys):
    with patch('archivey.internal.cli.open_archive') as mock_open_archive:
        mock_archive = MagicMock()
        mock_archive.get_members.return_value = [
            create_member(filename="test.txt", file_size=4, compress_size=4, crc32=12345)
        ]
        mock_archive.format = "zip"
        mock_archive.get_archive_info.return_value = "info"
        mock_open_archive.return_value.__enter__.return_value = mock_archive

        main(["-l", "dummy.zip"])

        captured = capsys.readouterr()
        assert "test.txt" in captured.out
        assert "00003039" in captured.out

def test_main_extract(capsys):
    with patch('archivey.internal.cli.open_archive') as mock_open_archive:
        mock_archive = MagicMock()
        mock_archive.format = "zip"
        mock_archive.get_archive_info.return_value = "info"
        mock_open_archive.return_value.__enter__.return_value = mock_archive

        main(["-x", "--dest", "/tmp", "dummy.zip"])

        mock_archive.extractall.assert_called_once_with(path="/tmp", members=None)

from archivey.internal.dependency_checker import DependencyVersions

def test_main_version(capsys):
    with patch('archivey.internal.cli.package_version', return_value="1.2.3"):
        with patch('archivey.internal.cli.get_dependency_versions', return_value=DependencyVersions(python_version="3.10")) as mock_get_versions:
            main(["--version", "dummy.zip"])
            captured = capsys.readouterr()
            assert "archivey 1.2.3" in captured.out
            assert "python_version" in captured.out
