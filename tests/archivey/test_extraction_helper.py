import pytest
import os
from unittest.mock import Mock, patch
from archivey.internal.extraction_helper import ExtractionHelper, apply_member_metadata
from archivey.config import OverwriteMode
from archivey.types import ArchiveMember, MemberType
from archivey.exceptions import ArchiveFileExistsError, ArchiveLinkTargetNotFoundError

@pytest.fixture
def extraction_helper(tmp_path):
    archive_reader = Mock()
    return ExtractionHelper(archive_reader, str(tmp_path), OverwriteMode.OVERWRITE)

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
    }
    defaults.update(kwargs)
    # HACK: directly set the private attribute to avoid the read-only error
    member = ArchiveMember(**{k: v for k, v in defaults.items() if k != "member_id"})
    member._member_id = kwargs.get("member_id", 0)
    return member


def test_apply_member_metadata(tmp_path):
    file_path = tmp_path / "test_file"
    file_path.touch()
    member = create_member(
        filename="test_file",
        mtime_with_tz=datetime.fromtimestamp(1678886400),
        mode=0o755
    )
    apply_member_metadata(member, str(file_path))
    stat_result = os.stat(file_path)
    assert stat_result.st_mtime == 1678886400
    assert stat_result.st_mode & 0o777 == 0o755

def test_check_overwrites_skip(extraction_helper, tmp_path):
    file_path = tmp_path / "test_file"
    file_path.touch()
    member = create_member(filename="test_file")
    extraction_helper.overwrite_mode = OverwriteMode.SKIP
    assert extraction_helper.check_overwrites(member, str(file_path)) is False
    assert member in extraction_helper.failed_extractions

def test_check_overwrites_error(extraction_helper, tmp_path):
    file_path = tmp_path / "test_file"
    file_path.touch()
    member = create_member(filename="test_file")
    extraction_helper.overwrite_mode = OverwriteMode.ERROR
    with pytest.raises(ArchiveFileExistsError):
        extraction_helper.check_overwrites(member, str(file_path))
    assert member in extraction_helper.failed_extractions

def test_check_overwrites_overwrite(extraction_helper, tmp_path):
    file_path = tmp_path / "test_file"
    file_path.touch()
    member = create_member(filename="test_file")
    assert extraction_helper.check_overwrites(member, str(file_path)) is True
    assert not file_path.exists()

def test_create_directory(extraction_helper, tmp_path):
    dir_path = tmp_path / "test_dir"
    member = create_member(filename="test_dir", type=MemberType.DIR)
    assert extraction_helper.create_directory(member, str(dir_path)) is True
    assert dir_path.is_dir()

def test_create_regular_file(extraction_helper, tmp_path):
    file_path = tmp_path / "test_file"
    member = create_member(filename="test_file", file_size=4)
    stream = Mock()
    stream.read.return_value = b"test"
    with patch('shutil.copyfileobj') as mock_copy:
        assert extraction_helper.create_regular_file(member, stream, str(file_path)) is True
        mock_copy.assert_called_once()
    assert file_path.parent.is_dir()

def test_create_link_symlink(extraction_helper, tmp_path):
    link_path = tmp_path / "link"
    target_path = tmp_path / "target"
    target_path.touch()
    member = create_member(filename="link", type=MemberType.SYMLINK, link_target="target")
    extraction_helper.archive_reader.resolve_link.return_value = create_member(filename="target")
    assert extraction_helper.create_link(member, str(link_path)) is True
    assert link_path.is_symlink()
    assert os.readlink(str(link_path)) == "target"

def test_create_link_hardlink(extraction_helper, tmp_path):
    link_path = tmp_path / "link"
    target_path = tmp_path / "target"
    target_path.touch()
    target_member = create_member(filename="target", member_id=1)
    member = create_member(filename="link", type=MemberType.HARDLINK, link_target="target")
    extraction_helper.archive_reader.resolve_link.return_value = target_member
    extraction_helper.extracted_path_by_source_id[1] = str(target_path)
    assert extraction_helper.create_link(member, str(link_path)) is True
    assert os.path.samefile(str(link_path), str(target_path))

def test_create_link_hardlink_target_not_found(extraction_helper):
    link_path = "/some/path/link"
    member = create_member(filename="link", type=MemberType.HARDLINK, link_target="target")
    extraction_helper.archive_reader.resolve_link.return_value = None
    with pytest.raises(ArchiveLinkTargetNotFoundError):
        extraction_helper.create_link(member, link_path)

def test_extract_member_other_type(extraction_helper):
    member = create_member(filename="other", type=MemberType.OTHER)
    assert extraction_helper.extract_member(member, None) is False
    assert member in extraction_helper.failed_extractions
