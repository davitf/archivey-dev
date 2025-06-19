import pytest

from archivey.core import open_archive
from archivey.exceptions import ArchiveNotSupportedError
from archivey.reader_registry import register_reader, unregister_reader
from archivey.types import ArchiveFormat
from archivey.base_reader import ArchiveReader
from archivey.types import ArchiveInfo

ISO_ARCHIVE = "tests/test_archives/basic_nonsolid__pycdlib.iso"


class DummyReader(ArchiveReader):
    def __init__(self, path: str):
        super().__init__(path, ArchiveFormat.ISO)

    def close(self) -> None:
        pass

    def get_members_if_available(self):
        return []

    def get_members(self):
        return []

    def iter_members_with_io(self, members=None, *, pwd=None, filter=None):
        return iter([])

    def get_archive_info(self) -> ArchiveInfo:
        return ArchiveInfo(format="iso")

    def has_random_access(self) -> bool:
        return True

    def get_member(self, member_or_filename):
        raise ArchiveNotSupportedError()

    def open(self, member_or_filename, *, pwd=None):
        raise ArchiveNotSupportedError()

    def extract(self, member_or_filename, path=None, pwd=None):
        raise ArchiveNotSupportedError()

    def extractall(self, path=None, members=None, *, pwd=None, filter=None):
        return {}

    def resolve_link(self, member):
        return None

    def iter_members_for_registration(self):
        return iter([])


def test_custom_reader_registration():
    with pytest.raises(ArchiveNotSupportedError):
        open_archive(ISO_ARCHIVE)

    register_reader(
        ArchiveFormat.ISO,
        lambda path, fmt, streaming_only, **kw: DummyReader(path),
    )
    try:
        with open_archive(ISO_ARCHIVE) as archive:
            assert isinstance(archive, DummyReader)
    finally:
        unregister_reader(ArchiveFormat.ISO)
