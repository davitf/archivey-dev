import os

from archivey.core import open_archive
from archivey.folder_reader import FolderReader
from archivey.registry import register_reader, get_reader_factory
from archivey.types import ArchiveFormat

class CustomFolderReader(FolderReader):
    pass


def test_custom_reader_registration(tmp_path):
    archive_dir = tmp_path / "data"
    archive_dir.mkdir()
    (archive_dir / "file.txt").write_text("content")

    original = get_reader_factory(ArchiveFormat.FOLDER)
    register_reader(
        ArchiveFormat.FOLDER,
        lambda path, **kwargs: CustomFolderReader(path),
    )

    try:
        with open_archive(archive_dir) as r:
            assert isinstance(r, CustomFolderReader)
    finally:
        if original is not None:
            register_reader(ArchiveFormat.FOLDER, original)

