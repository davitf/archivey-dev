import io

from archivey.core import open_archive
from archivey.types import ArchiveFormat


def test_open_zip_from_stream():
    path = "tests/test_archives/basic_nonsolid__zipfile_deflate.zip"
    with open(path, "rb") as f:
        data = f.read()
    stream = io.BytesIO(data)
    with open_archive(stream) as archive:
        assert archive.format == ArchiveFormat.ZIP
        members = archive.get_members()
        assert members[0].filename == "file1.txt"
        with archive.open(members[0]) as mf:
            assert mf.read().startswith(b"Hello")
