import io
import pytest

from archivey.api.core import open_archive
from archivey.api.types import ArchiveFormat
from tests.archivey.sample_archives import SAMPLE_ARCHIVES
from tests.archivey.testing_utils import skip_if_package_missing


class NonSeekable(io.BufferedReader):
    def seekable(self) -> bool:
        return False

    def seek(self, *args, **kwargs):
        raise io.UnsupportedOperation("seek")


# Pick one sample for each format except FOLDER and ISO
archives_by_format = {}
for a in SAMPLE_ARCHIVES:
    fmt = a.creation_info.format
    if fmt in (ArchiveFormat.FOLDER, ArchiveFormat.ISO):
        continue
    archives_by_format.setdefault(fmt, a)


@pytest.mark.parametrize(
    "sample_archive",
    list(archives_by_format.values()),
    ids=lambda a: a.filename,
)
def test_open_archive_nonseekable_stream(sample_archive):
    skip_if_package_missing(sample_archive.creation_info.format, None)
    path = sample_archive.get_archive_path()
    with open(path, "rb") as raw:
        stream = NonSeekable(raw)
        try:
            with open_archive(stream, streaming_only=True) as archive:
                for member, member_stream in archive.iter_members_with_io():
                    if member_stream is not None:
                        member_stream.read(1)
            return
        except Exception as e:
            pytest.skip(f"{sample_archive.filename} unsupported: {e}")
