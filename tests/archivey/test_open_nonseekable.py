import io

import pytest

from archivey.api.core import open_archive
from archivey.api.types import ArchiveFormat
from tests.archivey.sample_archives import SAMPLE_ARCHIVES
from tests.archivey.testing_utils import skip_if_package_missing


# Formats known to fail when opened from a non-seekable stream
SKIPPABLE_FORMATS: set[ArchiveFormat] = {
    ArchiveFormat.ZIP,
    ArchiveFormat.RAR,
    ArchiveFormat.SEVENZIP,
}


# Select one sample archive for each format except folder/iso
archives_by_format = {}
for a in SAMPLE_ARCHIVES:
    fmt = a.creation_info.format
    if fmt in (ArchiveFormat.FOLDER, ArchiveFormat.ISO):
        continue
    archives_by_format.setdefault(fmt, a)


class NonSeekableBytesIO(io.BytesIO):
    def seekable(self) -> bool:  # pragma: no cover - simple
        return False

    def seek(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("seek")

    def tell(self, *args, **kwargs):  # pragma: no cover - simple
        raise io.UnsupportedOperation("tell")


@pytest.mark.parametrize(
    "sample_archive", list(archives_by_format.values()), ids=lambda a: a.filename
)
def test_open_from_nonseekable_memory(sample_archive):
    """Ensure open_archive can read from non-seekable streams in streaming mode."""

    skip_if_package_missing(sample_archive.creation_info.format, None)

    path = sample_archive.get_archive_path()
    with open(path, "rb") as f:
        data = f.read()

    stream = NonSeekableBytesIO(data)

    try:
        with open_archive(stream, streaming_only=True) as archive:
            has_member = False
            for member, member_stream in archive.iter_members_with_io():
                has_member = True
                if member_stream is not None:
                    member_stream.read()
            assert has_member
    except Exception as exc:  # pragma: no cover - environment dependent
        if sample_archive.creation_info.format in SKIPPABLE_FORMATS:
            pytest.xfail(
                f"Format {sample_archive.creation_info.format} unsupported: {exc}"
            )
        else:
            raise
