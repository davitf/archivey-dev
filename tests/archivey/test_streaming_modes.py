import pathlib

import pytest

from archivey.core import open_archive
from archivey.types import TAR_COMPRESSED_FORMATS, ArchiveFormat, MemberType
from tests.archivey.sample_archives import SAMPLE_ARCHIVES, ArchiveInfo, filter_archives
from tests.archivey.testing_utils import skip_if_package_missing

REPRESENTATIVE_ARCHIVES = [
    filter_archives(SAMPLE_ARCHIVES, extensions=["zip"])[0],
    filter_archives(SAMPLE_ARCHIVES, extensions=["tar.gz"])[0],
    filter_archives(SAMPLE_ARCHIVES, prefixes=["large_single_file"], extensions=["gz"])[
        0
    ],
    filter_archives(SAMPLE_ARCHIVES, extensions=["rar"])[0],
]

# Use archives with several files for iterator behavior tests.
MULTIFILE_ARCHIVES = [
    filter_archives(SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["zip"])[
        0
    ],
    filter_archives(SAMPLE_ARCHIVES, prefixes=["basic_solid"], extensions=["tar.gz"])[
        0
    ],
    filter_archives(SAMPLE_ARCHIVES, prefixes=["basic_nonsolid"], extensions=["rar"])[
        0
    ],
]


def _first_regular_file(sample: ArchiveInfo):
    for f in sample.contents.files:
        if f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no regular file")


@pytest.mark.parametrize(
    "sample_archive", REPRESENTATIVE_ARCHIVES, ids=lambda a: a.filename
)
def test_random_access_mode(sample_archive: ArchiveInfo, sample_archive_path: str):
    if (
        sample_archive.creation_info.format == ArchiveFormat.ISO
        and not pathlib.Path(sample_archive_path).exists()
    ):
        pytest.skip("ISO archive not available")
    skip_if_package_missing(sample_archive.creation_info.format, None)

    first_file = _first_regular_file(sample_archive)
    with open_archive(sample_archive_path) as archive:
        assert archive.has_random_access()
        members = archive.get_members()
        assert any(m.filename == first_file.name for m in members)
        member = next(m for m in members if m.filename == first_file.name)
        with archive.open(member) as fh:
            assert fh.read() == (first_file.contents or b"")
        info = archive.get_members_if_available()
        assert info is not None and len(info) == len(members)


@pytest.mark.parametrize(
    "sample_archive", REPRESENTATIVE_ARCHIVES, ids=lambda a: a.filename
)
def test_streaming_only_mode(sample_archive: ArchiveInfo, sample_archive_path: str):
    if (
        sample_archive.creation_info.format == ArchiveFormat.ISO
        and not pathlib.Path(sample_archive_path).exists()
    ):
        pytest.skip("ISO archive not available")
    skip_if_package_missing(sample_archive.creation_info.format, None)

    first_file = _first_regular_file(sample_archive)
    with open_archive(sample_archive_path, streaming_only=True) as archive:
        assert not archive.has_random_access()
        with pytest.raises(ValueError):
            archive.get_members()
        with pytest.raises(ValueError):
            archive.open(first_file.name)

        info = archive.get_members_if_available()
        if (
            sample_archive.creation_info.format == ArchiveFormat.TAR
            or sample_archive.creation_info.format in TAR_COMPRESSED_FORMATS
        ):
            assert info is None
        else:
            assert info is not None and len(info) >= 1

        found = False
        for m, stream in archive.iter_members_with_io():
            if m.filename == first_file.name:
                assert stream is not None
                assert stream.read() == (first_file.contents or b"")
                found = True
                break
        assert found


@pytest.mark.parametrize("sample_archive", MULTIFILE_ARCHIVES, ids=lambda a: a.filename)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_iter_members_filter(
    sample_archive: ArchiveInfo, sample_archive_path: str, streaming_only: bool
):
    """Ensure iter_members_with_io honours the filter callable."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    target = next(f for f in sample_archive.contents.files if f.type == MemberType.FILE)

    with open_archive(sample_archive_path, streaming_only=streaming_only) as archive:
        seen = []
        for member, stream in archive.iter_members_with_io(
            filter=lambda m: m.filename == target.name
        ):
            seen.append(member.filename)
            if member.type == MemberType.FILE:
                assert stream is not None
                assert stream.read() == (target.contents or b"")

        assert seen == [target.name]


@pytest.mark.parametrize("sample_archive", MULTIFILE_ARCHIVES, ids=lambda a: a.filename)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_iter_members_partial_reads(
    sample_archive: ArchiveInfo, sample_archive_path: str, streaming_only: bool
):
    """Reading some members fully, partially or not at all should not break iteration."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    files = [f for f in sample_archive.contents.files if f.type == MemberType.FILE]
    assert len(files) >= 3

    with open_archive(sample_archive_path, streaming_only=streaming_only) as archive:
        read_index = 0
        for member, stream in archive.iter_members_with_io(
            filter=lambda m: m.type == MemberType.FILE
        ):
            if member.filename not in {f.name for f in files}:
                continue

            info = next(f for f in files if f.name == member.filename)
            assert stream is not None

            if read_index == 0:
                assert stream.read() == (info.contents or b"")
            elif read_index == 1:
                partial_len = max(1, len(info.contents or b"") // 2)
                assert stream.read(partial_len) == (info.contents or b"")[:partial_len]
            else:
                # Do not read the third file at all
                pass

            read_index += 1
            if read_index >= 3:
                break

        assert read_index == 3
