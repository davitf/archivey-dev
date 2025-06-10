import logging
import pathlib

import pytest

from archivey.core import open_archive
from archivey.types import TAR_COMPRESSED_FORMATS, ArchiveFormat, MemberType
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing


def _first_regular_file(sample: SampleArchive):
    for f in sample.contents.files:
        if f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no regular file")


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
def test_random_access_mode(sample_archive: SampleArchive, sample_archive_path: str):
    if (
        sample_archive.creation_info.format == ArchiveFormat.ISO
        and not pathlib.Path(sample_archive_path).exists()
    ):
        pytest.skip("ISO archive not available")
    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        assert archive.has_random_access()
        members_if_available = archive.get_members_if_available()
        members = archive.get_members()

        assert members_if_available == members

        # Open a file as a non-context manager
        f = archive.open("large3.txt")
        logger.warning("opened large3.txt")
        data = f.read(100)
        assert data.startswith(b"Large file #3\n")
        logger.warning(f"closing large3.txt, closed = {f.closed}")
        f.close()
        logger.warning(f"closed large3.txt, closed = {f.closed}")

        logger.warning("will open large3.txt again")

        with archive.open("large3.txt") as f:
            data = f.read(100)
            assert len(data) == 100
            data += f.read()  # Read the rest of the file

            assert data.startswith(b"Large file #3\n")
            member = archive.get_member("large3.txt")
            assert archive.get_member(member) is member
            assert data == sample_archive.contents.files[2].contents

        # Read multiple files at once
        sorted_members = sorted(members, key=lambda m: m.filename)
        files = [archive.open(m) for m in sorted_members]

        first_line = [f.readline() for f in files]
        rest_of_files = [f.read() for f in files[::-1]]
        rest_of_files.reverse()
        for i in range(len(files)):
            assert first_line[i] == f"Large file #{i + 1}\n".encode()
            assert (
                first_line[i] + rest_of_files[i]
                == sample_archive.contents.files[i].contents
            )

        for f in files:
            f.close()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
def test_streaming_only_mode(sample_archive: SampleArchive, sample_archive_path: str):
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


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_iter_members_filter(
    sample_archive: SampleArchive, sample_archive_path: str, streaming_only: bool
):
    """Ensure iter_members_with_io honours the filter callable."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    target = next(f for f in sample_archive.contents.files if f.type == MemberType.FILE)

    with open_archive(sample_archive_path, streaming_only=streaming_only) as archive:
        seen = []
        for member, stream in archive.iter_members_with_io(
            members=lambda m: m.filename == target.name
        ):
            seen.append(member.filename)
            if member.type == MemberType.FILE:
                assert stream is not None
                assert stream.read() == (target.contents or b"")

        assert seen == [target.name]


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_iter_members_partial_reads(
    sample_archive: SampleArchive, sample_archive_path: str, streaming_only: bool
):
    """Reading some members fully, partially or not at all should not break iteration."""
    skip_if_package_missing(sample_archive.creation_info.format, None)

    files = [f for f in sample_archive.contents.files if f.type == MemberType.FILE]
    assert len(files) == 5

    with open_archive(sample_archive_path, streaming_only=streaming_only) as archive:
        for i, (member, stream) in enumerate(
            archive.iter_members_with_io(members=lambda m: m.type == MemberType.FILE)
        ):
            if member.filename not in {f.name for f in files}:
                continue

            info = next(f for f in files if f.name == member.filename)
            assert stream is not None

            if i % 3 == 0:
                assert stream.read() == (info.contents or b"")
            elif i % 3 == 1:
                partial_len = max(1, len(info.contents or b"") // 2)
                assert stream.read(partial_len) == (info.contents or b"")[:partial_len]
            else:
                # Do not read the third file at all
                pass
