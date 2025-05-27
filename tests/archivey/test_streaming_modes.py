import logging
from typing import IO

import pytest

from archivey.core import open_archive
from archivey.types import TAR_COMPRESSED_FORMATS, ArchiveFormat, MemberType
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing


def _last_regular_file(sample: SampleArchive):
    for f in reversed(sample.contents.files):
        if f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no regular file")


def _first_regular_file(sample: SampleArchive):
    for f in sample.contents.files:
        if f.type == MemberType.FILE:
            return f
    raise ValueError("sample archive has no regular file")


logger = logging.getLogger(__name__)

#  "duplicate_files",
#                   "hardlinks_nonsolid", "hardlinks_solid",
#                   "hardlinks_with_duplicate_files",
#                   "hardlinks_recursive_and_broken",
#                   "symlinks", "symlinks_solid"


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
def test_random_access_mode(sample_archive: SampleArchive, sample_archive_path: str):
    skip_if_package_missing(sample_archive.creation_info.format, None)

    with open_archive(sample_archive_path) as archive:
        assert archive.has_random_access()
        members_if_available = archive.get_members_if_available()
        members = archive.get_members()

        assert members_if_available == members

        for sample_file in reversed(sample_archive.contents.files):
            # Open file without context manager
            f = archive.open(sample_file.name)
            data = f.read()
            assert sample_file.contents == data, f"{sample_file.name} contents mismatch"
            f.close()

        for sample_file in reversed(sample_archive.contents.files):
            # Open file with context manager
            with archive.open(sample_file.name) as f:
                data = f.read(100)
                assert len(data) == min(100, len(sample_file.contents or b""))
                data += (
                    f.read()
                )  # Read the rest of the file, which should be the whole file
                assert sample_file.contents == data, (
                    f"{sample_file.name} contents mismatch"
                )

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

    # After the archive context manager is closed, all streams should have been closed.
    # TODO: this is not actually true. Should this be implemented? Or should we test
    # that trying to read() from these streams raises an error?
    # for f in files:
    #     assert f.closed


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid"],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("close_streams", [False, True], ids=["noclose", "close"])
def test_streaming_only_mode(
    sample_archive: SampleArchive, sample_archive_path: str, close_streams: bool
):
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

        previous_stream: IO[bytes] | None = None
        for m, stream in archive.iter_members_with_io():
            if previous_stream is not None:
                assert previous_stream.closed
                with pytest.raises(ValueError):
                    data = previous_stream.read()
                    logger.info(
                        f"previous_stream.read() = {data[:20]} -- {previous_stream=}"
                    )

            previous_stream = stream

            assert (stream is None) == (m.type != MemberType.FILE)
            if close_streams and stream is not None:
                stream.close()  # Should be a no-op, not raise anything


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


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=[
            "basic_nonsolid",
            "basic_solid",
            "duplicate_files",
        ],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("streaming_only", [False, True], ids=["random", "stream"])
def test_iter_members_list_filter(
    sample_archive: SampleArchive, sample_archive_path: str, streaming_only: bool
):
    """Ensure iter_members_with_io honours the filter callable."""
    skip_if_package_missing(sample_archive.creation_info.format, None)
    if (
        sample_archive.filename.startswith("duplicate_files")
        and not sample_archive.creation_info.features.duplicate_files
    ):
        pytest.skip("Duplicate files feature is not enabled for this archive")

    file_names = {f.name for f in sample_archive.contents.files[::2]}
    file_contents = [
        (f.name, f.contents)
        for f in sample_archive.contents.files
        if f.name in file_names
    ]
    read_contents = []

    with open_archive(sample_archive_path, streaming_only=streaming_only) as archive:
        for member, stream in archive.iter_members_with_io(members=file_names):
            assert member.filename in file_names
            read_contents.append(
                (member.filename, stream.read() if stream is not None else None)
            )

    assert sorted(file_contents) == sorted(read_contents), file_names
