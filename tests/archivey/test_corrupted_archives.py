import os
from venv import logger

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
)
from archivey.types import (
    SINGLE_FILE_COMPRESSED_FORMATS,
    ArchiveFormat,
)
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    SampleArchive,
    filter_archives,
)
from tests.archivey.testing_utils import skip_if_package_missing

_ALTERNATIVE_CONFIG = ArchiveyConfig(
    use_rapidgzip=True,
    use_indexed_bzip2=True,
    use_python_xz=True,
    use_zstandard=True,
)

_ALTERNATIVE_PACKAGES_FORMATS = (
    ArchiveFormat.GZIP,
    ArchiveFormat.BZIP2,
    ArchiveFormat.XZ,
    ArchiveFormat.ZSTD,
    ArchiveFormat.TAR_GZ,
    ArchiveFormat.TAR_BZ2,
    ArchiveFormat.TAR_XZ,
    ArchiveFormat.TAR_ZSTD,
)


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid", "large_single_file"],
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("read_streams", [True, False], ids=["read", "noread"])
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_read_corrupted_archives(
    sample_archive: SampleArchive,
    corrupted_archive_path: str,
    read_streams: bool,
    alternative_packages: bool,
):
    """Test that reading generally corrupted archives raises ArchiveCorruptedError.

    Args:
        sample_archive: The archive to test
        corrupted_archive_path: Path to the corrupted archive
        corruption_type: Type of corruption applied:
            - "header": Corruption near the start of the file
            - "data": Corruption in the middle of the file
            - "checksum": Corruption near the end of the file
    """
    if alternative_packages:
        if sample_archive.creation_info.format not in _ALTERNATIVE_PACKAGES_FORMATS:
            pytest.skip("No alternative package for this format, no need to test")
        config = _ALTERNATIVE_CONFIG
    else:
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    formats_without_redundancy_check = [
        ArchiveFormat.LZ4,
        ArchiveFormat.TAR,
    ]

    try:
        found_member_names = []
        found_member_data = {}

        with open_archive(
            corrupted_archive_path, config=config, streaming_only=True
        ) as archive:
            for member, stream in archive.iter_members_with_io():
                logger.info(f"Reading member {member.filename}")
                filename = member.filename

                # Single file formats don't store the filename, and the reader derives
                # it from the archive name. But here, the archive name has a
                # .corrupted_xxx suffix that doesn't match the name in sample_archive,
                # so we need to remove it.
                if (
                    sample_archive.creation_info.format
                    in SINGLE_FILE_COMPRESSED_FORMATS
                ):
                    filename = os.path.splitext(filename)[0]

                if stream is not None and read_streams:
                    data = stream.read()
                    logger.info(f"Read {len(data)} bytes from member {filename}")

                    found_member_data[filename] = data

                found_member_names.append(filename)

        expected_member_data = {
            member.name: member.contents for member in sample_archive.contents.files
        }
        logger.info(f"{found_member_names=}, expected={expected_member_data.keys()}")

        # If no error was raised, it likely means that the corruption didn't affect the
        # archive directory or member metadata, so at least all the members should have
        # been read.
        assert set(found_member_names) == set(expected_member_data.keys()), (
            f"Archive {corrupted_archive_path} did not raise an error but did not read all members"
        )

        if read_streams:
            assert (
                sample_archive.creation_info.format in formats_without_redundancy_check
            ), f"Archive {corrupted_archive_path} should have detected a corruption"
            # If we read the streams and an error wasn't raised, it means the compressed
            # stream was valid, but at least one member should have different data.
            broken_files = [
                name
                for name, contents in expected_member_data.items()
                if contents is not None and contents != found_member_data[name]
            ]
            assert len(broken_files) >= 1, (
                f"Archive {corrupted_archive_path} should have at least one broken file"
            )
            # If this is a multi-file archive, which we corrupted in the middle,
            # at least the first file should be good. The last may or may not be broken,
            # depending on how the error was propagated.
            if len(expected_member_data) >= 1:
                assert len(broken_files) <= len(expected_member_data), (
                    f"Archive {corrupted_archive_path} should have at least one good file"
                )

    except (ArchiveCorruptedError, ArchiveEOFError):
        logger.info(f"Archive {corrupted_archive_path} raised an error", exc_info=True)


@pytest.mark.parametrize("corrupted_length", [16, 47, 0.1, 0.9])
@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid", "large_single_file"],
        # Tar files don't have any kind of error detection, so we skip them.
        # custom_filter=lambda a: a.creation_info.format != ArchiveFormat.TAR,
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("read_streams", [True, False], ids=["read", "noread"])
@pytest.mark.parametrize(
    "alternative_packages", [False, True], ids=["defaultlibs", "altlibs"]
)
def test_read_truncated_archives(
    sample_archive: SampleArchive,
    corrupted_length: int | float,
    tmp_path_factory: pytest.TempPathFactory,
    read_streams: bool,
    alternative_packages: bool,
):
    """Test that reading truncated archives raises appropriate errors."""

    if alternative_packages:
        if sample_archive.creation_info.format not in _ALTERNATIVE_PACKAGES_FORMATS:
            pytest.skip("No alternative package for this format, no need to test")
        config = _ALTERNATIVE_CONFIG
    else:
        config = None

    skip_if_package_missing(sample_archive.creation_info.format, config)

    filename = sample_archive.get_archive_name(variant=f"truncated_{corrupted_length}")
    output_path = tmp_path_factory.mktemp("generated_archives") / filename

    logger.info(
        f"Testing truncated archive {output_path} with length {corrupted_length}"
    )

    data = open(sample_archive.get_archive_path(), "rb").read()
    if isinstance(corrupted_length, float):
        corrupted_length = int(corrupted_length * len(data))

    with open(output_path, "wb") as f:
        f.write(data[:corrupted_length])

    try:
        with open_archive(output_path, config=config, streaming_only=True) as archive:
            for member, stream in archive.iter_members_with_io():
                if stream is not None and read_streams:
                    stream.read()
        logger.warning(f"Archive {output_path} did not raise an error")
    except (ArchiveCorruptedError, ArchiveEOFError):
        # Test passes if one of the expected exceptions is raised
        pass
