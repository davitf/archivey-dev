from venv import logger

import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
)
from archivey.types import (
    ArchiveFormat,
)
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    ArchiveInfo,
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
        # Tar files don't have any kind of error detection, so we skip them.
        custom_filter=lambda a: a.creation_info.format != ArchiveFormat.TAR,
    ),
    ids=lambda a: a.filename,
)
@pytest.mark.parametrize("read_streams", [True, False])
@pytest.mark.parametrize("alternative_packages", [False, True])
def test_read_corrupted_archives(
    sample_archive: ArchiveInfo,
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

    expect_no_error = not read_streams and sample_archive.creation_info.format in (
        ArchiveFormat.ZIP,
        ArchiveFormat.RAR,
        ArchiveFormat.SEVENZIP,
    )

    try:
        with open_archive(corrupted_archive_path, config=config) as archive:
            for member, stream in archive.iter_members_with_io():
                logger.info(f"Reading member {member.filename}")
                if stream is not None and read_streams:
                    data = stream.read()
                    logger.info(f"Read {len(data)} bytes from member {member.filename}")

        assert expect_no_error, (
            f"Archive {corrupted_archive_path} did not raise an error"
        )

    except (ArchiveCorruptedError, ArchiveEOFError):
        logger.info(f"Archive {corrupted_archive_path} raised an error", exc_info=True)


@pytest.mark.parametrize("corrupted_length", [16, 31, 47, 100, 0.1, 0.5, 0.9])
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
@pytest.mark.parametrize("read_streams", [True, False])
@pytest.mark.parametrize("alternative_packages", [False, True])
def test_read_truncated_archives(
    sample_archive: ArchiveInfo,
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
        with open_archive(output_path, config=config) as archive:
            for member, stream in archive.iter_members_with_io():
                if stream is not None and read_streams:
                    stream.read()
        logger.warning(f"Archive {output_path} did not raise an error")
    except (ArchiveCorruptedError, ArchiveEOFError):
        # Test passes if one of the expected exceptions is raised
        pass
