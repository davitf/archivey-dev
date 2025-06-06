import pytest

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.dependency_checker import get_dependency_versions
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEOFError,
)
from archivey.types import ArchiveFormat, TAR_COMPRESSED_FORMATS
from tests.archivey.sample_archives import (
    SAMPLE_ARCHIVES,
    ArchiveInfo,
    filter_archives,
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
def test_read_corrupted_archives(
    sample_archive: ArchiveInfo,
    corrupted_archive_path: str,
    # corruption_type: str,
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
    # archive_path = pathlib.Path(corrupted_archive_path)
    # if sample_archive.creation_info.format == ArchiveFormat.RAR:
    #     pytest.xfail("RAR library handles corrupted archives without error")
    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        if get_dependency_versions().unrar_version is None:
            pytest.skip("unrar not installed, skipping RAR corruption test")
    if sample_archive.creation_info.format in {ArchiveFormat.LZ4, ArchiveFormat.TAR_LZ4}:
        pytest.importorskip("lz4")
    if sample_archive.creation_info.format in {ArchiveFormat.ZSTD, ArchiveFormat.TAR_ZSTD}:
        pytest.importorskip("zstandard")
    if (
        sample_archive.creation_info.format in TAR_COMPRESSED_FORMATS
        and "truncate" not in corrupted_archive_path
    ):
        pytest.xfail("Tar archives have no integrity checks for modified data")
    if (
        sample_archive.creation_info.format in {ArchiveFormat.LZ4, ArchiveFormat.TAR_LZ4}
        and "truncate" not in corrupted_archive_path
    ):
        pytest.xfail("LZ4 library may not detect modified data")

    read_filenames = []
    with pytest.raises((ArchiveCorruptedError, ArchiveEOFError)):
        # For many corrupted archives, error might be raised on open or during iteration
        with open_archive(corrupted_archive_path) as archive:
            for member, stream in archive.iter_members_with_io():
                read_filenames.append(member.filename)
                if stream is not None:
                    stream.read()


@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(
        SAMPLE_ARCHIVES,
        prefixes=["large_files_nonsolid", "large_files_solid", "large_single_file"],
        extensions=[".gz", ".bz2", ".xz"],
    ),
    ids=lambda a: a.filename,
)
def test_read_corrupted_archives_with_alternative_packages(
    sample_archive: ArchiveInfo,
    corrupted_archive_path: str,
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
    config = ArchiveyConfig(
        use_rapidgzip=True,
        use_indexed_bzip2=True,
        use_python_xz=True,
    )
    deps = get_dependency_versions()
    if config.use_rapidgzip and deps.rapidgzip_version is None:
        pytest.skip("rapidgzip not installed, skipping alternative package test")
    if config.use_indexed_bzip2 and deps.indexed_bzip2_version is None:
        pytest.skip(
            "indexed_bzip2 not installed, skipping alternative package test"
        )
    if config.use_python_xz and deps.python_xz_version is None:
        pytest.skip("python-xz not installed, skipping alternative package test")
    if sample_archive.creation_info.format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    if sample_archive.creation_info.format == ArchiveFormat.RAR and get_dependency_versions().unrar_version is None:
        pytest.skip("unrar not installed, skipping RAR corruption test")
    if (
        sample_archive.creation_info.format in TAR_COMPRESSED_FORMATS
        and "truncate" not in corrupted_archive_path
    ):
        pytest.xfail("Tar archives have no integrity checks for modified data")
    if (
        sample_archive.creation_info.format in {ArchiveFormat.LZ4, ArchiveFormat.TAR_LZ4}
        and "truncate" not in corrupted_archive_path
    ):
        pytest.xfail("LZ4 library may not detect modified data")

    read_filenames = []
    with pytest.raises(ArchiveCorruptedError):
        # For many corrupted archives, error might be raised on open or during iteration
        with open_archive(corrupted_archive_path, config=config) as archive:
            for member, stream in archive.iter_members_with_io():
                read_filenames.append(member.filename)
                if stream is not None:
                    stream.read()
