import os
import pathlib

import pytest

from archivey.exceptions import PackageNotInstalledError
from tests.archivey.sample_archives import ArchiveInfo
from tests.create_archives import create_archive
from tests.create_corrupted_archives import corrupt_archive, truncate_archive


@pytest.fixture
def sample_archive_path(sample_archive: ArchiveInfo, tmp_path_factory) -> str:
    """Return path to the sample archive, creating it if needed."""
    path = pathlib.Path(sample_archive.get_archive_path())
    if path.exists():
        return str(path)

    base_dir = tmp_path_factory.mktemp("generated_archives")
    try:
        create_archive(sample_archive, str(base_dir))
    except PackageNotInstalledError as e:
        pytest.skip(
            f"Required library for {sample_archive.filename} is not installed: {e}"
        )
        raise
    return sample_archive.get_archive_path(str(base_dir))


@pytest.fixture
def truncated_archive_path(
    sample_archive: ArchiveInfo, sample_archive_path: str, tmp_path_factory
) -> str:
    """Return path to a truncated variant of the sample archive."""
    if not sample_archive.generate_corrupted_variants:
        pytest.skip("No truncated variant defined for this archive")
    base_dir = tmp_path_factory.mktemp("corrupted_archives")
    name, ext = os.path.splitext(sample_archive.filename)
    path = pathlib.Path(base_dir) / f"{name}.truncated{ext}"
    if not path.exists():
        truncate_archive(pathlib.Path(sample_archive_path), path)
    return str(path)


@pytest.fixture
def corrupted_archive_path(
    sample_archive: ArchiveInfo, sample_archive_path: str, tmp_path_factory
) -> str:
    """Return path to a corrupted variant of the sample archive."""
    if not sample_archive.generate_corrupted_variants:
        pytest.skip("No corrupted variant defined for this archive")
    base_dir = tmp_path_factory.mktemp("corrupted_archives")
    name, ext = os.path.splitext(sample_archive.filename)
    path = pathlib.Path(base_dir) / f"{name}.corrupted{ext}"
    if not path.exists():
        corrupt_archive(pathlib.Path(sample_archive_path), path)
    return str(path)
