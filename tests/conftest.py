import logging
import pathlib

import pytest

from archivey.exceptions import PackageNotInstalledError
from tests.archivey.sample_archives import ArchiveInfo
from tests.create_archives import create_archive
from tests.create_corrupted_archives import corrupt_archive

logger = logging.getLogger(__name__)


@pytest.fixture
def sample_archive_path(
    sample_archive: ArchiveInfo, tmp_path_factory: pytest.TempPathFactory
) -> str:
    """Return path to the sample archive, creating it if needed."""
    path = pathlib.Path(sample_archive.get_archive_path())
    if path.exists():
        return str(path)

    output_dir = tmp_path_factory.mktemp("generated_archives")
    try:
        return create_archive(sample_archive, str(output_dir))

    except PackageNotInstalledError as e:
        pytest.skip(
            f"Required library for {sample_archive.filename} is not installed: {e}"
        )


@pytest.fixture(params=["truncate", "single", "multiple", "zeroes", "ffs"])
def corrupted_archive_path(
    sample_archive: ArchiveInfo,
    sample_archive_path: str,
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> str:
    corruption_type = request.param
    path = pathlib.Path(
        sample_archive.get_archive_path(variant=f"corrupted_{corruption_type}")
    )
    if path.exists():
        return str(path)

    output_dir = tmp_path_factory.mktemp("generated_archives")
    output_path = output_dir / sample_archive.get_archive_name(
        variant=f"corrupted_{corruption_type}"
    )
    logger.info(
        f"Creating corrupted archive {output_path} with corruption type {corruption_type}"
    )
    corrupt_archive(
        pathlib.Path(sample_archive_path), output_path, corruption_type=corruption_type
    )
    return str(output_path)
