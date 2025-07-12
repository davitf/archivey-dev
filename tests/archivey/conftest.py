import logging
import pathlib

import pytest

from archivey.exceptions import PackageNotInstalledError
from archivey.internal.dependency_checker import (
    format_dependency_versions,
    get_dependency_versions,
)
from tests.archivey.create_archives import create_archive
from tests.archivey.sample_archives import SampleArchive

logger = logging.getLogger(__name__)


@pytest.fixture
def sample_archive_path(
    sample_archive: SampleArchive, tmp_path_factory: pytest.TempPathFactory
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


@pytest.fixture(autouse=True, scope="session")
def print_dependency_versions_on_failure(request):
    yield
    logger.warning(
        "\n"
        + "=" * 30
        + " Dependency Versions "
        + "=" * 30
        + "\n"
        + format_dependency_versions(get_dependency_versions())
        + "\n"
        + "=" * 80
    )
