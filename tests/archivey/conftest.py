import logging
import pathlib
from typing import cast

import pytest

from archivey.config import ArchiveyConfig
from archivey.exceptions import PackageNotInstalledError
from archivey.internal.dependency_checker import (
    format_dependency_versions,
    get_dependency_versions,
)
from archivey.types import ContainerFormat, StreamFormat
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


@pytest.fixture
def archive_configs(request: pytest.FixtureRequest) -> list[ArchiveyConfig]:
    """Provides all the configs that are relevant for a given archive type."""
    # cast to SampleArchive as this fixture is only used in tests that have a sample_archive fixture
    sample_archive = cast(SampleArchive, request.getfixturevalue("sample_archive"))

    configs = [ArchiveyConfig()]  # Default config

    stream_format = sample_archive.creation_info.format.stream
    container_format = sample_archive.creation_info.format.container

    if stream_format == StreamFormat.GZIP:
        configs.append(ArchiveyConfig(use_rapidgzip=True))
    elif stream_format == StreamFormat.BZIP2:
        configs.append(ArchiveyConfig(use_indexed_bzip2=True))
    elif stream_format == StreamFormat.XZ:
        configs.append(ArchiveyConfig(use_python_xz=True))
    elif stream_format == StreamFormat.ZSTD:
        configs.append(ArchiveyConfig(use_zstandard=True))

    if container_format == ContainerFormat.RAR:
        # For RAR, we test both with and without use_rar_stream
        configs.append(ArchiveyConfig(use_rar_stream=True))

    return configs


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
