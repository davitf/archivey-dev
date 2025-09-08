import logging
import pathlib

import pytest
from _pytest.main import Session
from _pytest.python import Metafunc

from archivey.config import ArchiveyConfig
from archivey.exceptions import PackageNotInstalledError
from archivey.internal.dependency_checker import (
    format_dependency_versions,
    get_dependency_versions,
)
from archivey.types import ArchiveFormat, StreamFormat
from tests.archivey.create_archives import create_archive
from tests.archivey.sample_archives import SampleArchive

logger = logging.getLogger(__name__)


def archive_configs_for(sample_archive: SampleArchive) -> list[ArchiveyConfig]:
    cfgs = [ArchiveyConfig()]
    stream_format = sample_archive.creation_info.format.stream
    if stream_format == StreamFormat.GZIP:
        cfgs.append(ArchiveyConfig(use_rapidgzip=True))
    elif stream_format == StreamFormat.BZIP2:
        cfgs.append(ArchiveyConfig(use_indexed_bzip2=True))
    elif stream_format == StreamFormat.XZ:
        cfgs.append(ArchiveyConfig(use_python_xz=True))
    elif stream_format == StreamFormat.ZSTD:
        cfgs.append(ArchiveyConfig(use_zstandard=True))

    if sample_archive.creation_info.format == ArchiveFormat.RAR:
        cfgs.append(ArchiveyConfig(use_rar_stream=True))

    return cfgs


def _cfg_id(cfg: ArchiveyConfig) -> str:
    # Make a short, stable id for nice test names
    bits = []
    if getattr(cfg, "use_rapidgzip", False):
        bits.append("rapidgzip")
    if getattr(cfg, "use_indexed_bzip2", False):
        bits.append("indexed_bzip2")
    if getattr(cfg, "use_python_xz", False):
        bits.append("python_xz")
    if getattr(cfg, "use_zstandard", False):
        bits.append("zstandard")
    if getattr(cfg, "use_rar_stream", False):
        bits.append("rar_stream")
    return ",".join(bits) or "default"


def pytest_generate_tests(metafunc: Metafunc):
    if "archive_config" in metafunc.fixturenames:
        marker = metafunc.definition.get_closest_marker("sample_archives")
        if not marker:
            pytest.fail(
                "Test function requested 'archive_config' fixture but is not marked "
                "with '@pytest.mark.sample_archives'"
            )

        sample_archives_list = list(marker.args[0])
        if marker.kwargs.get("exclude_folders"):
            sample_archives_list = [
                s
                for s in sample_archives_list
                if s.creation_info.format != ArchiveFormat.FOLDER
            ]

        params = []
        for sa in sample_archives_list:
            if sa.skip_test:
                params.append(
                    pytest.param(
                        sa,
                        ArchiveyConfig(),
                        id=f"{sa.filename}::skipped",
                        marks=pytest.mark.skip,
                    )
                )
                continue
            for cfg in archive_configs_for(sa):
                params.append(pytest.param(sa, cfg, id=f"{sa.filename}::{_cfg_id(cfg)}"))
        metafunc.parametrize("sample_archive,archive_config", params)


@pytest.fixture
def sample_archive(request) -> SampleArchive:
    return request.param


@pytest.fixture
def archive_config(request) -> ArchiveyConfig:
    return request.param


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
