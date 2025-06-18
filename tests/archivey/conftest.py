import logging
import os # Added
from pathlib import Path # Added
import pytest
from tests.archivey.sample_archives import SampleArchive, ArchiveFormat, GenerationMethod # Added
from tests.archivey.testing_utils import write_files_to_dir # Added


from archivey.dependency_checker import (
    format_dependency_versions,
    get_dependency_versions,
)

logger = logging.getLogger(__name__)


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

@pytest.fixture
def dynamic_archive_path_provider(tmp_path_factory: pytest.TempPathFactory):
    def _provider(sa: SampleArchive) -> str:
        if sa.creation_info.format == ArchiveFormat.FOLDER:
            # Sanitize filename for directory creation to avoid issues with special chars
            # and overly long names that might exceed path limits on some systems.
            # Using a hash or a simpler counter might be more robust for very long/complex names.
            sanitized_name_prefix = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in sa.file_basename)
            # Add a short unique part from the suffix to differentiate if basename is same for different folder types
            sanitized_suffix_part = "".join(c if c.isalnum() else '' for c in sa.creation_info.file_suffix[:10])
            dir_name_base = f"{sanitized_name_prefix}_{sanitized_suffix_part}"

            # tmp_path_factory by default creates dirs like "pytest-of-user/pytest-current/test_name0"
            # mktemp will create a subdir under that.
            archive_dir = tmp_path_factory.mktemp(dir_name_base, numbered=True)

            write_files_to_dir(archive_dir, sa.contents.files)
            return str(archive_dir)
        else:
            # For non-FOLDER types, return the standard path from SampleArchive.
            # This assumes that SampleArchive.get_archive_path() points to pre-existing files for other formats.
            return sa.get_archive_path()
    return _provider
