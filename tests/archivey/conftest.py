import logging

import pytest

from archivey.internal.dependency_checker import (
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
