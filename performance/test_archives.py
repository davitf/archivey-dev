from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from .base import ArchiveReadBench


class TestZip(ArchiveReadBench):
    @pytest.mark.parametrize(
        "archive_name",
        [
            "basic_nonsolid__infozip.zip",
            "basic_nonsolid__zipfile_deflate.zip",
        ],
    )
    def test_zip_read(self, benchmark: Any, archive_name: str) -> None:
        archive_path = self.TEST_ARCHIVES_DIR / archive_name
        benchmark(self.run_benchmark, archive_path)


class TestTar(ArchiveReadBench):
    @pytest.mark.parametrize(
        "archive_name",
        [
            "basic_solid__tarcmd.tar",
            "basic_solid__tarfile.tar",
            "basic_solid__tarcmd.tar.gz",
            "basic_solid__tarfile.tar.gz",
            "basic_solid__tarfile.tar.zst",
        ],
    )
    def test_tar_read(self, benchmark: Any, archive_name: str) -> None:
        archive_path = self.TEST_ARCHIVES_DIR / archive_name
        benchmark(self.run_benchmark, archive_path)
