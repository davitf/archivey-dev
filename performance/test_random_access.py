from __future__ import annotations

from typing import Any

import pytest

from .base import RandomAccessBench


class TestRandomAccess(RandomAccessBench):
    @pytest.mark.parametrize(
        "archive_name,config",
        [
            ("large_archive.zip", {}),
            ("large_archive.tar.gz", {"use_rapidgzip": True}),
            ("large_archive.tar.gz", {"use_rapidgzip": False}),
        ],
    )
    def test_random_access(
        self, benchmark: Any, archive_name: str, config: dict[str, Any]
    ) -> None:
        archive_path = self.TEST_ARCHIVES_DIR / archive_name
        benchmark(self.run_benchmark, archive_path, **config)
