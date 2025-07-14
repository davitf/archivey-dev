from __future__ import annotations

import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pytest

from archivey import open_archive
from archivey.config import archivey_config


@dataclass
class PerformanceResult:
    total_time: float
    average_time: float


class BaseBench(ABC):
    TEST_ARCHIVES_DIR: ClassVar[Path] = Path("tests/test_archives")
    LARGE_TEST_ARCHIVE: ClassVar[Path] = TEST_ARCHIVES_DIR / "large_archive.zip"
    MEMBER_COUNT: ClassVar[int] = 100

    @abstractmethod
    def run(self) -> dict[str, PerformanceResult]:
        ...


class ArchiveReadBench(BaseBench):
    def run_benchmark(self, archive_path: Path, **config: Any) -> PerformanceResult:
        start_time = time.monotonic()
        with archivey_config(**config):
            with open_archive(archive_path) as archive:
                for member in archive:
                    with member.open() as f:
                        f.read()
        end_time = time.monotonic()
        total_time = end_time - start_time
        return PerformanceResult(
            total_time=total_time, average_time=total_time / self.MEMBER_COUNT
        )


class RandomAccessBench(BaseBench):
    def run_benchmark(self, archive_path: Path, **config: Any) -> PerformanceResult:
        with archivey_config(**config):
            with open_archive(archive_path) as archive:
                members = list(archive)
                random.shuffle(members)
                start_time = time.monotonic()
                for member in members[: self.MEMBER_COUNT]:
                    with member.open() as f:
                        f.read()
                end_time = time.monotonic()
        total_time = end_time - start_time
        return PerformanceResult(
            total_time=total_time, average_time=total_time / self.MEMBER_COUNT
        )
