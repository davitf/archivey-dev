from __future__ import annotations

import os
import subprocess
from datetime import timezone
from typing import Optional

import pytest

from archivey.config import ArchiveyConfig
from archivey.dependency_checker import get_dependency_versions
from archivey.types import (
    TAR_FORMAT_TO_COMPRESSION_FORMAT,
    ArchiveFormat,
    MemberType,
)
from tests.archivey.sample_archives import (
    FileInfo,
)


def write_files_to_dir(dir: str | os.PathLike, files: list[FileInfo]):
    """Write the provided FileInfo objects to ``dir``."""
    for file in sorted(
        files,
        key=lambda x: [MemberType.FILE, MemberType.LINK, MemberType.DIR].index(x.type),
    ):
        full_path = os.path.join(dir, file.name)
        if file.type == MemberType.DIR:
            os.makedirs(full_path, exist_ok=True)
        elif file.type == MemberType.LINK:
            assert file.link_target is not None, "Link target is required"
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            os.symlink(
                file.link_target,
                full_path,
                target_is_directory=file.link_target_type == MemberType.DIR,
            )
        else:
            assert file.contents is not None, "File contents are required"
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(file.contents)

        os.utime(
            full_path,
            (
                file.mtime.replace(tzinfo=timezone.utc).timestamp(),
                file.mtime.replace(tzinfo=timezone.utc).timestamp(),
            ),
            follow_symlinks=False,
        )

        default_permissions_by_type = {
            MemberType.DIR: 0o755,
            MemberType.LINK: 0o777,
            MemberType.FILE: 0o644,
        }
        os.chmod(full_path, file.permissions or default_permissions_by_type[file.type])

    # List the files with ls
    subprocess.run(["ls", "-alF", "-R", "--time-style=full-iso", dir], check=True)


def skip_if_package_missing(format: ArchiveFormat, config: Optional[ArchiveyConfig]):
    format = TAR_FORMAT_TO_COMPRESSION_FORMAT.get(format, format)
    if config is None:
        config = ArchiveyConfig()

    if format == ArchiveFormat.SEVENZIP:
        pytest.importorskip("py7zr")
    elif format == ArchiveFormat.RAR:
        pytest.importorskip("rarfile")
        if get_dependency_versions().unrar_version is None:
            pytest.skip("unrar not installed, skipping RAR truncation test")
    elif format == ArchiveFormat.LZ4:
        pytest.importorskip("lz4")
    elif format == ArchiveFormat.GZIP and config.use_rapidgzip:
        pytest.importorskip("rapidgzip")
    elif format == ArchiveFormat.BZIP2 and config.use_indexed_bzip2:
        pytest.importorskip("indexed_bzip2")
    elif format == ArchiveFormat.XZ and config.use_python_xz:
        pytest.importorskip("lzma")
    elif format == ArchiveFormat.ZSTD and config.use_zstandard:
        pytest.importorskip("zstandard")
    elif format == ArchiveFormat.ZSTD:
        pytest.importorskip("pyzstd")
