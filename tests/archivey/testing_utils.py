from __future__ import annotations

import os
import subprocess
import zlib
from datetime import timezone
from typing import TYPE_CHECKING, Optional

import pytest

from archivey.config import ArchiveyConfig
from archivey.internal.dependency_checker import get_dependency_versions
from archivey.types import (
    TAR_FORMAT_TO_COMPRESSION_FORMAT,
    ArchiveFormat,
    MemberType,
)

if TYPE_CHECKING:
    from tests.archivey.sample_archives import (
        FileInfo,
    )


def write_files_to_dir(dir: str | os.PathLike, files: list[FileInfo]):
    """Write the provided FileInfo objects to ``dir``."""
    for file in sorted(
        files,
        key=lambda x: [
            MemberType.FILE,
            MemberType.HARDLINK,
            MemberType.SYMLINK,
            MemberType.DIR,
        ].index(x.type),
    ):
        full_path = os.path.join(dir, file.name)
        if file.type == MemberType.DIR:
            os.makedirs(full_path, exist_ok=True)
        elif file.type == MemberType.HARDLINK:
            assert file.link_target is not None, "Link target is required"
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            os.link(
                os.path.join(dir, file.link_target),
                full_path,
            )
        elif file.type == MemberType.SYMLINK:
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

        if file.type != MemberType.HARDLINK:
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
                MemberType.SYMLINK: 0o777,
                MemberType.FILE: 0o644,
            }
            perm = file.permissions or default_permissions_by_type[file.type]
            if file.type == MemberType.SYMLINK:
                try:
                    os.chmod(full_path, perm, follow_symlinks=False)
                except (NotImplementedError, OSError):
                    # Platforms without lchmod support may not allow setting
                    # permissions on symlinks. Ignore in that case.
                    pass
            else:
                os.chmod(full_path, perm)

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
        pytest.importorskip("xz")
    elif format == ArchiveFormat.ZSTD and config.use_zstandard:
        pytest.importorskip("zstandard")
    elif format == ArchiveFormat.ZSTD:
        pytest.importorskip("pyzstd")


def normalize_newlines(s: str | None) -> str | None:
    return s.replace("\r\n", "\n") if s else None


def get_crc32(data: bytes) -> int:
    """
    Compute CRC32 checksum for a file within an archive.
    Returns a hex string.
    """
    crc32_value: int = 0

    # Read the file in chunks
    crc32_value = zlib.crc32(data, crc32_value)
    return crc32_value & 0xFFFFFFFF


def remove_duplicate_files(files: list[FileInfo]) -> list[FileInfo]:
    """Remove duplicate files, leaving only the last one for each file name."""
    return list({file.name: file for file in files}.values())
