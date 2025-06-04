from __future__ import annotations

import os
import subprocess
from datetime import timezone

from archivey.types import MemberType
from tests.archivey.sample_archives import FileInfo


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
