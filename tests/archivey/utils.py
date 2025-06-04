from __future__ import annotations
import os
from archivey.types import MemberType
from sample_archives import FileInfo


def write_files_to_dir(dir: str, files: list[FileInfo]) -> None:
    """Write the given list of FileInfo objects to a directory."""
    # Leave directories for last so their timestamps aren't affected by file creation
    for file in sorted(
        files,
        key=lambda x: [MemberType.FILE, MemberType.LINK, MemberType.DIR].index(x.type),
    ):
        full_path = os.path.join(dir, file.name)
        if file.type == MemberType.DIR:
            os.makedirs(full_path, exist_ok=True)
        elif file.type == MemberType.LINK:
            assert file.link_target is not None, "Link target is required"
            dir_path = os.path.dirname(full_path)
            os.makedirs(dir_path, exist_ok=True)
            os.symlink(
                file.link_target,
                full_path,
                target_is_directory=file.link_target_type == MemberType.DIR,
            )
        else:
            assert file.contents is not None, "File contents are required"
            dir_path = os.path.dirname(full_path)
            os.makedirs(dir_path, exist_ok=True)
            with open(full_path, "wb") as f:
                f.write(file.contents)

        os.utime(
            full_path,
            (file.mtime.timestamp(), file.mtime.timestamp()),
            follow_symlinks=False,
        )

        default_permissions_by_type = {
            MemberType.DIR: 0o755,
            MemberType.LINK: 0o777,
            MemberType.FILE: 0o644,
        }
        os.chmod(full_path, file.permissions or default_permissions_by_type[file.type])

