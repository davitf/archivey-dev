from __future__ import annotations

import os
import posixpath
from dataclasses import replace
from typing import Callable

from .types import ArchiveMember, MemberType

__all__ = [
    "FilterFunc",
    "fully_trusted",
    "create_filter",
    "tar_filter",
    "data_filter",
    "get_filter",
]

FilterFunc = Callable[[ArchiveMember, str | None], ArchiveMember | None]

class FilterError(ValueError):
    """Raised when an archive member would extract outside the destination."""


def _sanitize_name(
    member: ArchiveMember,
    dest_path: str | None,
    *,
    allow_absolute: bool,
) -> tuple[str, str | None]:
    name = member.filename
    if os.path.isabs(name) and not allow_absolute:
        raise FilterError(f"Absolute path not allowed: {member.filename}")

    name = name.lstrip("/\\")

    if dest_path is None:
        norm = posixpath.normpath(name)
        if norm.startswith("..") or "/../" in norm:
            raise FilterError(f"Extraction outside destination: {member.filename}")
        return name, None

    dest_real = os.path.realpath(dest_path)
    target = os.path.realpath(os.path.join(dest_real, name))
    if os.path.commonpath([dest_real, target]) != dest_real:
        raise FilterError(f"Extraction outside destination: {member.filename}")
    return name, target


def _check_link(
    member: ArchiveMember,
    dest_path: str | None,
    name: str,
    *,
    allow_outside: bool,
) -> None:
    if member.link_target is None or allow_outside:
        return
    if os.path.isabs(member.link_target):
        raise FilterError(f"Absolute link target not allowed: {member.link_target}")

    if dest_path is None:
        norm = posixpath.normpath(member.link_target)
        if norm.startswith("..") or "/../" in norm:
            raise FilterError(
                f"Link target outside destination: {member.link_target} for {member.filename}"
            )
        return

    dest_real = os.path.realpath(dest_path)
    if member.type == MemberType.SYMLINK:
        target = os.path.realpath(
            os.path.join(dest_real, os.path.dirname(name), member.link_target)
        )
    else:
        target = os.path.realpath(os.path.join(dest_real, member.link_target))
    if os.path.commonpath([dest_real, target]) != dest_real:
        raise FilterError(
            f"Link target outside destination: {member.link_target} for {member.filename}"
        )


def _get_filtered_member(
    member: ArchiveMember,
    dest_path: str | None,
    *,
    for_data: bool,
    allow_absolute_paths: bool,
    allow_symlinks_to_outside: bool,
    sanitize_permissions: bool,
) -> ArchiveMember:
    name, _ = _sanitize_name(member, dest_path, allow_absolute=allow_absolute_paths)
    _check_link(
        member,
        dest_path,
        name,
        allow_outside=allow_symlinks_to_outside,
    )

    mode = member.mode
    if sanitize_permissions and mode is not None:
        mode &= 0o777
        if for_data and member.is_file:
            mode &= ~0o111
            mode |= 0o600
        if mode == member.mode:
            mode = None
    new_attrs = {}
    if name != member.filename:
        new_attrs["filename"] = name
    if mode is not None:
        new_attrs["mode"] = mode
    if new_attrs:
        member = replace(member, **new_attrs)
    return member


def fully_trusted(member: ArchiveMember, dest_path: str | None = None) -> ArchiveMember | None:
    return member


def create_filter(
    *,
    allow_absolute_paths: bool = False,
    allow_symlinks_to_outside: bool = False,
    sanitize_permissions: bool = True,
    for_data: bool = False,
) -> FilterFunc:
    def _filter(member: ArchiveMember, dest_path: str | None = None) -> ArchiveMember | None:
        return _get_filtered_member(
            member,
            dest_path,
            for_data=for_data,
            allow_absolute_paths=allow_absolute_paths,
            allow_symlinks_to_outside=allow_symlinks_to_outside,
            sanitize_permissions=sanitize_permissions,
        )

    return _filter

# Default filters inspired by Python's tarfile module
tar_filter = create_filter()

data_filter = create_filter(for_data=True)


_FILTERS: dict[str, FilterFunc] = {
    "fully_trusted": fully_trusted,
    "tar": tar_filter,
    "data": data_filter,
}


def get_filter(name: str | None) -> FilterFunc | None:
    if name is None:
        return None
    try:
        return _FILTERS[name]
    except KeyError:
        raise ValueError(f"Unknown filter name: {name}")
