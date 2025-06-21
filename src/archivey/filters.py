from __future__ import annotations

import os
from dataclasses import replace
from typing import Callable

from .types import ArchiveMember, MemberType

__all__ = [
    "FilterFunc",
    "fully_trusted",
    "tar_filter",
    "data_filter",
    "get_filter",
]

FilterFunc = Callable[[ArchiveMember, str | None], ArchiveMember | None]

class FilterError(ValueError):
    """Raised when an archive member would extract outside the destination."""


def _sanitize_name(member: ArchiveMember, dest_path: str) -> tuple[str, str]:
    name = member.filename.lstrip("/\\")
    if os.path.isabs(name):
        raise FilterError(f"Absolute path not allowed: {member.filename}")
    dest_real = os.path.realpath(dest_path)
    target = os.path.realpath(os.path.join(dest_real, name))
    if os.path.commonpath([dest_real, target]) != dest_real:
        raise FilterError(f"Extraction outside destination: {member.filename}")
    return name, target


def _check_link(member: ArchiveMember, dest_path: str, name: str) -> None:
    if member.link_target is None:
        return
    if os.path.isabs(member.link_target):
        raise FilterError(f"Absolute link target not allowed: {member.link_target}")
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


def _get_filtered_member(member: ArchiveMember, dest_path: str, for_data: bool) -> ArchiveMember:
    name, _ = _sanitize_name(member, dest_path)
    _check_link(member, dest_path, name)

    mode = member.mode
    if mode is not None:
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


def tar_filter(member: ArchiveMember, dest_path: str | None = None) -> ArchiveMember | None:
    if dest_path is None:
        return member
    return _get_filtered_member(member, dest_path, False)


def data_filter(member: ArchiveMember, dest_path: str | None = None) -> ArchiveMember | None:
    if dest_path is None:
        return member
    return _get_filtered_member(member, dest_path, True)


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
