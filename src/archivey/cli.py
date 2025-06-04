# A zipfile-like interface for reading all the files in an archive.

import argparse
import hashlib
import logging
import os
import builtins
import zlib
from datetime import datetime
from typing import IO, Tuple

from tqdm import tqdm

from archivey.io_helpers import IOStats, StatsIO

from archivey.base_reader import ArchiveReader
from archivey.core import open_archive
from archivey.exceptions import (
    ArchiveError,
)
from archivey.types import ArchiveMember, MemberType

logging.basicConfig(level=logging.INFO)


def format_mode(member_type: MemberType, mode: int) -> str:
    permissions = mode & 0o777
    type_char = (
        "d"
        if member_type == MemberType.DIR
        else "l"
        if member_type == MemberType.LINK
        else "-"
    )
    # Convert permissions to rwxrwxrwx format
    permissions_str = type_char
    letters = "xwr" * 3
    for bit in range(8, -1, -1):
        if permissions & (1 << bit):
            permissions_str += letters[bit]
        else:
            permissions_str += "-"
    return permissions_str


def get_member_checksums(member_file: IO[bytes]) -> Tuple[int, str]:
    """
    Compute both CRC32 and SHA256 checksums for a file within an archive.
    Returns a tuple of (crc32, sha256) as hex strings.
    """
    crc32_value: int = 0
    sha256 = hashlib.sha256()

    # Read the file in chunks
    for block in iter(lambda: member_file.read(65536), b""):
        crc32_value = zlib.crc32(block, crc32_value)
        sha256.update(block)
    return crc32_value & 0xFFFFFFFF, sha256.hexdigest()


parser = argparse.ArgumentParser(
    description="List contents of archive files with checksums."
)
parser.add_argument("files", nargs="+", help="Archive files to process")
parser.add_argument(
    "--use-libarchive",
    action="store_true",
    help="Use libarchive for processing archives",
)
parser.add_argument(
    "--use-rar-stream",
    action="store_true",
    help="Use the RAR stream reader for RAR files",
)
parser.add_argument("--stream", action="store_true", help="Stream the archive")
parser.add_argument("--info", action="store_true", help="Print info about the archive")
parser.add_argument("--password", help="Password for encrypted archives")
parser.add_argument("--hide-progress", action="store_true", help="Hide progress bar")
parser.add_argument(
    "--use-stored-metadata",
    action="store_true",
    help="Use stored metadata for single file archives",
)
parser.add_argument(
    "--track-io",
    action="store_true",
    help="Track IO statistics for archive file access",
)

args = parser.parse_args()

stats_per_file: dict[str, IOStats] = {}
if args.track_io:
    original_open = builtins.open
    target_paths = {os.path.abspath(p) for p in args.files}

    def patched_open(file, mode="r", *oargs, **okwargs):
        path = None
        if isinstance(file, (str, bytes, os.PathLike)):
            path = os.path.abspath(file)
        if path in target_paths and "r" in mode and not any(m in mode for m in ["w", "a", "+"]):
            f = original_open(file, mode, *oargs, **okwargs)
            stats = stats_per_file.setdefault(path, IOStats())
            return StatsIO(f, stats)
        return original_open(file, mode, *oargs, **okwargs)

    builtins.open = patched_open


def process_member(
    member: ArchiveMember, archive: ArchiveReader, stream: IO[bytes] | None = None
):
    stream_to_close: IO[bytes] | None = None

    encrypted_str = "E" if member.encrypted else " "
    size_str = "?" * 12 if member.file_size is None else f"{member.file_size:12d}"
    format_str = format_mode(member.type, member.mode or 0)

    if member.is_file:
        assert isinstance(member.filename, str)
        assert isinstance(member.mtime, datetime)

        try:
            if member.extra:
                print(f"{member.filename} {member.extra}")

            if stream is None:
                stream = stream_to_close = archive.open(member, pwd=args.password)

            crc32, sha256 = get_member_checksums(stream)
            if member.crc32 is not None and member.crc32 != crc32:
                crc_error = f" != {member.crc32:08x}"
            else:
                crc_error = ""

            print(
                f"{encrypted_str}  {size_str}  {format_str}  {crc32:08x}{crc_error}  {sha256[:16]}  {member.mtime}  {member.filename}"
            )

        except ArchiveError as e:
            formated_crc = (
                f"{member.crc32:08x}" if member.crc32 is not None else "?" * 8
            )
            print(
                f"{encrypted_str}  {size_str}  {format_str}  {formated_crc}  {' ' * 16}  {member.mtime}  {member.filename} -- ERROR: {repr(e)}"
            )
        finally:
            if stream_to_close is not None:
                stream_to_close.close()

    elif member.is_link:
        assert isinstance(member.link_target, str) or member.link_target is None
        print(
            f"{encrypted_str}  {size_str}  {format_str}  {' ' * 8}  {' ' * 16}  {member.mtime}  {member.filename} -> {member.link_target}"
        )
    else:
        print(
            f"{encrypted_str}  {size_str}  {format_str}  {' ' * 8}  {' ' * 16}  {member.mtime}  {member.filename} {member.type.upper()}"
        )
    if member.comment:
        print(f"    Comment: {member.comment}")


for archive_path in args.files:
    try:
        print(f"\nProcessing {archive_path}:")
        with open_archive(
            archive_path,
            use_libarchive=args.use_libarchive,
            use_rar_stream=args.use_rar_stream,
            pwd=args.password,
            use_single_file_stored_metadata=args.use_stored_metadata,
        ) as archive:
            print(f"Archive format: {archive.format} {archive.get_archive_info()}")
            if args.info:
                continue

            if args.stream:
                members_if_available = archive.get_members_if_available()

                for member, stream in tqdm(
                    archive.iter_members_with_io(),
                    desc="Computing checksums",
                    disable=args.hide_progress,
                    total=len(members_if_available)
                    if members_if_available is not None
                    else None,
                ):
                    process_member(member, archive, stream)

            else:
                members = archive.get_members()

                for member in tqdm(
                    members,
                    desc="Computing checksums",
                    disable=args.hide_progress,
                    total=len(members) if members is not None else None,
                ):
                    process_member(member, archive)

    except ArchiveError as e:
        print(f"Error processing {archive_path}: {e}")
    if args.track_io:
        abs_path = os.path.abspath(archive_path)
        stats = stats_per_file.get(abs_path)
        if stats is not None:
            print(
                f"IO stats for {archive_path}: {stats.bytes_read} bytes read, {stats.seek_calls} seeks"
            )
    print()

if args.track_io:
    builtins.open = original_open
