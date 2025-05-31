# A zipfile-like interface for reading all the files in an archive.

import argparse
import hashlib
import logging
import zlib
from datetime import datetime
from typing import IO, Tuple

from tqdm import tqdm

from archivey.archive_stream import ArchiveStream
from archivey.exceptions import (
    ArchiveError,
)
from archivey.types import MemberType

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

args = parser.parse_args()

for archive_path in args.files:
    try:
        print(f"\nProcessing {archive_path}:")
        with ArchiveStream(
            archive_path,
            use_libarchive=args.use_libarchive,
            use_rar_stream=args.use_rar_stream,
            use_single_file_stored_metadata=args.use_stored_metadata,
            pwd=args.password,
        ) as archive:
            print(
                f"Archive format: {archive.get_format()} {archive.get_archive_info()}"
            )
            if args.info:
                continue

            members = archive.infolist() if not args.stream else archive.info_iter()

            for member in tqdm(
                members, desc="Computing checksums", disable=args.hide_progress
            ):
                encrypted_str = "E" if member.encrypted else " "
                size_str = (
                    "?" * 12 if member.file_size is None else f"{member.file_size:12d}"
                )
                format_str = format_mode(member.type, member.mode or 0)

                if member.is_file:
                    assert isinstance(member.filename, str)
                    assert isinstance(member.mtime, datetime)

                    if "inside" in member.filename:
                        print("SKIPPING", member.filename)
                        continue

                    try:
                        if member.extra:
                            print(f"{member.filename} {member.extra}")

                        with archive.open(member, pwd=args.password) as f:
                            crc32, sha256 = get_member_checksums(f)
                            if member.crc32 is not None and member.crc32 != crc32:
                                crc_error = f" != {member.crc32:08x}"
                            else:
                                crc_error = ""

                        print(
                            f"{encrypted_str}  {size_str}  {format_str}  {crc32:08x}{crc_error}  {sha256[:16]}  {member.mtime}  {member.filename}"
                        )

                    except ArchiveError as e:
                        formated_crc = (
                            f"{member.crc32:08x}"
                            if member.crc32 is not None
                            else "?" * 8
                        )
                        print(
                            f"{encrypted_str}  {size_str}  {format_str}  {formated_crc}  {' ' * 16}  {member.mtime}  {member.filename} -- ERROR: {repr(e)}"
                        )

                elif member.is_link:
                    assert (
                        isinstance(member.link_target, str)
                        or member.link_target is None
                    )
                    print(
                        f"{encrypted_str}  {size_str}  {format_str}  {' ' * 8}  {' ' * 16}  {member.mtime}  {member.filename} -> {member.link_target}"
                    )
                else:
                    print(
                        f"{encrypted_str}  {size_str}  {format_str}  {' ' * 8}  {' ' * 16}  {member.mtime}  {member.filename} {member.type.upper()}"
                    )
                if member.comment:
                    print(f"    Comment: {member.comment}")

    except ArchiveError as e:
        print(f"Error processing {archive_path}: {e}")
    print()
