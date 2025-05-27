# A zipfile-like interface for reading all the files in an archive.

from datetime import datetime
import hashlib
from typing import Tuple, IO
import zlib
from archivey.archive_stream import ArchiveStream
from archivey.exceptions import (
    ArchiveError,
)
from archivey.formats import (
    detect_archive_format_by_signature,
    detect_archive_format_by_filename,
)

import argparse
from tqdm import tqdm


def get_member_checksums(member_file: IO[bytes]) -> Tuple[str, str]:
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
    return format(crc32_value & 0xFFFFFFFF, "08x"), sha256.hexdigest()


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

args = parser.parse_args()

for archive_path in args.files:
    try:
        print(f"\nProcessing {archive_path}:")
        format_by_signature = detect_archive_format_by_signature(archive_path)
        print(f"Format by signature: {format_by_signature}")
        format_by_filename = detect_archive_format_by_filename(archive_path)
        print(f"Format by filename: {format_by_filename}")
        with ArchiveStream(
            archive_path,
            use_libarchive=args.use_libarchive,
            use_rar_stream=args.use_rar_stream,
        ) as archive:
            print(
                f"Archive format: {archive.get_format()} {archive.get_archive_info()}"
            )
            if args.info:
                continue

            members = archive.infolist() if not args.stream else archive.info_iter()

            for member in tqdm(members, desc="Computing checksums"):
                if member.is_file:
                    assert isinstance(member.filename, str)
                    assert isinstance(member.mtime, datetime)

                    if "inside" in member.filename:
                        print("SKIPPING", member.filename)
                        continue

                    try:
                        with archive.open(member) as f:
                            crc32, sha256 = get_member_checksums(f)
                        print(
                            f"{member.size:12d} {crc32} {sha256} {member.filename} {member.mtime}"
                        )
                    except ArchiveError as e:
                        print(f"Error processing {member.filename}: {repr(e)}")

                elif member.is_link:
                    assert (
                        isinstance(member.link_target, str)
                        or member.link_target is None
                    )
                    print(
                        f"{member.size:12d} {member.type.upper()} {member.filename} {member.mtime} {member.link_target}"
                    )
                else:
                    print(
                        f"{member.size:12d} {member.type.upper()} {member.filename} {member.mtime}"
                    )
                if member.comment:
                    print(f"    Comment: {member.comment}")

    except ArchiveError as e:
        print(f"Error processing {archive_path}: {e}")
    print()
