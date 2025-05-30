"""Archivey: A Python library for reading various archive formats.

This library provides a unified, stream-based interface for accessing
members within archives of different formats. It aims for a `zipfile`-like
user experience.

Core components include:
    - ArchiveStream: The main class for opening and interacting with archives.
    - Various Reader Classes: Backend implementations for specific archive
      formats (e.g., ZipReader, TarReader).
    - Custom Exceptions: A hierarchy of exceptions for archive-related errors.
    - Data Types: Enums and dataclasses for representing archive metadata.

Supported formats include ZIP, RAR, 7z, TAR (and its compressed variants),
GZIP, BZIP2, XZ, ZSTD, and LZ4.
"""
from archivey.archive_stream import ArchiveStream
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError,
)
from archivey.formats import detect_archive_format_by_signature
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

__all__ = [
    "ArchiveStream",
    "ArchiveError",
    "ArchiveFormatError",
    "ArchiveCorruptedError",
    "ArchiveEncryptedError",
    "ArchiveEOFError",
    "ArchiveMemberNotFoundError",
    "ArchiveNotSupportedError",
    "ArchiveMember",
    "ArchiveInfo",
    "ArchiveFormat",
    "detect_archive_format_by_signature",
    "MemberType",
]
