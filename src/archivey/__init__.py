from archivey.archive_stream import ArchiveStream
from archivey.exceptions import (
    ArchiveCorruptedError,
    ArchiveEncryptedError,
    ArchiveEOFError,
    ArchiveError,
    ArchiveFormatError,
    ArchiveMemberNotFoundError,
    ArchiveNotSupportedError, # Existing, similar to UnsupportedArchiveError
    # ArchiveOperationError, # Will add if used by new writer code explicitly
)
from archivey.formats import detect_archive_format_by_signature
from archivey.types import ArchiveFormat, ArchiveInfo, ArchiveMember, MemberType

# Added imports for writers and factory
from archivey.core import open_archive_writer
from archivey.base_writer import ArchiveWriter
from archivey.zip_writer import ZipWriter
from archivey.tar_writer import TarWriter
# Assuming 'open' (reader factory) would be in core as well, if it exists
# from archivey.core import open 

# If specific new exceptions were defined and used by writers, they'd be imported from .exceptions
# For example, if UnsupportedArchiveError was a new distinct exception:
# from archivey.exceptions import UnsupportedArchiveError 

__all__ = [
    # Existing symbols
    "ArchiveStream",
    "ArchiveError",
    "ArchiveFormatError",
    "ArchiveCorruptedError",
    "ArchiveEncryptedError",
    "ArchiveEOFError",
    "ArchiveMemberNotFoundError",
    "ArchiveNotSupportedError", # Keep existing name
    "ArchiveMember",
    "ArchiveInfo",
    "ArchiveFormat",
    "detect_archive_format_by_signature",
    "MemberType",

    # Added symbols for writers
    "open_archive_writer",
    "ArchiveWriter",
    "ZipWriter",
    "TarWriter",
    
    # Add 'open' if it's the reader factory and should be public
    # "open", 
]
