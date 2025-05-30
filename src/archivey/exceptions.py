# Common exceptions for all archive types
class ArchiveError(Exception):
    """Base exception for all archive-related errors in the archivey library.

    All other custom exceptions raised by this library inherit from this class.
    """

    pass


class ArchiveFormatError(ArchiveError):
    """Raised when an archive file does not conform to the expected format.

    This can occur if the file is not a valid archive of the type being
    processed, or if its internal structure is recognized but inconsistent.
    """

    pass


class ArchiveCorruptedError(ArchiveError):
    """Raised when an archive is detected as corrupted or structurally invalid.

    This may indicate issues like failed CRC checks, malformed headers,
    or incomplete data streams within the archive.
    """

    pass


class ArchiveEncryptedError(ArchiveError):
    """Raised when attempting to access an encrypted archive or member without a password,
    or with an incorrect password.
    """

    pass


class ArchiveEOFError(ArchiveError):
    """Raised when an unexpected end-of-file (EOF) is encountered while reading an archive.

    This typically means the archive is truncated or incomplete.
    """

    pass


class ArchiveMemberNotFoundError(ArchiveError, KeyError):
    """Raised when a requested member (file or directory) is not found in the archive.

    This exception inherits from `KeyError` for partial compatibility with
    interfaces that expect dictionary-like key errors when looking up members.
    """

    pass


class ArchiveNotSupportedError(ArchiveError):
    """Raised when the archive format is not supported by the library or current configuration.

    This can happen if the format is unknown, or if optional dependencies
    required for a specific format are not installed.
    """

    pass
