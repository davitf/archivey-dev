"""Defines custom exceptions used throughout the archivey library."""


# Common exceptions for all archive types
class ArchiveError(Exception):
    """Base exception for all archive-related errors encountered by archivey."""

    pass


class ArchiveFormatError(ArchiveError):
    """Raised when an archive's format is incorrect or unexpected."""

    pass


class ArchiveCorruptedError(ArchiveError):
    """Raised when an archive is detected as corrupted, incomplete, or invalid."""

    pass


class ArchiveEncryptedError(ArchiveError):
    """
    Raised when an archive or its member is encrypted and either no password
    was provided, or the provided password was incorrect.
    """

    pass


class ArchiveEOFError(ArchiveError):
    """Raised when an unexpected end-of-file is encountered while reading an archive."""

    pass


class ArchiveMemberNotFoundError(ArchiveError):
    """Raised when a specifically requested member is not found within the archive."""

    pass


class ArchiveNotSupportedError(ArchiveError):
    """Raised when the detected archive format is not supported by archivey."""

    pass


class ArchiveMemberCannotBeOpenedError(ArchiveError):
    """
    Raised when a requested member cannot be opened for reading,
    often because it's a directory, a special file type not meant for direct
    opening, or a link whose target cannot be resolved or opened.
    """

    pass


class PackageNotInstalledError(ArchiveError):
    """
    Raised when a required third-party library or package for handling a specific
    archive format is not installed in the environment.
    """

    pass


class ArchiveIOError(ArchiveError):
    """Raised for general input/output errors during archive operations."""

    pass


class ArchiveFileExistsError(ArchiveError):
    """
    Raised during extraction if a file to be extracted already exists and
    the overwrite mode prevents overwriting it.
    """

    pass


class ArchiveLinkTargetNotFoundError(ArchiveError):
    """
    Raised when a symbolic or hard link within the archive points to a target
    that cannot be found within the same archive.
    """

    pass
