"""
Custom exceptions raised by Archivey.

The base `ArchiveError` can be accessed from the :mod:`archivey` module, but you
can import more specific subtypes from here if you need to handle them specifically.
"""


# Common exceptions for all archive types
class ArchiveError(Exception):
    """Base exception for all archive-related errors encountered by archivey."""

    def __init__(
        self,
        message: str,
        archive_path: str | None = None,
        member_name: str | None = None,
    ):
        super().__init__(message)
        self.archive_path = archive_path
        self.member_name = member_name

    def __str__(self):
        base = super().__str__()
        if self.archive_path:
            base = f"{base} (in {self.archive_path})"
        if self.member_name:
            base = f"{base} (when processing {self.member_name})"
        return base


class ArchiveCorruptedError(ArchiveError):
    """Raised when an archive is detected as corrupted, incomplete, or invalid."""

    pass


class ArchiveEOFError(ArchiveCorruptedError):
    """Raised when an unexpected end-of-file is encountered while reading an archive."""

    pass


class ArchiveEncryptedError(ArchiveError):
    """
    Raised when an archive or its member is encrypted and either no password
    was provided, or the provided password was incorrect.
    """

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


class ArchiveStreamNotSeekableError(ArchiveError):
    """Raised when a stream is not seekable and it's not supported by the archive format or library."""

    pass


class ArchiveFilterError(ArchiveError):
    """Raised when a filter rejects a member due to unsafe properties."""

    pass
