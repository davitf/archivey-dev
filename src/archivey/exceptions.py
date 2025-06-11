# Common exceptions for all archive types
class ArchiveError(Exception):
    """Base exception for all archive-related errors."""

    pass


class ArchiveFormatError(ArchiveError):
    """Raised when an archive is not in the expected format."""

    pass


class ArchiveCorruptedError(ArchiveError):
    """Raised when an archive is corrupted or invalid."""

    pass


class ArchiveEncryptedError(ArchiveError):
    """Raised when an archive is encrypted and password is required."""

    pass


class ArchiveEOFError(ArchiveError):
    """Raised when unexpected EOF is encountered while reading an archive."""

    pass


class ArchiveMemberNotFoundError(ArchiveError):
    """Raised when a requested member is not found in the archive."""

    pass


class ArchiveNotSupportedError(ArchiveError):
    """Raised when the archive format is not supported."""

    pass


class ArchiveMemberCannotBeOpenedError(ArchiveError):
    """Raised when a requested member cannot be opened."""

    pass


class PackageNotInstalledError(ArchiveError):
    """Raised when a required library is not installed."""

    pass


class ArchiveIOError(ArchiveError):
    """Raised when an I/O error occurs."""

    pass


class ArchiveFileExistsError(ArchiveError):
    """Raised when a file already exists while extracting."""

    pass


class ArchiveLinkTargetNotFoundError(ArchiveError):
    """Raised when a link target is not found in the archive."""

    pass
