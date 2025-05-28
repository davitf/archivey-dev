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


class UnrarNotInstalledError(ArchiveError):
    """Raised when unrar command is not found."""

    def __init__(self, message="unrar command not found. Please install unrar and ensure it is in your PATH. It can usually be installed via your system's package manager (e.g., `apt-get install unrar` or `brew install unrar`)."):
        super().__init__(message)
