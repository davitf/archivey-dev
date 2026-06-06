# Error Handling Specification

## Purpose

Define the exception hierarchy raised by Archivey and the guarantee that all
errors caused by archive problems are surfaced as `ArchiveError` subclasses, so
that callers can handle archive failures with a single base type while still
discriminating specific causes.

## Requirements

### Requirement: All archive errors derive from ArchiveError

Every error caused by an archive problem SHALL be an instance of `ArchiveError`
(covering reading, decoding, member access, extraction, encryption, and
unsupported features). Underlying library exceptions SHALL be translated into the
appropriate `ArchiveError` subclass by each reader's `_translate_exception` hook.

#### Scenario: Library exception is wrapped
- **WHEN** an underlying library raises a corruption error while reading a member
- **THEN** the caller observes an `ArchiveError` subclass, not the raw library
  exception

### Requirement: ArchiveError carries archive and member context

`ArchiveError` SHALL accept optional `archive_path` and `member_name` attributes,
and its string representation SHALL append them when present.

#### Scenario: Error string includes context
- **WHEN** an `ArchiveError` with an `archive_path` and `member_name` is stringified
- **THEN** the message includes both the archive path and the member being processed

### Requirement: Read errors form a coherent subtree

`ArchiveReadError` SHALL be the base for errors while reading or decoding archive
content, with subclasses `ArchiveUnsupportedFeatureError`, `ArchiveCorruptedError`
(and its subclass `ArchiveEOFError`), and `ArchiveStreamNotSeekableError`.

#### Scenario: Corrupted archive
- **WHEN** an archive is detected as corrupted, incomplete, or invalid
- **THEN** `ArchiveCorruptedError` (or its subclass `ArchiveEOFError` for
  unexpected end-of-file) is raised

#### Scenario: Unexpected end of file
- **WHEN** the input ends while a member or stream is still being read
- **THEN** `ArchiveEOFError` is raised, which is a subclass of
  `ArchiveCorruptedError`

#### Scenario: Unsupported feature
- **WHEN** an archive uses a format feature that Archivey cannot handle
- **THEN** `ArchiveUnsupportedFeatureError` is raised

#### Scenario: Non-seekable stream where seeking is required
- **WHEN** a non-seekable stream is supplied to a reader or backend that needs
  random access
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: Member errors form a coherent subtree

`ArchiveMemberError` SHALL be the base for member-related errors, with subclasses
`ArchiveMemberNotFoundError`, `ArchiveMemberCannotBeOpenedError`, and
`ArchiveLinkTargetNotFoundError`.

#### Scenario: Member not found
- **WHEN** a requested member name does not exist in the archive
- **THEN** `ArchiveMemberNotFoundError` is raised

#### Scenario: Member cannot be opened
- **WHEN** opening a member that is a directory, special file, or unresolved link
- **THEN** `ArchiveMemberCannotBeOpenedError` is raised

### Requirement: Extraction errors form a coherent subtree

`ArchiveExtractionError` SHALL be the base for extraction-to-filesystem errors,
with subclass `ArchiveFileExistsError` raised when an existing file blocks a
write and the overwrite mode forbids overwriting.

#### Scenario: Existing file blocks extraction
- **WHEN** extraction would overwrite an existing file and `overwrite_mode` is
  `ERROR`
- **THEN** `ArchiveFileExistsError` is raised

### Requirement: Specialized errors are provided for encryption, filters, support, and packages

The hierarchy SHALL include `ArchiveEncryptedError` (missing or wrong password),
`ArchiveFilterError` (a filter rejected a member), `ArchiveNotSupportedError`
(unsupported/undetectable format), and `PackageNotInstalledError` (a required
optional dependency is not installed).

#### Scenario: Wrong or missing password
- **WHEN** an encrypted member is read with no password or an incorrect one
- **THEN** `ArchiveEncryptedError` is raised

#### Scenario: Optional dependency missing
- **WHEN** a format requires an optional package that is not installed
- **THEN** `PackageNotInstalledError` is raised
