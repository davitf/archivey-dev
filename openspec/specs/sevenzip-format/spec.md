# 7z Format Reading Specification

## Purpose

Define how Archivey reads 7-Zip archives via the `py7zr` package, including
solidity, encryption, metadata mapping, link-target resolution, the
batch/threaded extraction model used for solid archives, and error translation.

## Requirements

### Requirement: 7z requires the py7zr package

The 7z reader SHALL raise `PackageNotInstalledError` when `py7zr` is not
installed.

#### Scenario: py7zr missing
- **WHEN** a 7z archive is opened and `py7zr` is not installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: 7z requires a seekable source

The 7z reader SHALL raise `ArchiveStreamNotSeekableError` for non-seekable
sources.

#### Scenario: Non-seekable 7z source
- **WHEN** a 7z archive is opened from a non-seekable stream
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: 7z reports solidity and encryption

`get_archive_info()` SHALL report whether the archive is solid and whether it is
password-protected.

#### Scenario: Solid 7z
- **WHEN** a solid 7z archive is opened
- **THEN** `get_archive_info().is_solid` is `True`

### Requirement: 7z member metadata is mapped from py7zr file info

The reader SHALL populate filename (directories with trailing `/`), `file_size`,
`compress_size`, modification time, `type` (including symlink and other), Unix
`mode`, `crc32` (0 for empty files, the stored value for files, otherwise
`None`), Windows attributes when present, and the `encrypted` flag.

#### Scenario: Empty file CRC
- **WHEN** a 7z member is an empty regular file
- **THEN** its `crc32` is `0`

#### Scenario: Encrypted member flag
- **WHEN** a 7z member's coders require a password
- **THEN** its `encrypted` flag is `True`

### Requirement: Link targets are resolved during reading

The reader SHALL resolve symlink targets by extracting link content; when a link
target requires a password that is not available at open time,
`ArchiveEncryptedError` SHALL be raised.

#### Scenario: Encrypted link target without password
- **WHEN** opening a 7z link whose target is encrypted and no password is available
- **THEN** `ArchiveEncryptedError` is raised

### Requirement: Members are read and extracted via a batch model

Because 7z archives are commonly solid, the reader SHALL extract members through
`py7zr`'s extraction mechanism (using a background producer that streams
extracted content), mapping `py7zr`'s sanitized output names back to the
corresponding `ArchiveMember`. Empty files SHALL be yielded immediately without
invoking extraction.

#### Scenario: Solid 7z iteration
- **WHEN** a solid 7z archive is iterated
- **THEN** members are produced from a batch extraction pass and mapped back to
  their `ArchiveMember` objects

#### Scenario: Extraction failure surfaces per member
- **WHEN** extraction fails partway through iteration
- **THEN** remaining pending file members are yielded with an error stream that
  raises on read

### Requirement: 7z errors are translated

The reader SHALL translate `py7zr` exceptions: `Bad7zFile` to
`ArchiveCorruptedError`, `PasswordRequired` to `ArchiveEncryptedError`,
unsupported-compression errors to `ArchiveUnsupportedFeatureError`, `EOFError`
and truncation `struct.error` to `ArchiveEOFError`, and `lzma.LZMAError` (often a
wrong password or corrupt data) to `ArchiveCorruptedError`.

#### Scenario: Corrupted 7z
- **WHEN** `py7zr` raises `Bad7zFile`
- **THEN** `ArchiveCorruptedError` is raised

#### Scenario: Truncated 7z
- **WHEN** `py7zr` raises `EOFError`
- **THEN** `ArchiveEOFError` is raised
