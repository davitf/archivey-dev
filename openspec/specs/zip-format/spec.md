# ZIP Format Reading Specification

## Purpose

Define how Archivey reads ZIP archives via the stdlib `zipfile` module: member
listing from the central directory, metadata mapping, symlink handling,
encryption, comments, and translation of `zipfile` errors into `ArchiveError`s.

## Requirements

### Requirement: ZIP archives require a seekable source

The ZIP reader SHALL require a seekable source. When opened from a non-seekable
stream, it SHALL raise `ArchiveStreamNotSeekableError`.

#### Scenario: Non-seekable ZIP source
- **WHEN** a ZIP archive is opened from a non-seekable stream
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: ZIP archives are non-solid with an upfront member list

The ZIP reader SHALL report `is_solid=False` and SHALL support providing the full
member list from the central directory without reading file data.

#### Scenario: Listing without decompression
- **WHEN** members are listed for a ZIP archive
- **THEN** they are read from the central directory and `is_solid` is `False`

### Requirement: ZIP member metadata is mapped from ZipInfo

The reader SHALL populate `filename` (normalized to forward slashes),
`file_size`, `compress_size`, modification time (using the extended-timestamp
extra field for sub-second precision when present), `type`, Unix `mode` (from the
external attributes), `crc32`, a human-readable `compression_method`, `comment`,
`create_system`, and the `encrypted` flag.

#### Scenario: Compression method name
- **WHEN** a member uses the deflate method
- **THEN** its `compression_method` is the human-readable name for deflate

#### Scenario: Extended timestamp precision
- **WHEN** a member carries an extended-timestamp extra field
- **THEN** its modification time reflects that higher-precision value

### Requirement: Symlinks expose their target

When a member's Unix mode marks it a symbolic link, the reader SHALL classify it
as `SYMLINK` and SHALL read its stored content as the UTF-8 link target.

#### Scenario: ZIP symlink
- **WHEN** a ZIP member has the symlink mode bit set
- **THEN** its `type` is `SYMLINK` and `link_target` is its decoded content

### Requirement: ZIP errors are translated

The reader SHALL translate `zipfile.BadZipFile` to `ArchiveCorruptedError`,
password-related `RuntimeError`s to `ArchiveEncryptedError`, unsupported
compression-method errors to `ArchiveUnsupportedFeatureError`, and seek-related
`io.UnsupportedOperation` to `ArchiveStreamNotSeekableError`.

#### Scenario: Corrupted ZIP
- **WHEN** `zipfile` raises `BadZipFile`
- **THEN** `ArchiveCorruptedError` is raised

#### Scenario: Wrong password
- **WHEN** reading an encrypted member with a wrong or missing password
- **THEN** `ArchiveEncryptedError` is raised

#### Scenario: Unsupported compression method
- **WHEN** a member uses a compression method `zipfile` cannot handle
- **THEN** `ArchiveUnsupportedFeatureError` is raised

### Requirement: Archive comment and encryption status are reported

`get_archive_info()` SHALL expose the archive comment (decoded with encoding
fallbacks) and SHALL indicate in `extra` whether any member is encrypted.

#### Scenario: Archive comment
- **WHEN** a ZIP archive has a comment
- **THEN** `get_archive_info().comment` returns the decoded comment
