# RAR Format Reading Specification

## Purpose

Define how Archivey reads RAR archives via the `rarfile` package (and the `unrar`
binary), including version/solidity reporting, metadata mapping with RAR-specific
workarounds, link-target resolution, encryption handling, the optional
unrar-streaming reader for solid archives, and error translation.

## Requirements

### Requirement: RAR requires the rarfile package

The RAR reader SHALL raise `PackageNotInstalledError` when the `rarfile` package
is not installed.

#### Scenario: rarfile missing
- **WHEN** a RAR archive is opened and `rarfile` is not installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: RAR requires a seekable source

The RAR reader SHALL raise `ArchiveStreamNotSeekableError` for non-seekable
sources.

#### Scenario: Non-seekable RAR source
- **WHEN** a RAR archive is opened from a non-seekable stream
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: RAR reports version, solidity, and header encryption

`get_archive_info()` SHALL report the RAR version, whether the archive is solid,
the archive comment, whether a password is needed, and whether the headers are
encrypted.

#### Scenario: Solid RAR
- **WHEN** a solid RAR archive is opened
- **THEN** `get_archive_info().is_solid` is `True`

### Requirement: RAR member metadata is mapped with format workarounds

The reader SHALL populate filename (working around the RAR 2.9–4 UTF-16
truncation bug for non-BMP characters), sizes, modification time (RAR5 UTC or
RAR4 local), type (including hardlinks), `mode`, a human-readable
`compression_method`, `comment`, `create_system`, Windows attributes, and the
`encrypted` flag.

#### Scenario: Hardlink classification
- **WHEN** a RAR entry is a hard link
- **THEN** its `type` is `HARDLINK`

### Requirement: Encrypted RAR5 CRCs are not reported as plain CRCs

The reader SHALL set `crc32` to `None` when a RAR 5.0 member uses tweaked
(password-dependent) checksums, rather than reporting the tweaked value as a
plain CRC32.

#### Scenario: Tweaked checksum
- **WHEN** a member's checksum is password-tweaked
- **THEN** the member's `crc32` is `None`

### Requirement: RAR link targets are resolved, using unrar when needed

The reader SHALL determine link targets from RAR5 redirection metadata when
present, and otherwise by reading the member; when that is unavailable it MAY
extract via the `unrar` binary. When a link target requires a password that is
not available, `ArchiveEncryptedError` SHALL be raised at open time.

#### Scenario: RAR5 redirect target
- **WHEN** a RAR5 symlink stores its target in redirection metadata
- **THEN** `link_target` is populated from that metadata

#### Scenario: Encrypted link target without password
- **WHEN** opening a link whose target is encrypted and no password is available
- **THEN** `ArchiveEncryptedError` is raised

### Requirement: Encrypted members verify the password on open

When opening an encrypted member, the reader SHALL raise `ArchiveEncryptedError`
when no password is supplied, and SHALL raise `ArchiveEncryptedError` when the
supplied password is verified incorrect.

#### Scenario: Missing password
- **WHEN** an encrypted RAR member is opened without a password
- **THEN** `ArchiveEncryptedError` is raised

#### Scenario: Wrong password
- **WHEN** an encrypted RAR member is opened with a wrong password
- **THEN** `ArchiveEncryptedError` is raised

### Requirement: use_rar_stream enables single-pass solid extraction

When `use_rar_stream` is `True`, iteration SHALL use the `unrar` binary to stream
all member contents in a single pass (avoiding per-member re-decompression of
solid archives). This SHALL require the `unrar` binary, raising
`PackageNotInstalledError` if it is missing, and SHALL verify each member's CRC
as it is read. When `False`, iteration SHALL use the default `rarfile`-based path.

#### Scenario: Streaming solid RAR
- **WHEN** a solid RAR archive is iterated with `use_rar_stream=True`
- **THEN** member contents are produced in a single decompression pass

#### Scenario: unrar binary missing
- **WHEN** `use_rar_stream=True` but the `unrar` binary is not available
- **THEN** `PackageNotInstalledError` is raised

### Requirement: RAR errors are translated

The reader SHALL translate `rarfile` exceptions: `BadRarFile`/`NotRarFile` to
`ArchiveCorruptedError`, `RarWrongPassword`/`PasswordRequired` to
`ArchiveEncryptedError`, `NoCrypto` to `PackageNotInstalledError`, and
seek-related `io.UnsupportedOperation` to `ArchiveStreamNotSeekableError`.

#### Scenario: Corrupted RAR
- **WHEN** `rarfile` raises `BadRarFile`
- **THEN** `ArchiveCorruptedError` is raised
