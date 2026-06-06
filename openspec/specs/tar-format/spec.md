# TAR Format Reading Specification

## Purpose

Define how Archivey reads TAR archives (plain and compressed: `.tar.gz`,
`.tar.bz2`, `.tar.xz`, `.tar.zst`, `.tar.lz4`, `.tar.lz`, `.tar.Z`) via the stdlib
`tarfile` module, including streaming vs random access, the integrity check for
silently-truncated archives, metadata mapping, and error translation.

## Requirements

### Requirement: TAR uses streaming or random-access mode based on opening

When opened in streaming mode, the TAR reader SHALL open the archive as a
forward-only stream (`r|`); otherwise it SHALL open in seekable mode (`r:`). In
random-access mode a non-seekable source SHALL raise
`ArchiveStreamNotSeekableError`.

#### Scenario: Streaming tar
- **WHEN** a tar archive is opened with `streaming=True`
- **THEN** it is read as a forward-only stream

#### Scenario: Random-access tar from non-seekable source
- **WHEN** a tar archive is opened with `streaming=False` from a non-seekable
  source
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: Compressed tars are decompressed and treated as solid

For a compressed tar, the reader SHALL open the inner decompression stream for
the detected stream format and SHALL report `is_solid=True` (reading an arbitrary
member may require decompressing earlier data). Plain tars SHALL report
`is_solid=False`.

#### Scenario: tar.gz solidity
- **WHEN** a `.tar.gz` archive is opened
- **THEN** `get_archive_info().is_solid` is `True`

### Requirement: TAR does not support passwords

The TAR reader SHALL reject a non-`None` password by raising `ValueError`, since
the format has no encryption.

#### Scenario: Password on tar
- **WHEN** a tar archive is opened with a password
- **THEN** a `ValueError` is raised

### Requirement: TAR member list requires iteration

Because tar has no central directory, the reader SHALL report that an upfront
member list is not available; `get_members_if_available()` SHALL return `None` in
streaming mode (before iteration) and the full list otherwise.

#### Scenario: No upfront list in streaming mode
- **WHEN** `get_members_if_available()` is called on a streaming tar before
  iteration
- **THEN** `None` is returned

### Requirement: TAR member metadata is mapped from TarInfo

The reader SHALL populate `filename` (directories with trailing `/`),
`file_size`, modification time as UTC, `type` (file/dir/symlink/hardlink/other),
`mode`, `uid`/`gid`, `uname`/`gname`, and `link_target` for links.

#### Scenario: Hardlink classification
- **WHEN** a tar entry is a hard link
- **THEN** its `type` is `HARDLINK` and `link_target` is the linked name

#### Scenario: UTC modification time
- **WHEN** a tar entry has a modification time
- **THEN** `mtime_with_tz` is timezone-aware in UTC

### Requirement: Integrity check detects silent truncation

The reader SHALL verify, when `tar_check_integrity` is `True` (default) and after
iterating all members, that the archive ends with the expected trailing zero
blocks, raising `ArchiveCorruptedError` if they are missing or corrupt — guarding
against `tarfile` silently stopping at a corrupted metadata section.

#### Scenario: Truncated tar detected
- **WHEN** a tar archive is corrupted such that `tarfile` stops early, with
  `tar_check_integrity=True`
- **THEN** `ArchiveCorruptedError` is raised

#### Scenario: Integrity check disabled
- **WHEN** `tar_check_integrity=False`
- **THEN** no trailing-block verification is performed

### Requirement: TAR errors are translated

The reader SHALL translate `tarfile.ReadError` indicating unexpected end-of-data
to `ArchiveEOFError`, and other `tarfile.ReadError`s to `ArchiveCorruptedError`.

#### Scenario: Unexpected end of tar data
- **WHEN** `tarfile` reports unexpected end of data
- **THEN** `ArchiveEOFError` is raised
