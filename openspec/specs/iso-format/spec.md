# ISO 9660 Format Reading Specification

## Purpose

Define how Archivey reads ISO 9660 optical-disc images via the `pycdlib`
package, including namespace selection (Rock Ridge / Joliet / plain ISO 9660),
metadata mapping, symlink handling, and error translation.

## Requirements

### Requirement: ISO requires the pycdlib package

The ISO reader SHALL raise `PackageNotInstalledError` when `pycdlib` is not
installed.

#### Scenario: pycdlib missing
- **WHEN** an ISO image is opened and `pycdlib` is not installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: ISO requires a seekable source and no password

The ISO reader SHALL raise `ArchiveStreamNotSeekableError` for non-seekable
sources, and SHALL raise `ValueError` if a password is supplied (ISO has no
encryption).

#### Scenario: Non-seekable ISO source
- **WHEN** an ISO image is opened from a non-seekable stream
- **THEN** `ArchiveStreamNotSeekableError` is raised

#### Scenario: Password on ISO
- **WHEN** an ISO image is opened with a password
- **THEN** a `ValueError` is raised

### Requirement: ISO selects the richest available namespace

The reader SHALL choose names and metadata from the richest available extension:
Rock Ridge, then Joliet, then plain ISO 9660. Plain ISO names SHALL have the
`;1` version suffix stripped.

#### Scenario: Rock Ridge preferred
- **WHEN** an ISO image has Rock Ridge extensions
- **THEN** member names and POSIX metadata come from Rock Ridge

#### Scenario: Joliet fallback
- **WHEN** an ISO has Joliet but no Rock Ridge
- **THEN** member names come from the Joliet namespace

### Requirement: ISO member metadata is mapped from directory records

The reader SHALL populate filename (directories with trailing `/`), `file_size`,
modification time (preferring Rock Ridge time fields, otherwise the directory
record date), `type` (file/dir/symlink), `mode`/`uid`/`gid` from Rock Ridge POSIX
entries, and `link_target` from Rock Ridge symlink entries. `crc32` SHALL be
`None` and `compression_method` SHALL indicate stored (uncompressed) data.

#### Scenario: Rock Ridge symlink
- **WHEN** a member has a Rock Ridge symlink entry
- **THEN** its `type` is `SYMLINK` and `link_target` is the decoded target

#### Scenario: Stored data
- **WHEN** any ISO member is listed
- **THEN** its `crc32` is `None` and its data is reported as uncompressed

### Requirement: ISO reports volume metadata

`get_archive_info()` SHALL expose the interchange level as version, the volume
identifier as comment, and Rock Ridge / Joliet / UDF presence and the system
identifier in `extra`.

#### Scenario: Rock Ridge presence reported
- **WHEN** an ISO has Rock Ridge extensions
- **THEN** `get_archive_info().extra` indicates Rock Ridge

### Requirement: ISO errors are translated

The reader SHALL translate `pycdlib` exceptions to `ArchiveCorruptedError`.

#### Scenario: Corrupted ISO
- **WHEN** `pycdlib` raises a parsing exception
- **THEN** `ArchiveCorruptedError` is raised
