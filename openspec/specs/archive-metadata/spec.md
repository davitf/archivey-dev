# Archive Metadata Model Specification

## Purpose

Define the data model that Archivey presents to callers regardless of source
format: the `ArchiveFormat`/`ContainerFormat`/`StreamFormat` enums, the
`MemberType` and `CreateSystem` enums, and the `ArchiveMember` and `ArchiveInfo`
dataclasses, including normalization rules and compatibility properties.

## Requirements

### Requirement: Archive format is a container/stream pair

`ArchiveFormat` SHALL be an immutable pair of a `ContainerFormat` and a
`StreamFormat`, and SHALL provide named constants for all supported combinations
(e.g. `ZIP`, `TAR_GZ`, `GZIP`, `ISO`, `FOLDER`, `UNKNOWN`). It SHALL produce a
human-readable file extension via `file_extension()`/`str()`.

#### Scenario: Compressed tar format
- **WHEN** the format is `ArchiveFormat.TAR_GZ`
- **THEN** its container is `TAR`, its stream is `GZIP`, and its extension is
  `tar.gz`

#### Scenario: Single-file stream format
- **WHEN** the format is `ArchiveFormat.GZIP`
- **THEN** its container is `RAW_STREAM` and its extension is `gz`

### Requirement: Member types are enumerated

`MemberType` SHALL enumerate `FILE`, `DIR`, `SYMLINK`, `HARDLINK`, and `OTHER`,
covering every member a reader can report.

#### Scenario: Type classification
- **WHEN** a member is a symbolic link
- **THEN** its `type` is `MemberType.SYMLINK`

### Requirement: Filenames are normalized

`ArchiveMember.filename` SHALL use forward slashes as separators, and directory
names SHALL end with a trailing `/`. The unmodified name, when the format stores
one separately, SHALL be available in `raw_filename`.

#### Scenario: Directory trailing slash
- **WHEN** a member represents a directory
- **THEN** its `filename` ends with `/`

#### Scenario: Backslashes normalized
- **WHEN** a source stores a path with backslashes
- **THEN** the member's `filename` uses forward slashes

### Requirement: ArchiveMember records optional metadata fields

`ArchiveMember` SHALL provide optional fields for size (`file_size`,
`compress_size`), timestamps (`mtime_with_tz`, `atime`, `ctime`), ownership and
permissions (`mode`, `uid`, `gid`, `uname`, `gname`), integrity (`crc32`),
`compression_method`, `comment`, `create_system`, `windows_attrs`, `encrypted`,
`link_target`, and a `raw_info` reference to the underlying library object, each
populated when the format provides it.

#### Scenario: Unknown field is None
- **WHEN** a format does not record a field (e.g. no CRC)
- **THEN** that field on the member is `None` (or `False`/empty as appropriate)

### Requirement: Modification time exposes timezone-aware and naive forms

`mtime_with_tz` SHALL carry timezone information when the format uses global
time, or be naive when the format uses local time. The `mtime` property SHALL
return the same instant without timezone information for compatibility.

#### Scenario: UTC-based format
- **WHEN** a format stores modification times in UTC
- **THEN** `mtime_with_tz` is timezone-aware and `mtime` returns the naive value

### Requirement: Convenience and compatibility properties are provided

`ArchiveMember` SHALL provide `is_file`, `is_dir`, `is_link`, `is_other`, and
`is_junction` predicates, plus zipfile-compatibility members `date_time` and
`CRC` (an alias of `crc32`).

#### Scenario: is_link for hardlink
- **WHEN** a member's type is `HARDLINK` or `SYMLINK`
- **THEN** `is_link` returns `True`

#### Scenario: date_time tuple
- **WHEN** a member has a modification time
- **THEN** `date_time` returns a `(year, month, day, hour, minute, second)` tuple

#### Scenario: Junction detection
- **WHEN** a member is a Windows NTFS junction (a symlink flagged as a junction)
- **THEN** `is_junction` returns `True`

### Requirement: Members can be copied with filter edits tracked

`ArchiveMember.replace(**kwargs)` SHALL return a copy with the given fields
updated and SHALL mark the copy as edited by a filter, leaving the original
unchanged.

#### Scenario: Filter edits a member
- **WHEN** a filter calls `member.replace(filename=new_name)`
- **THEN** a new member with the updated filename is returned and the original is
  unchanged

### Requirement: ArchiveInfo records archive-level metadata

`ArchiveInfo` SHALL provide the archive `format`, an optional `version`, an
`is_solid` flag (whether reading one member may require decompressing earlier
ones), an optional `comment`, and an `extra` dictionary for format-specific data.

#### Scenario: Solid archive
- **WHEN** an archive stores members in a shared compression block
- **THEN** `ArchiveInfo.is_solid` is `True`
