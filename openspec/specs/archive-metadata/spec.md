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

#### Scenario: Link member filename is the link's own path
- **WHEN** a member is a symlink or hardlink
- **THEN** its `filename` is the link's own path (without a trailing `/`, since
  its type is not `DIR`) and its target is reported separately in `link_target`

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

### Requirement: Convenience type predicates are provided

`ArchiveMember` SHALL provide the boolean predicates `is_file`, `is_dir`,
`is_link` (true for either symlink or hardlink), `is_other`, and `is_junction`.

#### Scenario: is_link for hardlink
- **WHEN** a member's type is `HARDLINK` or `SYMLINK`
- **THEN** `is_link` returns `True`

#### Scenario: Junction detection
- **WHEN** a member is a Windows NTFS junction
- **THEN** its `type` is `MemberType.SYMLINK`, `extra["is_junction"]` is `True`,
  and `is_junction` returns `True`

### Requirement: zipfile-compatible accessors are provided

`ArchiveMember` SHALL provide accessors that mirror Python's `zipfile.ZipInfo`
so that code written against `zipfile` keeps working: `date_time` (a
`(year, month, day, hour, minute, second)` tuple) and `CRC` (an alias of
`crc32`). The directly-named fields `filename`, `file_size`, `compress_size`,
`comment`, and `create_system` SHALL also carry the same meaning as the
corresponding `ZipInfo` attributes.

#### Scenario: date_time tuple
- **WHEN** a member has a modification time
- **THEN** `date_time` returns a `(year, month, day, hour, minute, second)` tuple

#### Scenario: CRC alias
- **WHEN** a member has a known `crc32`
- **THEN** `CRC` returns the same value as `crc32`

#### Scenario: date_time without a modification time
- **WHEN** a member has no modification time
- **THEN** `date_time` returns `None`

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
The `version` field SHALL be a format-dependent string identifying the archive
format version when known (for example `"4"` or `"5"` for RAR4/RAR5, or the
ISO 9660 interchange level), and `None` when the format has no meaningful version.

#### Scenario: Solid archive
- **WHEN** an archive stores members in a shared compression block
- **THEN** `ArchiveInfo.is_solid` is `True`

#### Scenario: Format-dependent version
- **WHEN** a RAR5 archive's info is read
- **THEN** `ArchiveInfo.version` is `"5"`
