# Extraction Filters Specification

## Purpose

Define the sanitization filters that protect against unsafe archive members
during iteration and extraction: the built-in `ExtractionFilter` policies
(`FULLY_TRUSTED`, `TAR`, `DATA`), the rules they enforce, and the `create_filter`
factory for custom policies. Filters mirror Python's `tarfile` named filters.

## Requirements

### Requirement: Three built-in filter policies are provided

Archivey SHALL provide `ExtractionFilter.FULLY_TRUSTED` (no checks),
`ExtractionFilter.TAR` (path and permission sanitization), and
`ExtractionFilter.DATA` (the default; stricter, also blocking special files and
unsafe ownership/permissions). `DATA` SHALL be the default `extraction_filter`.

#### Scenario: Fully trusted passes members unchanged
- **WHEN** the `FULLY_TRUSTED` filter is applied to a member
- **THEN** the member is returned unchanged

### Requirement: Filters reject paths that escape the destination

The `TAR` and `DATA` filters SHALL reject absolute paths and paths containing
`..` components, and SHALL reject any target that would resolve outside the
destination directory, by raising `ArchiveFilterError`.

#### Scenario: Absolute path rejected
- **WHEN** a member's name is an absolute path and a sanitizing filter is applied
- **THEN** `ArchiveFilterError` is raised

#### Scenario: Parent-traversal rejected
- **WHEN** a member's name contains `..` that escapes the archive root
- **THEN** `ArchiveFilterError` is raised

#### Scenario: Leading slashes stripped
- **WHEN** a member name has leading `/` or `\` characters but is otherwise safe
- **THEN** the name is normalized to a relative path inside the destination

### Requirement: Filters sanitize link targets

For symlinks, the `TAR` and `DATA` filters SHALL verify the target (resolved
relative to the link's own directory) stays inside the archive root; for
hardlinks, the target SHALL be checked as a path inside the root. Unsafe targets
SHALL raise `ArchiveFilterError`.

#### Scenario: Symlink escaping the root rejected
- **WHEN** a symlink target resolves outside the archive root
- **THEN** `ArchiveFilterError` is raised

### Requirement: Filters sanitize permissions

When a member has a mode, the `TAR` filter SHALL strip setuid/setgid/sticky and
group/other write bits. The `DATA` filter SHALL additionally remove executable
bits and force owner read/write on regular files, and SHALL drop the explicit
mode on directories.

#### Scenario: Data filter de-escalates a file mode
- **WHEN** the `DATA` filter is applied to an executable regular file
- **THEN** the resulting member's mode has the executable bits removed and owner
  read/write set

### Requirement: The data filter blocks special files and strips ownership

The `DATA` filter SHALL reject members of type `OTHER` (special files) with
`ArchiveFilterError`, and SHALL clear `uid`, `gid`, `uname`, and `gname` so
extracted files are owned by the extracting user.

#### Scenario: Special file rejected by data filter
- **WHEN** the `DATA` filter is applied to a device or other special member
- **THEN** `ArchiveFilterError` is raised

#### Scenario: Ownership cleared by data filter
- **WHEN** the `DATA` filter is applied to a member with uid/gid set
- **THEN** the resulting member has `uid`, `gid`, `uname`, and `gname` set to `None`

### Requirement: create_filter builds custom policies

`create_filter()` SHALL build a filter from boolean options (`for_data`,
`sanitize_names`, `sanitize_link_targets`, `sanitize_permissions`,
`raise_on_error`). When `raise_on_error` is `False`, a rejected member SHALL be
skipped (the filter returns `None`) and logged instead of raising.

#### Scenario: Non-raising filter skips unsafe members
- **WHEN** a filter created with `raise_on_error=False` encounters an unsafe member
- **THEN** the filter returns `None` (skipping the member) and logs a warning

### Requirement: Filters are callable with one or two arguments

A filter SHALL be callable as `filter(member)` (iteration) or
`filter(member, dest_path)` (extraction), returning a possibly-modified
`ArchiveMember` or `None` to skip it. When a destination path is provided,
out-of-destination checks SHALL use it.

#### Scenario: Iteration-style call
- **WHEN** a filter is called with only a member during iteration
- **THEN** it returns the sanitized member or `None`
