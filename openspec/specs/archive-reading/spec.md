# Archive Reading Specification

## Purpose

Define the uniform `ArchiveReader` interface that all format readers expose, and
the shared behavior provided by the base reader: member registration and
identity, member listing, sequential iteration with per-member streams, random
access (`open`/`extract`/`extractall`), link resolution, streaming-mode
restrictions, and resource lifecycle. This is the format-independent contract;
format-specific behavior is defined in the per-format capabilities.

## Requirements

### Requirement: ArchiveReader is a context manager that releases resources

`ArchiveReader` SHALL support the context-manager protocol and SHALL close all
underlying resources (the archive object and any opened member streams) on exit.
`close()` SHALL be idempotent.

#### Scenario: Use as context manager
- **WHEN** a reader is used in a `with` block
- **THEN** `close()` is called automatically when the block exits

#### Scenario: Idempotent close
- **WHEN** `close()` is called more than once
- **THEN** no error is raised

### Requirement: Members have a stable identity within the archive

Each registered `ArchiveMember` SHALL be assigned a sequential `member_id` in
archive order and an `archive_id` identifying the archive it belongs to. These
SHALL be usable to disambiguate members that share a filename and to preserve
ordering.

#### Scenario: Sequential member ids
- **WHEN** members are registered while reading an archive
- **THEN** each member receives a `member_id` increasing in archive order

#### Scenario: Duplicate filenames are distinguishable
- **WHEN** two members share the same filename
- **THEN** they have distinct `member_id` values

### Requirement: get_members returns the full member list

`get_members()` SHALL return the complete list of `ArchiveMember` objects,
reading the whole archive if necessary. It SHALL raise `ValueError` when the
archive was opened in streaming mode.

#### Scenario: Full listing
- **WHEN** `get_members()` is called on a randomly-accessible archive
- **THEN** all members are returned as `ArchiveMember` objects

#### Scenario: get_members in streaming mode
- **WHEN** `get_members()` is called on an archive opened with `streaming=True`
- **THEN** a `ValueError` is raised

### Requirement: get_members_if_available avoids full traversal

`get_members_if_available()` SHALL return the member list when it can be obtained
without scanning or decompressing the whole archive (e.g. from a central
directory), and SHALL return `None` otherwise (e.g. a not-yet-iterated tar
stream).

#### Scenario: Central-directory format
- **WHEN** `get_members_if_available()` is called on a ZIP archive
- **THEN** the member list is returned without reading all file data

#### Scenario: Streaming tar before iteration
- **WHEN** `get_members_if_available()` is called on a streaming tar that has not
  been iterated and whose reader does not support an upfront member list
- **THEN** `None` is returned

### Requirement: iter_members_with_streams yields members with lazy streams

`iter_members_with_streams()` SHALL iterate members, yielding
`(ArchiveMember, stream)` tuples. The stream SHALL be a readable binary stream
for file members and `None` for non-file members. Streams SHALL be opened lazily
and SHALL be closed automatically when iteration advances or the generator is
closed.

#### Scenario: File member yields a stream
- **WHEN** iterating and the current member is a regular file
- **THEN** the yielded stream reads the member's decompressed content

#### Scenario: Non-file member yields None
- **WHEN** iterating and the current member is a directory or link
- **THEN** the yielded stream is `None`

#### Scenario: Streams auto-close on advance
- **WHEN** iteration advances to the next member without the stream being closed
- **THEN** the previous member's stream is closed automatically

### Requirement: Iteration accepts member selection and a filter

`iter_members_with_streams()` SHALL accept a `members` selector (a collection of
names/`ArchiveMember` objects, or a predicate), a per-call `pwd`, and a `filter`
(an `ExtractionFilter` policy or a callable). Members not selected SHALL be
skipped; filtered-out members SHALL be excluded.

#### Scenario: Predicate selection
- **WHEN** a predicate selecting only `.txt` members is passed
- **THEN** only members for which the predicate returns `True` are yielded

### Requirement: Streaming-mode archives iterate at most once

When opened with `streaming=True`, `iter_members_with_streams()` SHALL be usable
only once; a second iteration attempt SHALL raise `ValueError`.

#### Scenario: Second streaming iteration rejected
- **WHEN** `iter_members_with_streams()` is called a second time on a
  streaming-mode archive
- **THEN** a `ValueError` is raised

### Requirement: open returns a stream for a file member (random access)

`open()` SHALL return a readable binary stream for the given member or filename.
For links, it SHALL resolve to and open the link's target. It SHALL require
random access and raise `ValueError` in streaming mode.

#### Scenario: Open by filename
- **WHEN** `open("dir/file.txt")` is called on a randomly-accessible archive
- **THEN** a readable binary stream of the file's content is returned

#### Scenario: Open a symlink follows the target
- **WHEN** `open()` is called on a symlink that points to a file in the archive
- **THEN** the returned stream reads the target file's content

#### Scenario: Open a non-openable member
- **WHEN** `open()` is called on a directory, special file, or a link whose target
  is not a file in the archive
- **THEN** `ArchiveMemberCannotBeOpenedError` is raised

#### Scenario: Open in streaming mode
- **WHEN** `open()` is called on an archive opened with `streaming=True`
- **THEN** a `ValueError` is raised

### Requirement: get_member resolves names to members

`get_member()` SHALL return the `ArchiveMember` for a filename, or validate and
return a provided `ArchiveMember`. An unknown name SHALL raise
`ArchiveMemberNotFoundError`.

#### Scenario: Lookup by name
- **WHEN** `get_member("a/b.txt")` is called for an existing member
- **THEN** the corresponding `ArchiveMember` is returned

#### Scenario: Unknown name
- **WHEN** `get_member()` is called with a name not in the archive
- **THEN** `ArchiveMemberNotFoundError` is raised

### Requirement: extract writes a single member to disk

`extract()` SHALL extract one member to a target path (defaulting to the current
directory), returning the written path or `None` for non-file entries. It SHALL
require random access.

#### Scenario: Extract one file
- **WHEN** `extract("readme.txt", path="out")` is called
- **THEN** the file is written under `out` and its path is returned

### Requirement: extractall writes selected members to a directory

`extractall()` SHALL extract all or selected members to a target directory
(created if needed), applying the configured or supplied `filter` and
`overwrite_mode`, and SHALL return a mapping from written paths to their
`ArchiveMember`. In streaming mode it SHALL be callable only once.

#### Scenario: Extract all members
- **WHEN** `extractall("out")` is called
- **THEN** all members are written under `out` and a path→member mapping is returned

#### Scenario: Selection and filter applied
- **WHEN** `extractall("out", members=selector, filter="data")` is called
- **THEN** only selected members are extracted, sanitized by the data filter

### Requirement: resolve_link follows links to a final target

`resolve_link()` SHALL return the member itself for non-links; for symlinks and
hardlinks it SHALL follow the chain to the final non-link target within the
archive, detecting cycles. It SHALL return `None` when the target is not present
in the archive or a loop is detected.

#### Scenario: Resolve a hardlink
- **WHEN** `resolve_link()` is called on a hardlink whose target file exists
  earlier in the archive
- **THEN** the target `ArchiveMember` is returned

#### Scenario: Resolve a symlink chain
- **WHEN** a symlink points to another symlink that points to a file
- **THEN** the final file member is returned

#### Scenario: Dangling link
- **WHEN** a link's target is not present in the archive
- **THEN** `None` is returned

#### Scenario: Link cycle
- **WHEN** links form a cycle
- **THEN** `None` is returned

### Requirement: has_random_access reports the access mode

`has_random_access()` SHALL return `True` when random-access methods
(`open`, `get_members`, `extract`) can be used, and `False` when the archive was
opened in streaming mode or from a non-seekable source.

#### Scenario: Random-access archive
- **WHEN** an archive is opened normally from a seekable source
- **THEN** `has_random_access()` returns `True`

#### Scenario: Streaming archive
- **WHEN** an archive is opened with `streaming=True`
- **THEN** `has_random_access()` returns `False`

### Requirement: get_archive_info reports archive-level metadata

`get_archive_info()` SHALL return an `ArchiveInfo` describing the archive's
format, solidity, optional version, comment, and format-specific extras.

#### Scenario: Archive info
- **WHEN** `get_archive_info()` is called
- **THEN** an `ArchiveInfo` with at least the archive `format` is returned

### Requirement: Member streams are opened through a translating wrapper

Streams returned to callers SHALL be wrapped so that exceptions raised while
reading are passed through the reader's `_translate_exception` and surfaced as
`ArchiveError` subclasses, and so that archive/member context is attached.

#### Scenario: Read error during streaming
- **WHEN** a corruption error occurs while reading a member stream
- **THEN** the caller observes an `ArchiveError` subclass carrying the member name
