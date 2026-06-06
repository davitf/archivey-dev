# Filesystem Extraction Specification

## Purpose

Define how members are written to the filesystem during `extract()`/
`extractall()`: directory, file, symlink, and hardlink creation; overwrite-mode
enforcement; metadata (timestamps, permissions, Windows attributes) application;
deferred extraction for readers that extract in batches; and the handling of
duplicate/hardlinked targets. This behavior is implemented by the shared
extraction helper.

## Requirements

### Requirement: Output paths are computed under the destination root

Each member SHALL be written to the normalized join of the destination root and
the member's (filter-sanitized) filename.

#### Scenario: Path under root
- **WHEN** a member `a/b.txt` is extracted to root `out`
- **THEN** it is written to `out/a/b.txt`

### Requirement: Overwrite mode governs existing targets

When a target path already exists, the configured `overwrite_mode` SHALL apply:
`OVERWRITE` removes/replaces the existing file, `SKIP` skips the member and
records it as not extracted, and `ERROR` raises `ArchiveFileExistsError`.
Overwriting one directory with another SHALL always be allowed.

#### Scenario: Error mode on existing file
- **WHEN** a target file exists and `overwrite_mode` is `ERROR`
- **THEN** `ArchiveFileExistsError` is raised

#### Scenario: Skip mode on existing file
- **WHEN** a target file exists and `overwrite_mode` is `SKIP`
- **THEN** the member is not written and is recorded as a skipped extraction

#### Scenario: Overwrite mode replaces file
- **WHEN** a target file exists and `overwrite_mode` is `OVERWRITE`
- **THEN** the existing file is removed and the member is written

#### Scenario: Type conflict between dir and file
- **WHEN** a member is a directory but the existing target is a file (or vice
  versa)
- **THEN** `ArchiveFileExistsError` is raised

### Requirement: Directories, files, and links are created per type

Directories SHALL be created (including parents). Regular files SHALL be written
by streaming their content, creating parent directories as needed. Symlinks SHALL
be created pointing at their (sanitized) target; hardlinks SHALL be created
linking to the already-extracted target file.

#### Scenario: Regular file written from stream
- **WHEN** a file member with content is extracted
- **THEN** its bytes are written to the target path and the path is recorded

#### Scenario: Symlink created
- **WHEN** a symlink member is extracted
- **THEN** a symbolic link to its target is created at the member's path

#### Scenario: Self-referential link skipped
- **WHEN** a link's resolved target is the link's own path
- **THEN** no link is created and the member is treated as handled

### Requirement: Hardlinks fall back to copying when linking is unavailable

Extraction SHALL fall back to copying the target file's content to the
hardlink's path when creating a hardlink fails (unsupported platform or OS error).

#### Scenario: Hardlink unsupported
- **WHEN** `os.link` fails for a hardlink member
- **THEN** the target file's content is copied to the hardlink path instead

### Requirement: Deferred extraction is supported for batch readers

The helper SHALL record a file member as pending when it is presented without a
stream (the reader extracts in a later batch pass), and SHALL complete it when
the reader reports the extracted file. Hardlinks whose target is not yet
extracted SHALL likewise be deferred until the target exists.

#### Scenario: File extracted in a later pass
- **WHEN** a reader defers a file and later reports it extracted
- **THEN** the helper finalizes the file (moving or copying it to the target
  path) and records the mapping

### Requirement: Member metadata is applied after content is written

After files are written, the helper SHALL apply available metadata: modification
(and access) times, Unix permissions, and the Windows read-only attribute.
Timestamp/permission application on symlinks SHALL occur only on platforms that
support it, and SHALL be skipped for hardlinks. OS errors while applying metadata
SHALL be tolerated without aborting extraction.

#### Scenario: mtime applied
- **WHEN** a file member has a modification time
- **THEN** the extracted file's mtime is set accordingly

#### Scenario: Permissions applied
- **WHEN** a file member has a Unix mode
- **THEN** the extracted file's permissions are set accordingly

#### Scenario: Windows read-only attribute applied
- **WHEN** a member's Windows attributes include the read-only bit
- **THEN** write permission is removed from the extracted file

### Requirement: extractall reports written and failed members

Extraction SHALL return a mapping from written filesystem paths to their
`ArchiveMember` objects, and SHALL track members that failed or were skipped so
callers and readers can inspect them.

#### Scenario: Path-to-member mapping
- **WHEN** extraction completes
- **THEN** a mapping of each written path to its member is available
