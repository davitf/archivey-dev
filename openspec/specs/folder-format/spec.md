# Folder Reading Specification

## Purpose

Define how Archivey presents an on-disk directory as an archive via the folder
reader, so the same `ArchiveReader` interface works for real directories:
recursive traversal, member metadata from the filesystem, hardlink and symlink
detection, and safe member opening confined to the folder.

## Requirements

### Requirement: Folders are opened from a filesystem path only

The folder reader SHALL operate on a directory path and SHALL NOT support being
opened from a stream. It SHALL reject a non-`None` password with `ValueError` and
report `is_solid=False`.

#### Scenario: Password on folder
- **WHEN** a folder is opened with a password
- **THEN** a `ValueError` is raised

### Requirement: Folder contents are traversed deterministically

The reader SHALL walk the directory tree top-down without following symlinks,
yielding directories before the files they contain, in sorted order.

#### Scenario: Deterministic ordering
- **WHEN** a folder with nested directories and files is read
- **THEN** entries are produced in a stable, sorted order with directories first

### Requirement: Folder member metadata comes from the filesystem

The reader SHALL populate filename (relative, forward-slash, directories with
trailing `/`), `file_size`, modification time (UTC), `type`, `mode`, ownership
(`uid`/`gid`/`uname`/`gname` where available), and `link_target` for symlinks.

#### Scenario: Symlink target
- **WHEN** a directory entry is a symbolic link
- **THEN** its `type` is `SYMLINK` and `link_target` is read via the OS

### Requirement: Repeated inodes are reported as hardlinks

The reader SHALL classify a later entry as a `HARDLINK` when its inode has
already been seen in the traversal, with the target being the first entry having
that inode.

#### Scenario: Hardlinked files
- **WHEN** two paths share the same inode
- **THEN** the second is reported as a `HARDLINK` pointing at the first

### Requirement: Opening a member is confined to the folder

When opening a member, the reader SHALL resolve the path within the folder root
and SHALL raise `ArchiveMemberNotFoundError` if it would escape the root or does
not exist.

#### Scenario: Escaping path rejected
- **WHEN** a resolved member path falls outside the folder root
- **THEN** `ArchiveMemberNotFoundError` is raised

#### Scenario: Missing entry
- **WHEN** opening a member that no longer exists on disk
- **THEN** `ArchiveMemberNotFoundError` is raised
