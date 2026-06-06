# Command-Line Interface Specification

## Purpose

Define the `archivey` command-line tool (also runnable as `python -m archivey`),
which lists, verifies, and extracts archives. The CLI is primarily a testing and
exploration aid built on the public API, and also reports installed optional
dependencies.

## Requirements

### Requirement: The CLI operates in list, test, or extract mode

The CLI SHALL accept one or more archive files and a mode: `--list` (list members
without verifying), `--test` (default; list members and verify checksums), or
`--extract` (write members to disk). It SHALL print the detected format and
archive info for each file.

#### Scenario: Default test mode
- **WHEN** `archivey file.zip` is run with no mode flag
- **THEN** members are listed and their checksums are verified

#### Scenario: Extract mode
- **WHEN** `archivey --extract --dest out file.zip` is run
- **THEN** the archive's members are extracted under `out`

### Requirement: Member output shows type, size, checksum, time, and name

For each member the CLI SHALL print a line including an encryption marker, size,
a permission/type string, checksum, modification time, and the filename; links
SHALL show their target and other members SHALL show their type.

#### Scenario: Listing a file member
- **WHEN** a regular file member is listed
- **THEN** its line includes size, mode/type, modification time, and filename

#### Scenario: Listing a link member
- **WHEN** a symlink member is listed
- **THEN** its line shows the link target

### Requirement: The CLI exposes configuration and streaming options

The CLI SHALL provide flags mapping to configuration: `--use-rar-stream`,
`--use-rapidgzip`, `--use-indexed-bzip2`, `--use-stored-metadata`,
`--overwrite-mode`, a `--password`, and `--stream` for single-pass iteration.

#### Scenario: Streaming iteration
- **WHEN** `archivey --stream file.tar.gz` is run
- **THEN** the archive is processed in a single sequential pass

### Requirement: Member names can be filtered by shell patterns

Patterns supplied after a `--` separator SHALL filter members by fnmatch-style
matching; a member SHALL be included when it matches any pattern.

#### Scenario: Pattern filter
- **WHEN** `archivey --list file.zip -- "*.txt"` is run
- **THEN** only members whose names match `*.txt` are listed

### Requirement: The CLI reports version and dependency information

`--version` SHALL print the Archivey version and the detected versions (or
absence) of optional dependencies, including the Python version and the `unrar`
binary.

#### Scenario: Version output
- **WHEN** `archivey --version` is run
- **THEN** the Archivey version and optional-dependency versions are printed

### Requirement: Archive errors are reported without aborting other files

When processing a file raises an `ArchiveError`, the CLI SHALL report the error
for that file and continue with the remaining files.

#### Scenario: One corrupted archive among several
- **WHEN** several archives are passed and one raises an `ArchiveError`
- **THEN** the error is reported and the other archives are still processed
