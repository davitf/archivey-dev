## REMOVED Requirements

### Requirement: 7z requires the py7zr package

**Reason**: Metadata is now parsed by a built-in native 7z header parser. py7zr is
still used for decompression in this phase, so the requirement is restated (below)
rather than dropped outright.

**Migration**: No action for callers using the `optional` extra (py7zr stays
installed for decompression). Reading metadata no longer triggers py7zr private APIs.

## ADDED Requirements

### Requirement: 7z metadata is parsed natively; decompression requires py7zr

The 7z reader SHALL parse archive metadata (member list, folders, solidity,
encryption, comment) with a built-in native header parser that requires no
third-party Python package. Decompression SHALL continue to use py7zr, and the
reader SHALL raise `PackageNotInstalledError` when a member is opened or extracted
but py7zr is not installed.

#### Scenario: Metadata read without py7zr
- **WHEN** a 7z archive is opened and py7zr is not installed
- **THEN** the member list and metadata are still read by the native parser

#### Scenario: Decompression requires py7zr
- **WHEN** a 7z member's content is read but py7zr is not installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: 7z exposes per-member compression method

The reader SHALL populate each member's `compression_method` with a human-readable
name derived from its folder's coder chain (for example `LZMA2`, `LZMA`, `PPMD`, or
a combination such as `LZMA2 + BCJ`).

#### Scenario: LZMA2 member
- **WHEN** a 7z member is stored in an LZMA2-coded folder
- **THEN** its `compression_method` reflects LZMA2 (rather than `None`)

#### Scenario: Filtered LZMA2 member
- **WHEN** a member's folder prepends a BCJ or Delta filter before LZMA2
- **THEN** its `compression_method` reflects the filter and the codec

### Requirement: 7z exposes the archive comment

The reader SHALL surface the archive comment (from `FILES_INFO`) in
`ArchiveInfo.comment` when present, rather than discarding it.

#### Scenario: Archive with a comment
- **WHEN** a 7z archive stores a comment
- **THEN** `get_archive_info().comment` returns it

### Requirement: Unsupported 7z variants raise a clean error

The native parser SHALL raise an `ArchiveError` for multi-volume 7z archives rather
than producing incorrect output, and SHALL handle anti-items without corrupting the
member list.

#### Scenario: Multi-volume 7z
- **WHEN** a multi-volume 7z archive is opened
- **THEN** an `ArchiveError` is raised
