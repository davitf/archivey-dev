# Single-File Compressed Reading Specification

## Purpose

Define how Archivey presents a standalone compressed file (gzip, bzip2, xz, zstd,
lz4, lzip, zlib, brotli, Unix compress) as an archive containing exactly one
member, including the member's name derivation, optional stored-metadata
extraction, and size reporting.

## Requirements

### Requirement: A compressed file is an archive with one member

The single-file reader SHALL expose exactly one member representing the
decompressed content of the input. It SHALL reject a non-`None` password with
`ValueError`.

#### Scenario: Single member
- **WHEN** a `.gz` file is opened via `open_archive`
- **THEN** the archive has exactly one member whose stream yields the
  decompressed data

#### Scenario: Password rejected
- **WHEN** a single compressed file is opened with a password
- **THEN** a `ValueError` is raised

### Requirement: The member name is derived from the source

When opened from a path, the member name SHALL be the source base name with the
recognized compression extension removed (or with `.uncompressed` appended when
the extension is unknown). When opened from a stream, the member name SHALL be
`uncompressed`.

#### Scenario: Strip known extension
- **WHEN** `data.txt.gz` is opened from a path
- **THEN** the member's filename is `data.txt`

#### Scenario: Stream source name
- **WHEN** a compressed stream (no path) is opened
- **THEN** the member's filename is `uncompressed`

### Requirement: Stored gzip metadata can populate the member

The reader SHALL read the gzip header's stored original filename and modification
time and use them for the member when `use_single_file_stored_metadata` is `True`
and the source is a seekable gzip file.

#### Scenario: Use stored gzip filename
- **WHEN** a gzip file stores an original filename and
  `use_single_file_stored_metadata=True`
- **THEN** the member's filename comes from the gzip header

### Requirement: Decompressed size is reported when cheaply available

The reader SHALL populate the member's `file_size` for formats whose decompressed
size can be determined without full decompression on a seekable source (xz and
lzip).

#### Scenario: xz size known
- **WHEN** a seekable `.xz` file is opened
- **THEN** the single member's `file_size` is the decompressed size
