# Format Detection Specification

## Purpose

Define how Archivey determines an archive's `ArchiveFormat` from its content
(magic-byte signatures), its filename extension, content heuristics for formats
without fixed signatures, and self-extracting executables — and how conflicts
between content- and name-based detection are resolved.

## Requirements

### Requirement: Directories are detected as the folder format

When the input is a path that points to an existing directory, detection SHALL
return `ArchiveFormat.FOLDER`.

#### Scenario: Directory path
- **WHEN** `detect_archive_format(path)` is called and `path` is a directory
- **THEN** `ArchiveFormat.FOLDER` is returned

### Requirement: Formats are detected by magic-byte signature

Detection SHALL identify formats by reading magic bytes at known offsets,
including ZIP, RAR4/RAR5, 7z, gzip, bzip2, xz, zstd, lz4, lzip, zlib, Unix
compress, TAR (`ustar` at offset 257), and ISO 9660 (volume descriptor magics at
offset 0x8001).

#### Scenario: ZIP signature
- **WHEN** the input begins with the bytes `PK\x03\x04`
- **THEN** the detected format is `ArchiveFormat.ZIP`

#### Scenario: RAR4 and RAR5 signatures
- **WHEN** the input begins with the RAR4 or RAR5 signature
- **THEN** the detected format is `ArchiveFormat.RAR`

#### Scenario: ustar TAR signature
- **WHEN** the bytes `ustar` appear at offset 257
- **THEN** the detected format is `ArchiveFormat.TAR`

#### Scenario: ISO 9660 signature
- **WHEN** an ISO volume-descriptor magic (e.g. `CD001`) appears at offset 0x8001
- **THEN** the detected format is `ArchiveFormat.ISO`

### Requirement: Brotli and signature-less tar are detected heuristically

For formats without a usable fixed signature, detection SHALL fall back to
content heuristics: attempting a small Brotli decompression, and attempting to
open the input as a tar stream (reading only the first member header).

#### Scenario: Brotli stream
- **WHEN** no fixed signature matches and a small prefix decompresses as Brotli
- **THEN** the detected format is `ArchiveFormat.BROTLI`

#### Scenario: Non-ustar tar
- **WHEN** no fixed signature matches but the input opens as a tar stream
- **THEN** the detected format is `ArchiveFormat.TAR`

### Requirement: Compressed tar archives are detected when enabled

Detection SHALL, when `detect_compressed_tar` is `True` and a single-file
compression format is detected, decompress the stream and, if the decompressed
content is a tar archive, return the corresponding TAR container with that stream
format (e.g. `TAR_GZ` instead of `GZIP`).

#### Scenario: gzip-compressed tar
- **WHEN** the content is gzip-compressed and the decompressed data is a tar
  archive, with `detect_compressed_tar=True`
- **THEN** the detected format is `ArchiveFormat.TAR_GZ`

#### Scenario: Plain gzip file
- **WHEN** the content is gzip-compressed but the decompressed data is not a tar
  archive
- **THEN** the detected format is `ArchiveFormat.GZIP`

### Requirement: Self-extracting RAR archives are detected

Detection SHALL probe registered SFX detectors and return `ArchiveFormat.RAR`
when no other format matches, the input begins with an executable magic (PE, ELF,
Mach-O, or `#!`), and the executable embeds a RAR archive.

#### Scenario: RAR SFX executable
- **WHEN** the input is an executable that embeds a RAR archive
- **THEN** the detected format is `ArchiveFormat.RAR`

### Requirement: Filenames are detected by extension

`detect_archive_format_by_filename()` SHALL map known extensions (including
compound tar extensions like `.tar.gz`, `.tgz`, `.tbz2`, `.txz`, `.tzst`, and
single-file extensions like `.gz`, `.xz`, `.zst`, `.lz`, `.zz`, `.br`, `.Z`) to
their `ArchiveFormat`.

#### Scenario: Compound tar extension
- **WHEN** a filename ends with `.tar.gz`
- **THEN** the format is `ArchiveFormat.TAR_GZ`

#### Scenario: Unknown extension
- **WHEN** a filename has no recognized extension
- **THEN** `ArchiveFormat.UNKNOWN` is returned

### Requirement: Signature detection takes precedence over filename, with warnings

`detect_archive_format()` SHALL prefer the signature-based result. When signature
and filename disagree, or when only one source yields a result, it SHALL log a
warning and return the signature result when available, otherwise the filename
result.

#### Scenario: Both unknown
- **WHEN** neither signature nor filename yields a format
- **THEN** `ArchiveFormat.UNKNOWN` is returned and a warning is logged

#### Scenario: Signature unknown, filename known
- **WHEN** the signature is unknown but the filename maps to a format
- **THEN** the filename-based format is returned and a warning is logged

#### Scenario: Signature and filename disagree
- **WHEN** the signature indicates one format and the extension indicates another
- **THEN** the signature-based format is returned and a mismatch warning is logged

### Requirement: Detection restores the stream position

After inspecting a stream, detection SHALL leave the stream usable for the caller
(it does not consume the input destructively); when reading from a stream the
position is reset to the start after probing.

#### Scenario: Stream remains readable after detection
- **WHEN** detection reads magic bytes from a seekable stream
- **THEN** the stream is seeked back so subsequent reads start at the beginning
