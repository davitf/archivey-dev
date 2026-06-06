# Seekable Decompressor Stream Specification

## Purpose

Define the reusable decompressor-stream base classes that turn a stateful,
forward-only decompressor into a readable, seekable binary stream with a
seek-point index for efficient random access. This underlies the zlib, brotli,
lzip, and xz native backends. It defines the shared read/seek/size model;
format-specific block/member handling is defined in the per-format stream specs.

## Requirements

### Requirement: DecompressorStream exposes a readable binary interface

`DecompressorStream` SHALL provide `read(n)` (n bytes, or all to EOF when n < 0),
`readinto`, and `readall`, buffering decompressed output in fixed-size chunks and
tracking the current decompressed position.

#### Scenario: Read all content
- **WHEN** `read(-1)` is called
- **THEN** the entire decompressed content is returned and end-of-stream is
  reached

#### Scenario: Partial reads concatenate correctly
- **WHEN** successive `read(n)` calls are made
- **THEN** their concatenation equals the full decompressed content

### Requirement: Seeking is supported via a seek-point index

When the underlying source is seekable, the stream SHALL support `seek()` with
`SEEK_SET`, `SEEK_CUR`, and `SEEK_END`, and `tell()`. Backward seeks SHALL
restart decompression from the nearest preceding seek point and read forward to
the target; forward seeks SHALL skip intervening data.

#### Scenario: Seek backward then read
- **WHEN** the stream seeks backward to an earlier position and reads
- **THEN** the bytes returned match that position in the decompressed content

#### Scenario: Seek forward skips data
- **WHEN** the stream seeks forward past the current position
- **THEN** intervening bytes are skipped and reading resumes at the target

### Requirement: SEEK_END resolves the total size via the index

`seek(0, SEEK_END)` SHALL determine the total decompressed size, building the
seek-point index when necessary, and SHALL position the stream at the end.

#### Scenario: Seek to end
- **WHEN** `seek(0, SEEK_END)` is called
- **THEN** the returned position equals the total decompressed size

### Requirement: Non-seekable sources degrade to sequential reading

When the underlying source is not seekable, the stream SHALL still read
sequentially to EOF, SHALL report `seekable()` as `False`, and SHALL raise
`io.UnsupportedOperation` from `seek()`.

#### Scenario: Sequential-only mode
- **WHEN** the source is non-seekable
- **THEN** `read()` returns the full content but `seek()` raises
  `io.UnsupportedOperation`

### Requirement: Truncated input raises ArchiveEOFError

The stream SHALL raise `ArchiveEOFError` when the source ends before the
decompressor reports completion.

#### Scenario: Truncated stream
- **WHEN** the compressed source ends while the decompressor is mid-stream
- **THEN** `ArchiveEOFError` is raised

### Requirement: Segmented formats register per-segment seek points

`_SegmentedDecompressorStream` SHALL track compressed and decompressed cursors
across multiple segments (lzip members or xz streams/blocks), registering a seek
point at each completed segment during forward reads and supporting a shared
backward index scan to build the full index without decompressing.

#### Scenario: Seek point per completed segment
- **WHEN** a multi-segment stream is read forward and a segment completes
- **THEN** a seek point is registered at that segment boundary

### Requirement: zlib and brotli streams are seekable decompressors

The zlib and brotli backends SHALL be implemented as `DecompressorStream`
subclasses, providing seekable, readable decompression of their formats.

#### Scenario: zlib stream reads correctly
- **WHEN** a zlib-compressed stream is read
- **THEN** it returns the original uncompressed content
