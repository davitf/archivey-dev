# Lzip Decompressor Stream Specification

## Purpose

Define `LzipDecompressorStream`, the native lzip backend built on stdlib `lzma`.
It decompresses single- and multi-member lzip files, validates per-member
trailers (CRC32 and decompressed size), and supports random access via a backward
index scan over member trailers.

## Requirements

### Requirement: Single- and multi-member lzip files read correctly

The stream SHALL decompress a lzip file consisting of one or more concatenated
members, producing all members' content concatenated in order.

#### Scenario: Multi-member read
- **WHEN** a lzip file with two members is read fully
- **THEN** the output is both members' decompressed content concatenated in order

### Requirement: Member trailers are validated

For each member, the stream SHALL verify the trailing CRC32 against the
decompressed data and verify the stored data size, raising
`ArchiveCorruptedError` on a mismatch.

#### Scenario: CRC mismatch
- **WHEN** a member's stored CRC32 does not match its decompressed data
- **THEN** `ArchiveCorruptedError` is raised

### Requirement: Invalid headers are rejected

The stream SHALL validate each member's `LZIP` magic, version (1), and dictionary
size field, raising `ArchiveCorruptedError` for invalid values.

#### Scenario: Bad magic
- **WHEN** a member does not begin with the `LZIP` magic
- **THEN** `ArchiveCorruptedError` is raised (unless it is trailing non-lzip data;
  see the trailing-data requirement)

### Requirement: Truncated and empty inputs raise the right errors

If the input ends mid-member, the stream SHALL raise `ArchiveEOFError`. If no
complete member is ever seen, it SHALL raise `ArchiveCorruptedError`.

#### Scenario: Truncated member
- **WHEN** the input ends while a member is still being decompressed
- **THEN** `ArchiveEOFError` is raised

#### Scenario: No members
- **WHEN** the input contains no valid lzip member
- **THEN** `ArchiveCorruptedError` is raised

### Requirement: Trailing non-lzip data is tolerated

Data after the last valid member that does not begin with the `LZIP` magic SHALL
be treated as the end of the lzip content (per the lzip specification) rather than
an error.

#### Scenario: Trailing bytes
- **WHEN** valid members are followed by non-`LZIP` trailing bytes
- **THEN** decompression returns only the valid members' content without error

### Requirement: Random access uses a backward trailer scan

The stream SHALL build its seek-point index by scanning member trailers backward
from EOF, enabling `SEEK_END` and member-boundary seeks without decompressing the
whole file. Per-member seek points SHALL also be registered during forward reads.

#### Scenario: Seek to end without decompressing
- **WHEN** `seek(0, SEEK_END)` is called on a multi-member lzip file
- **THEN** the total decompressed size is returned via the backward scan
