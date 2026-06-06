## ADDED Requirements

### Requirement: Compression method is available as a typed value

A `CompressionMethod` enum SHALL enumerate the compression methods Archivey
recognizes (such as `STORED`, `DEFLATE`, `LZMA`, `LZMA2`, `ZSTD`, `BZIP2`, `PPMD`,
and `BCJ2`) with an `UNKNOWN` fallback. It SHALL be a `StrEnum` so existing string
comparisons keep working. `ArchiveMember.compression_method` SHALL use these values
for recognized methods and remain `None` when the format does not report one.

#### Scenario: Known method is typed
- **WHEN** a member is compressed with a method Archivey recognizes
- **THEN** its `compression_method` equals the corresponding `CompressionMethod` value
  and also compares equal to that method's string

#### Scenario: Unreported method stays None
- **WHEN** the source format does not report a compression method for a member
- **THEN** the member's `compression_method` is `None`
