## ADDED Requirements

### Requirement: Compression method is available as a typed value

A `CompressionMethod` enum SHALL enumerate the compression methods Archivey
recognizes (such as `STORED`, `DEFLATE`, `LZMA`, `LZMA2`, `ZSTD`, `BZIP2`, `PPMD`,
and `BCJ2`) with an `UNKNOWN` fallback. It SHALL be a `StrEnum` so existing string
comparisons keep working. `ArchiveMember.compression_method` SHALL hold the
recognized **primary** codec as a `CompressionMethod` value, SHALL be `UNKNOWN`
when the format reports a codec Archivey does not map, and SHALL remain `None` when
the format does not report a compression method at all.

Because a closed enum cannot represent a multi-filter chain (such as 7z
`LZMA2 + BCJ2`) or a third-party reader's own codec name, `ArchiveMember` SHALL also
expose a free-form `compression_method_detail: Optional[str]` carrying the full,
verbatim description without loss. When the format reports only a single recognized
codec, `compression_method_detail` MAY be `None` (the typed value already says
everything). Readers SHALL NOT discard codec information silently: anything the
format reports that does not fit the enum SHALL be preserved in
`compression_method_detail`.

#### Scenario: Known method is typed
- **WHEN** a member is compressed with a method Archivey recognizes
- **THEN** its `compression_method` equals the corresponding `CompressionMethod` value
  and also compares equal to that method's string

#### Scenario: Unreported method stays None
- **WHEN** the source format does not report a compression method for a member
- **THEN** the member's `compression_method` is `None` and `compression_method_detail`
  is `None`

#### Scenario: Multi-filter chain preserves detail
- **WHEN** a 7z member uses a filter chain such as LZMA2 followed by a BCJ filter
- **THEN** `compression_method` is the typed primary codec (`CompressionMethod.LZMA2`)
  and `compression_method_detail` is the full chain string (e.g. `"LZMA2 + BCJ"`)

#### Scenario: Unmapped codec is UNKNOWN but not lost
- **WHEN** the format reports a codec Archivey does not have an enum value for
- **THEN** `compression_method` is `CompressionMethod.UNKNOWN` and
  `compression_method_detail` carries the reported codec name
