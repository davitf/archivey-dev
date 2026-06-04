## ADDED Requirements

### Requirement: XZ stream supports sequential decompression
`XzDecompressorStream` SHALL decompress single-stream and multi-stream XZ files sequentially when the underlying stream is non-seekable, producing the same bytes as `lzma.open`.

#### Scenario: Single-stream XZ file reads correctly
- **WHEN** a single-stream XZ file is opened with `XzDecompressorStream`
- **THEN** `read()` returns the complete decompressed content

#### Scenario: Multi-stream XZ file reads correctly
- **WHEN** a multi-stream XZ file (two or more concatenated XZ streams) is opened
- **THEN** `read()` returns the decompressed content of all streams concatenated in order

#### Scenario: Non-seekable stream degrades to sequential-only
- **WHEN** `XzDecompressorStream` wraps a non-seekable file-like object
- **THEN** `seekable()` returns `False` and `seek()` raises `io.UnsupportedOperation`
- **THEN** `read()` still returns the full decompressed content

#### Scenario: Stream padding between XZ streams is handled transparently
- **WHEN** a file contains 4-byte-aligned null padding bytes between two XZ streams
- **THEN** the padding is silently skipped and decompression continues with the next stream

#### Scenario: Trailing non-XZ bytes after valid streams are silently ignored
- **WHEN** a file has valid XZ streams followed by bytes that do not start with the XZ stream magic
- **THEN** `read()` returns only the decompressed content of the valid streams without raising an error

---

### Requirement: XZ stream supports SEEK_END without decompression
`XzDecompressorStream` SHALL resolve `seek(0, SEEK_END)` using a backwards index scan that reads only stream footers and block indices — no decompression is performed.

#### Scenario: SEEK_END returns total decompressed size
- **WHEN** `seek(0, SEEK_END)` is called on a multi-stream XZ file
- **THEN** the returned position equals the sum of all blocks' uncompressed sizes across all streams

#### Scenario: SEEK_END does not decompress any data
- **WHEN** `seek(0, SEEK_END)` is called on a fresh `XzDecompressorStream`
- **THEN** the stream's internal decompressed-byte counter remains at 0 after the call

#### Scenario: SEEK_END on multi-stream file returns correct total size
- **WHEN** a file has two XZ streams with decompressed sizes A and B
- **THEN** `seek(0, SEEK_END)` returns `A + B`

---

### Requirement: XZ stream supports block-level random access
After the backwards index is built, `XzDecompressorStream` SHALL seek to any decompressed byte offset by restarting decompression from the nearest block boundary, not from the start of the file.

#### Scenario: Backward seek restarts from nearest block boundary
- **WHEN** the index is built and a backward seek targets a position inside block N
- **THEN** decompression restarts from block N's compressed start, not from byte 0

#### Scenario: Forward seek across indexed blocks skips decompression
- **WHEN** the index is built and a forward seek targets the start of block N
- **THEN** blocks before N are not decompressed

#### Scenario: seek(-N, SEEK_END) lands in the correct block
- **WHEN** `seek(-N, SEEK_END)` is called after the index is built
- **THEN** the stream position is `total_size - N` and subsequent `read()` returns the last N bytes

---

### Requirement: Backwards index scan is correct for multi-stream files
`_read_xz_index_backwards` SHALL walk all XZ streams from EOF to start, decoding each stream's footer and block index, and return a list of `_XzBlockBounds` covering all streams.

#### Scenario: Backwards scan finds all streams
- **WHEN** `_read_xz_index_backwards` is called on a two-stream XZ file
- **THEN** the returned list contains entries for all blocks in both streams, in forward order

#### Scenario: Backwards scan produces correct block offsets
- **WHEN** `_read_xz_index_backwards` is called on a single-stream file with two blocks
- **THEN** `block[0].compressed_start + round_up(block[0].unpadded_size) == block[1].compressed_start`
- **THEN** `block[0].decompressed_start + block[0].uncompressed_size == block[1].decompressed_start`

#### Scenario: Backwards scan handles stream padding
- **WHEN** a file has null-byte padding between two XZ streams
- **THEN** the scan skips the padding and correctly locates the preceding stream's footer

#### Scenario: Corrupt footer magic raises ArchiveCorruptedError
- **WHEN** the last 2 bytes of the file are not `YZ`
- **THEN** `_read_xz_index_backwards` raises `ArchiveCorruptedError`

#### Scenario: Corrupt index CRC32 raises ArchiveCorruptedError
- **WHEN** the index bytes do not match the CRC32 stored in the index
- **THEN** `_read_xz_index_backwards` raises `ArchiveCorruptedError`

---

### Requirement: SingleFileReader reports correct file_size for XZ and lzip
`SingleFileReader` SHALL populate `member.file_size` for both XZ and lzip files by triggering the stream's backwards index scan, not via a separate metadata read. For multi-stream XZ files this fixes the previous bug where only the last stream's size was counted.

#### Scenario: Single-stream XZ file_size is correct
- **WHEN** a single-stream XZ file is opened via `SingleFileReader`
- **THEN** `member.file_size` equals the decompressed size of that stream

#### Scenario: Multi-stream XZ file_size is correct
- **WHEN** an XZ file with two streams of decompressed sizes A and B is opened via `SingleFileReader`
- **THEN** `member.file_size` equals `A + B`

#### Scenario: lzip file_size is now populated
- **WHEN** a lzip file is opened via `SingleFileReader`
- **THEN** `member.file_size` is set to the total decompressed size (previously it was `None`)

---

### Requirement: use_python_xz config field is removed
The `use_python_xz` field SHALL be removed from `ArchiveyConfig`. `XzDecompressorStream` is always used for XZ streams.

#### Scenario: XZ streams always use XzDecompressorStream
- **WHEN** `get_stream_open_fn(StreamFormat.XZ, config)` is called with any config
- **THEN** the returned open function produces an `XzDecompressorStream`

#### Scenario: python-xz library is no longer required
- **WHEN** the python-xz library is not installed
- **THEN** XZ archives open and decompress correctly without error
