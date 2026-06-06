# XZ Decompressor Stream Specification

## Overview

`XzDecompressorStream` is the default decompressor for XZ-format files. It extends
`_SegmentedDecompressorStream` (the intermediate base class for segmented formats)
and provides:

- **Sequential decompression** with no external dependencies (stdlib `lzma` only)
- **Efficient SEEK_END** via backwards index scan — no decompression performed
- **Block-level random access** — seeking restarts from the nearest block boundary,
  not the start of the file
- **Correct multi-stream support** — concatenated XZ streams are handled transparently
  in both forward reads and backwards index scans
- **Non-seekable stream support** — degrades gracefully to sequential-only mode

An optional `python-xz` library backend is available via `use_python_xz=True` in
`ArchiveyConfig`, but the default is always `XzDecompressorStream`.

---

## Architecture

```
DecompressorStream[T]
  └── _SegmentedDecompressorStream[_SDT]   ← shared cursor + feed/flush skeleton
        └── XzDecompressorStream            ← XZ-specific logic

_XzState         ← forward streaming state machine (NEED_HEADER → IN_STREAM cycle)
_XzBlockChain    ← block-level decompressor for random access seeks
_XzBlockBounds   ← dataclass: compressed/decompressed offsets + block metadata
```

### Key data structures

**`_XzBlockBounds`** — one entry per block (from the XZ stream index):
- `compressed_start`: absolute byte offset of the block in the file
- `decompressed_start`: absolute decompressed byte offset
- `unpadded_size`: raw block size (padding to 4-byte boundary not included)
- `uncompressed_size`: decompressed size of this block
- `check`: XZ check type (CRC32=1, CRC64=4, SHA256=10)
- `decompressed_end` property: `decompressed_start + uncompressed_size`

**`SeekPoint.state`** — stores an `_XzBlockBounds` directly for block-level seek
points. Stream-level fallback seek points have `state=None`.

---

## Sequential Decompression

### Requirement: Single-stream XZ file reads correctly
`XzDecompressorStream` SHALL decompress a single-stream XZ file, producing the
same bytes as `lzma.open`.

#### Scenario: Single-stream read
- WHEN a single-stream XZ file is opened with `XzDecompressorStream`
- THEN `read()` returns the complete decompressed content

### Requirement: Multi-stream XZ file reads correctly
Concatenated XZ streams SHALL be decompressed in order.

#### Scenario: Multi-stream read
- WHEN a multi-stream XZ file (two or more concatenated XZ streams) is opened
- THEN `read()` returns the decompressed content of all streams concatenated in order

### Requirement: Stream padding is handled transparently
4-byte-aligned null padding bytes between XZ streams (as allowed by the XZ spec)
SHALL be silently skipped.

#### Scenario: Stream padding
- WHEN a file contains 4-byte-aligned null padding between two XZ streams
- THEN the padding is silently skipped and decompression continues with the next stream

### Requirement: Trailing non-XZ bytes are silently ignored
Bytes after all valid XZ streams that do not begin with the XZ stream magic SHALL
be silently ignored (not treated as an error), consistent with `_LzipState` behaviour.

#### Scenario: Trailing non-XZ bytes
- WHEN a file has valid XZ streams followed by bytes not starting with the XZ magic
- THEN `read()` returns only the decompressed content of the valid streams

---

## Non-Seekable Stream Support

### Requirement: Non-seekable stream degrades to sequential-only
When `XzDecompressorStream` wraps a non-seekable file-like object, it SHALL
still decompress correctly while disabling all seeking and index-building features.

#### Scenario: Non-seekable stream
- WHEN `XzDecompressorStream` wraps a non-seekable file-like object
- THEN `seekable()` returns `False`
- THEN `seek()` raises `io.UnsupportedOperation`
- THEN `read()` still returns the full decompressed content

---

## SEEK_END Without Decompression

### Requirement: SEEK_END resolves via backwards index scan
`seek(0, SEEK_END)` SHALL return the total decompressed size by reading only XZ
stream footers and block indices — no decompression is performed.

#### Scenario: SEEK_END returns total decompressed size
- WHEN `seek(0, SEEK_END)` is called on a multi-stream XZ file
- THEN the returned position equals the sum of all blocks' uncompressed sizes across all streams

#### Scenario: SEEK_END does not decompress any data
- WHEN `seek(0, SEEK_END)` is called on a fresh `XzDecompressorStream`
- THEN the stream's internal decompressed-byte counter (`_decomp_cursor`) remains at 0

#### Scenario: SEEK_END on multi-stream file is correct
- WHEN a file has two XZ streams with decompressed sizes A and B
- THEN `seek(0, SEEK_END)` returns `A + B`

---

## Block-Level Random Access

### Requirement: Backwards index scan is correct for multi-stream files
`_read_xz_index_backwards` SHALL walk all XZ streams from EOF to file start,
decoding each stream's footer and block index, and return a list of `_XzBlockBounds`
covering all blocks in all streams in forward order.

#### Scenario: Backwards scan finds all streams
- WHEN `_read_xz_index_backwards` is called on a two-stream XZ file
- THEN the returned list contains entries for all blocks in both streams, in forward order

#### Scenario: Backwards scan produces correct block offsets
- WHEN `_read_xz_index_backwards` is called on a single-stream file with two blocks
- THEN `block[0].compressed_start + round_up(block[0].unpadded_size) == block[1].compressed_start`
- THEN `block[0].decompressed_start + block[0].uncompressed_size == block[1].decompressed_start`

#### Scenario: Backwards scan handles stream padding
- WHEN a file has null-byte padding between two XZ streams
- THEN the scan skips the padding and correctly locates the preceding stream's footer

#### Scenario: Corrupt footer magic raises ArchiveCorruptedError
- WHEN the last 2 bytes of the file are not `YZ`
- THEN `_read_xz_index_backwards` raises `ArchiveCorruptedError`

#### Scenario: Corrupt index CRC32 raises ArchiveCorruptedError
- WHEN the index bytes do not match the stored CRC32
- THEN `_read_xz_index_backwards` raises `ArchiveCorruptedError`

### Requirement: Backward seek restarts from nearest block boundary
After the index is built, seeking backward SHALL restart decompression from the
nearest block boundary rather than the start of the file.

#### Scenario: Backward seek uses nearest block
- WHEN the index is built and a backward seek targets a position inside block N
- THEN decompression restarts from block N's compressed start, not from byte 0

### Requirement: Forward seek across indexed blocks skips decompression
After the index is built, seeking forward SHALL skip all blocks before the target.

#### Scenario: Forward seek across blocks
- WHEN the index is built and a forward seek targets the start of block N
- THEN blocks before N are not decompressed

### Requirement: seek(-N, SEEK_END) lands in the correct block

#### Scenario: Relative seek from end
- WHEN `seek(-N, SEEK_END)` is called after the index is built
- THEN the stream position is `total_size - N`
- THEN subsequent `read()` returns the last N bytes

---

## Progressive Index Building

### Requirement: Per-stream backward scan runs during forward reads
When a complete XZ stream is read sequentially, `XzDecompressorStream` SHALL
immediately perform a backwards scan of just that stream's compressed range to
populate block-level seek points. This provides block-level random access without
waiting for a full `_build_index()` call (triggered by SEEK_END or a backward seek).

- The per-stream scan saves and restores `_inner.tell()`
- If the scan fails with `ArchiveCorruptedError`, a warning is logged and the
  stream-level seek point is retained as fallback
- Once `_index_built = True` (after a full `_build_index()` run), per-stream scans
  are suppressed

### Requirement: Block seek points and stream seek points coexist correctly
Block-level seek points (from both per-stream scans and full `_build_index()`)
and stream-level fallback seek points coexist in `_seek_points`. The initial
`SeekPoint(0, 0, state=None)` (stream-level fallback) is always present and covers
seeks to the very start of the file. `add_seek_points` handles deduplication.

---

## Block Decompressor — Synthetic XZ Stream Wrapper

### Requirement: Block-level decompression uses synthetic XZ stream wrapping
`_XzBlockChain` SHALL decompress individual blocks by wrapping each block's raw
bytes in a complete synthetic XZ stream (`[stream header][block bytes][index+footer]`)
and decompressing with `LZMADecompressor(format=FORMAT_XZ)`. This avoids parsing
the XZ block header's filter chain directly and is identical to the technique used
by the python-xz library.

- The three values needed to construct the synthetic stream (`check`, `unpadded_size`,
  `uncompressed_size`) are available from the `_XzBlockBounds` stored in `SeekPoint.state`
- `_XzBlockChain` manages transitions across multiple consecutive blocks automatically,
  so the `_SegmentedDecompressorStream` base class sees a single uninterrupted feed/flush
  interface

---

## SingleFileReader Integration

### Requirement: SingleFileReader reports correct file_size for XZ and lzip
`SingleFileReader` SHALL populate `member.file_size` for XZ and lzip files by
triggering the stream's backwards index scan immediately after opening (`seek(0, SEEK_END)`
then `seek(0)`), reading `_size` from the stream. No separate metadata read function
is needed.

#### Scenario: Single-stream XZ file_size is correct
- WHEN a single-stream XZ file is opened via `SingleFileReader`
- THEN `member.file_size` equals the decompressed size of that stream

#### Scenario: Multi-stream XZ file_size is correct
- WHEN an XZ file with two streams of decompressed sizes A and B is opened
- THEN `member.file_size` equals `A + B`

#### Scenario: lzip file_size is populated
- WHEN a lzip file is opened via `SingleFileReader`
- THEN `member.file_size` is set to the total decompressed size (not `None`)

---

## Optional python-xz Backend

### Requirement: use_python_xz config field selects the python-xz library backend
When `use_python_xz=True` is set in `ArchiveyConfig`, XZ streams SHALL use
`indexed_bzip2`-style random access via the `python-xz` library.

#### Scenario: use_python_xz=True uses python-xz
- WHEN `get_stream_open_fn(StreamFormat.XZ, config)` is called with `config.use_python_xz=True`
- THEN the returned open function uses `xz.open(path)`

#### Scenario: use_python_xz=True with missing library raises PackageNotInstalledError
- WHEN `use_python_xz=True` and the python-xz library is not installed
- THEN opening an XZ stream raises `PackageNotInstalledError`

#### Scenario: Default always uses XzDecompressorStream
- WHEN `use_python_xz` is `False` (default)
- THEN XZ archives open via `XzDecompressorStream` regardless of whether python-xz is installed

---

## Error Handling

### Requirement: Truncated mid-stream raises ArchiveEOFError
If the input ends while a stream is being decompressed (not cleanly between streams),
`_XzState.flush()` SHALL raise `ArchiveEOFError`.

### Requirement: No valid streams raises ArchiveCorruptedError
If `flush()` is called without any complete XZ streams having been seen,
`_XzState.flush()` SHALL raise `ArchiveCorruptedError`.

### Requirement: LZMA errors are translated to ArchiveCorruptedError
`lzma.LZMAError` SHALL be caught and translated to `ArchiveCorruptedError` by
`_translate_xz_exception`.
