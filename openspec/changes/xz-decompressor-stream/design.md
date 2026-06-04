## Context

`DecompressorStream` is the project's base class for seekable decompressor streams. `LzipDecompressorStream` already uses it to give lzip files efficient random access via a backwards trailer scan. XZ shares the same structural property: each stream ends with a 12-byte footer pointing to a variable-length index that records all blocks' compressed and uncompressed sizes. This makes the same backwards scan viable for XZ.

Current XZ paths:
- **Default** (`lzma.open`): no random access; SEEK_END decompresses the entire file
- **Optional** (`use_python_xz=True`, python-xz library): random access at block level, but requires seekable input, always does an upfront full scan, and is an external dependency

`LzipDecompressorStream` shows the pattern works: implement a state machine for forward streaming, implement `_build_index` for the backwards scan, store `SeekPoint` objects at member/stream/block boundaries. The base class handles the rest.

## Goals / Non-Goals

**Goals:**
- Eliminate python-xz as a dependency; `XzDecompressorStream` covers everything it did
- Support efficient SEEK_END (no decompression — just backwards index scan)
- Support block-level random access: seeking to any position restarts decompression from the nearest block boundary, not the start of the file
- Handle multi-stream XZ files correctly (forward streaming and backwards scan)
- Fix the `read_xz_metadata` multi-stream bug in `single_file_reader.py`
- Handle non-seekable streams gracefully (sequential-only, no index built)
- Handle stream padding (4-byte-aligned nulls between streams)
- Handle trailing non-XZ bytes after valid streams (silently stop, like lzip)

**Non-Goals:**
- XZ write support
- Parallel/threaded decompression
- Block reader LRU cache (the python-xz `RollingBlockReadStrategy`); may be added later
- Supporting `use_python_xz=True` as a transition path (just remove it)

## Decisions

### Decision 1: Stream-level vs block-level seek points — go straight to block-level

**Choice**: implement block-level seek points from the start.

**Rationale**: The backwards scan already reads per-block records from the index (that's the XZ index format — blocks, not streams). Stopping at stream-level would mean discarding the block data we already parsed. For typical `.tar.xz` files compressed single-threaded, there is one stream with one large block; stream-level seeking gives exactly one seek point (start of file) which provides no benefit over `lzma.open`. Block-level is the minimum useful granularity.

**Alternative considered**: stream-level only first, add blocks later. Rejected — streams are cheap to compute from blocks (just accumulate), so it's no extra work.

### Decision 2: Block decompressor — synthetic XZ stream wrapper

**Choice**: wrap each block's raw bytes in a synthetic complete XZ stream (`[stream header][block bytes][index+footer]`) and decompress with `LZMADecompressor(format=FORMAT_XZ)`.

**Rationale**: This is exactly what python-xz's `BlockRead` does. It avoids parsing the XZ block header format (which contains the filter chain). All three values needed to build the synthetic wrapper — `check`, `unpadded_size`, `uncompressed_size` — are already available from the stream's index. `LZMADecompressor(format=FORMAT_XZ)` handles the block header parsing internally.

**Alternative considered**: `FORMAT_RAW` with explicit LZMA2 filter parameters extracted from the block header. Requires parsing the block header (flags, filter count, filter IDs, filter properties). More code, more spec coverage needed, no benefit.

### Decision 3: `SeekPoint.state` carries block metadata

**Choice**: block-level seek points store `(check: int, unpadded_size: int, uncompressed_size: int)` in `SeekPoint.state`. `_create_decompressor` inspects `point.state` to choose between stream-level decompressor (state is `None`) and block-level decompressor (state has tuple).

**Rationale**: `SeekPoint.state` is typed `Any` and already used by `_LzipState`-style decompressors for carrying format-specific state. Block metadata is small (3 ints per block).

**Alternative considered**: separate `XzBlockSeekPoint` subclass. Over-engineered; the `Any` state field exists precisely for this use.

### Decision 4: Forward streaming state machine — `_XzState`

**Choice**: implement `_XzState` analogous to `_LzipState`. States: `NEED_HEADER` (accumulate 12 bytes, verify `\xfd7zXZ\x00` magic) → `IN_STREAM` (feed to `LZMADecompressor(FORMAT_XZ)`, watch for `.eof`) → back to `NEED_HEADER`.

Key difference from lzip: XZ `FORMAT_XZ` decompressor accepts raw XZ bytes — no synthetic header prepending needed. The `unused_data` after `.eof` is directly the bytes of the next stream (or stream padding or trailing garbage).

Stream padding: between-stream null bytes (4-byte aligned) are consumed in `NEED_HEADER` state by checking if the accumulated 12 bytes start with `\x00\x00\x00\x00` and skipping up to 12 bytes of nulls before looking for the real magic.

Compressed size tracking: during forward streaming we need `compressed_size` to record seek points. Computed as `bytes_fed_to_dec - len(dec.unused_data)` when `.eof` triggers. We track `_bytes_fed_to_current_dec` in `_XzState`.

Progressive seek point building: after each stream completes, `_update_index` adds a `SeekPoint` at the next stream's start. This mirrors how `LzipDecompressorStream._update_index` works. However, forward streaming only gives stream-boundary seek points; block-boundary points come from `_build_index` (backwards scan).

**Trade-off**: After a SEEK_END triggers `_build_index`, the seek_points list transitions from stream-boundary only to block-boundary. Seek points at stream boundaries are a subset of block boundaries (the first block of each stream starts at the stream boundary), so they merge cleanly.

### Decision 5: `_build_index` — standalone `_read_xz_index_backwards` function

**Choice**: extract a `_read_xz_index_backwards(stream, file_size, stop_at, start_decompressed_offset) -> list[_XzBlockBounds]` function (analogous to `_read_index_backwards` for lzip), placed in a new `xz_stream.py` module. `XzDecompressorStream._build_index` calls it. `read_xz_metadata` in `single_file_reader.py` also calls it (fixing the multi-stream bug).

**XZ backwards scan algorithm**:
```
compressed_end = file_size
while compressed_end > stop_at:
    # 1. Skip stream padding (4-byte aligned nulls before the footer)
    while True:
        seek to compressed_end - 4, read 4 bytes
        if not all-zero: break
        compressed_end -= 4

    # 2. Read 12-byte footer at compressed_end - 12
    footer → check, backward_size (verified against CRC32)

    # 3. Read index at compressed_end - 12 - backward_size
    index → list of (unpadded_size, uncompressed_size) per block

    # 4. Compute block offsets
    blocks_compressed_total = sum(round_up(unpadded_size) for each block)
    stream_header_start = compressed_end - 12 - backward_size - blocks_compressed_total - 12

    # 5. Verify stream header magic at stream_header_start

    # 6. Emit _XzBlockBounds entries for each block in this stream

    compressed_end = stream_header_start
```

**Alternative considered**: not extracting a shared function and keeping `read_xz_metadata` as-is but fixed. Rejected — the shared function makes both callers correct and consistent.

### Decision 6: `read_xz_metadata` — reuse `_read_xz_index_backwards`

**Choice**: rewrite `read_xz_metadata` to call `_read_xz_index_backwards(stream, file_size)` and sum all blocks' `uncompressed_size` across all streams.

**Multi-stream bug**: current code reads only the last stream's index and only its blocks. For a 2-stream XZ file (stream sizes A and B), it returns B instead of A+B. The rewrite walks all streams.

**Alternative considered**: remove `read_xz_metadata` and leave `file_size=None` for XZ (matching lzip behaviour). Rejected for now — users rely on `ArchiveMember.file_size` being populated for XZ files. Could be revisited later.

### Decision 7: Remove `use_python_xz` and python-xz dependency

**Choice**: remove `use_python_xz` from `ArchiveyConfig`, remove `open_python_xz_stream` and `_translate_python_xz_exception` from `compressed_streams.py`, remove python-xz from `pyproject.toml` optional deps.

**Rationale**: `XzDecompressorStream` strictly improves on python-xz: no seekability requirement, progressive index building, no external dep. There is no reason to keep the flag.

**Migration**: callers setting `use_python_xz=True` will get a `TypeError` (unknown field). Document in changelog.

## Risks / Trade-offs

**[Risk] Block decompression via synthetic stream has extra overhead** → Each block read prepends 12 bytes and appends a variable index+footer. For tiny blocks this is overhead-heavy. Mitigation: XZ blocks are typically ≥ 100 KB; overhead is negligible.

**[Risk] MBI decoding edge cases in XZ index** → Malformed multi-byte integers could panic or produce garbage offsets. Mitigation: validate MBI byte count (XZ spec: max 9 bytes per integer), catch and translate to `ArchiveCorruptedError`.

**[Risk] Stream padding edge cases** → The null-skip loop in the backwards scan could consume too much if the file is mostly zeros. Mitigation: limit null-skipping to `< 4` bytes before each footer (the XZ spec says padding is any multiple of 4 null bytes; we read 4 bytes at a time and stop when non-zero).

**[Risk] Removing `use_python_xz` is a breaking config API change** → Any caller that passes `use_python_xz=True` will break. Mitigation: document in changelog; the field is behind an `ArchiveyConfig` dataclass so static type checkers will catch it.

**[Risk] Forward streaming compressed-size tracking is approximate** → `bytes_fed - len(unused_data)` is correct only if the decompressor doesn't buffer. `LZMADecompressor` is streaming (no internal buffering beyond what's needed); this is fine in practice, but worth a unit test.

## Open Questions

1. **`_XzState` progressive seek points**: should forward streaming produce stream-boundary seek points or block-boundary ones? The block index is only available via backwards scan; during forward streaming we don't parse the XZ index inline. So forward streaming gives stream-boundary only; backwards scan upgrades to block-boundary. This is acceptable — confirm.

2. **Transition behaviour for existing code that passes `use_python_xz=True`**: hard error (TypeError from dataclass) or keep the field with a `DeprecationWarning`? Prefer hard removal given this is a minor library.

3. **Tests for non-seekable XZ streams**: the `xz` library raises on non-seekable input; our stream should degrade gracefully. Add a specific test confirming sequential-only mode works.
