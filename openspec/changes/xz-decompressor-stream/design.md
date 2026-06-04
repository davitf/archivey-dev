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
- Remove `read_xz_metadata`; populate `file_size` from the stream for both XZ and lzip
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

### Decision 3: `SeekPoint.state` carries block metadata; stream-level is only a fallback

**Choice**: block-level seek points store `(check: int, unpadded_size: int, uncompressed_size: int)` in `SeekPoint.state`. `_create_decompressor` inspects `point.state` to choose between stream-level decompressor (state is `None`) and block-level decompressor (state has tuple).

**Why two modes at all?** Block-level decompression requires knowing `check`, `unpadded_size`, and `uncompressed_size` for the block before any bytes are read. This metadata lives in the XZ index (at the end of each stream). The very first seek point `SeekPoint(0, 0, state=None)` is created before any index has been read, so it cannot carry block metadata. It uses stream-level (`_XzState`) as a fallback. Every other seek point — added after index data becomes available — carries block metadata and uses the block-level decompressor. In practice, stream-level is only ever used for the single initial seek point.

**After the index is known** (either from `_build_index()` or from the per-stream scan described in Decision 4a), all seek points except `SeekPoint(0, 0)` are block-level. The first block of stream 0 always starts at decompressed offset 0 — `add_seek_points` treats it as a duplicate of `SeekPoint(0, 0)` (same `decompressed_offset`), so it is not added; `SeekPoint(0, 0, state=None)` stays and handles seeks to the start of the file via `_XzState`. All other blocks (decompressed offset > 0) get proper block-level seek points.

**Seeking to a block-level seek point:** `_create_decompressor` creates an `_XzBlockChain` — an object that knows the full ordered list of block seek points from the current one onward. It manages a synthetic-stream decompressor per block and transitions automatically to the next block when the current block's synthetic stream is exhausted. This is necessary because the base class's forward-read loop calls `_read_decompressed_chunk` repeatedly until the target position is reached; if the decompressor "finished" after the first block, the base class would incorrectly declare EOF.

**`SeekPoint.state` is typed `Any`** and exists precisely for format-specific state. Block metadata is 3 ints per block.

**Alternative considered**: separate `XzBlockSeekPoint` subclass. Over-engineered; the `Any` state field exists precisely for this use.

### Decision 4: Forward streaming state machine — `_XzState`

**Choice**: implement `_XzState` analogous to `_LzipState`. States: `NEED_HEADER` (accumulate 12 bytes, verify `\xfd7zXZ\x00` magic) → `IN_STREAM` (feed to `LZMADecompressor(FORMAT_XZ)`, watch for `.eof`) → back to `NEED_HEADER`.

Key difference from lzip: XZ `FORMAT_XZ` decompressor accepts raw XZ bytes — no synthetic header prepending needed. The `unused_data` after `.eof` is directly the bytes of the next stream (or stream padding or trailing garbage).

Stream padding: between-stream null bytes (4-byte aligned) are consumed in `NEED_HEADER` state by checking if the accumulated 12 bytes start with `\x00\x00\x00\x00` and skipping up to 12 bytes of nulls before looking for the real magic.

Compressed size tracking: during forward streaming we need `compressed_size` to record seek points. Computed as `bytes_fed_to_dec - len(dec.unused_data)` when `.eof` triggers. We track `_bytes_fed_to_current_dec` in `_XzState`.

### Decision 4a: Per-stream backward scan populates block seek points during forward reads

**Choice**: in `_update_index` (called when a stream completes during forward reading), immediately perform a mini-backwards scan of just the completed stream to populate its block-level seek points. This does not wait for the user to trigger a full `_build_index()`.

**Rationale**: when stream N completes, we know its exact compressed span: `[comp_cursor_at_stream_start, comp_cursor_at_stream_start + comp_size)`. We can call `_read_xz_index_backwards(inner, stream_end, stop_at=stream_start, start_decompressed_offset=stream_decomp_start)` to extract all block bounds for that stream. The inner stream's position is saved and restored. The result is added to `_seek_points` via `add_seek_points`. If the scan fails (corrupt index), we log a warning and skip — stream-level or `_build_index` fallback still works.

**Effect**: after sequentially reading even one stream, forward and backward seeks within that stream use block-level granularity. `_build_index()` (triggered by SEEK_END or a seek past the known frontier) covers streams not yet reached by forward reads.

**Merge with `_build_index` results**: seek points from per-stream scans and from `_build_index` are both block-level (same `SeekPoint.state` shape). `add_seek_points` handles deduplication. After `_build_index` runs, `_index_built = True` suppresses any further per-stream scans.

### Decision 5: `_build_index` — standalone `_read_xz_index_backwards` function

**Choice**: extract a `_read_xz_index_backwards(stream, file_size, stop_at, start_decompressed_offset) -> list[_XzBlockBounds]` function (analogous to `_read_index_backwards` for lzip), placed in a new `xz_stream.py` module. Both `XzDecompressorStream._build_index` and `_update_index`'s per-stream scan call it.

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

**Alternative considered**: not extracting a shared function and keeping `read_xz_metadata` as-is but fixed. Rejected — see Decision 6: `read_xz_metadata` is removed entirely in favour of reading size from the stream.

### Decision 6: `SingleFileReader` reads `file_size` from the stream, not from a separate scan

**Choice**: remove `read_xz_metadata` (and any equivalent for lzip). Instead, after `SingleFileReader` opens the stream as `self.fileobj`, call `self.fileobj.seek(0, io.SEEK_END)` to trigger the backwards index scan and set `_size`, then `self.fileobj.seek(0)` to reset. Read `self.fileobj._size` and assign to `member.file_size`.

**Applies to both XZ and lzip**: lzip currently leaves `file_size=None` in `ArchiveMember`. Under this decision, both XZ and lzip populate `file_size` from the stream at construction time, with zero extra code per format.

**Efficiency**: the backwards scan reads only footers, indices, and trailers — no decompression. For a typical XZ file, this is a handful of seeks and small reads. The cost is comparable to the current `read_xz_metadata` call, with no extra work since the stream would have done the scan anyway on the user's first SEEK_END.

**Current multi-stream bug in `read_xz_metadata`**: the current code reads only the last stream's index and returns only that stream's decompressed size. This approach eliminates the bug entirely — the backwards scan in `XzDecompressorStream._build_index` walks all streams.

**What happens to `read_xz_metadata`**: the function is removed from `single_file_reader.py`. The helpers it used (`_read_xz_multibyte_integer`, `XZ_MAGIC_FOOTER`, `XZ_STREAM_HEADER_MAGIC`) are also removed (their functionality lives in `xz_stream.py`).

**Alternative considered**: keep `read_xz_metadata` but rewrite it to call `_read_xz_index_backwards`. Rejected — it duplicates the scan (once in `read_xz_metadata`, once when the user first seeks), and requires maintaining a separate code path per format. Reading from the stream is simpler and uniform.

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

1. **Transition behaviour for existing code that passes `use_python_xz=True`**: hard error (TypeError from dataclass) or keep the field with a `DeprecationWarning`? Prefer hard removal given this is a minor library.

2. **Tests for non-seekable XZ streams**: the `xz` library raises on non-seekable input; our stream should degrade gracefully. Add a specific test confirming sequential-only mode works.

3. **`_XzBlockChain` and the base class**: the base class `_read_decompressed_chunk` does not know about block boundaries. `_XzBlockChain` must either (a) override `_read_decompressed_chunk` in `XzDecompressorStream` to be block-aware, or (b) expose the same `feed/flush/is_finished` interface as `_XzState` but internally manage block transitions and limit reads from `_inner` to exactly `round_up(unpadded_size)` bytes per block. Option (b) keeps the base class interface intact; confirm this is the approach before implementing.
