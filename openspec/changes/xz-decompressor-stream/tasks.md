## 1. New xz_stream.py module — data structures and backward index scan

- [x] 1.1 Create `src/archivey/formats/xz_stream.py` with `_XzBlockBounds` dataclass (fields: `compressed_start`, `decompressed_start`, `unpadded_size`, `uncompressed_size`, `check`); add `decompressed_end` property
- [x] 1.2 Implement MBI helpers `_encode_mbi` / `_decode_mbi` (variable-length integer encoding used in XZ index); these can be thin wrappers or inline in the index parsing function
- [x] 1.3 Implement `_parse_xz_index(data: bytes) -> list[tuple[int, int]]` — decodes MBI-encoded `(unpadded_size, uncompressed_size)` records from raw index bytes; verifies CRC32 and index indicator byte; raises `ArchiveCorruptedError` on any validation failure
- [x] 1.4 Implement `_parse_xz_footer(data: bytes) -> tuple[int, int]` — returns `(check, backward_size)` from a 12-byte footer; verifies `YZ` magic and CRC32
- [x] 1.5 Implement `_parse_xz_header(data: bytes) -> int` — returns `check` from a 12-byte header; verifies `\xfd7zXZ\x00` magic and CRC32
- [x] 1.6 Implement `_read_xz_index_backwards(stream, file_size, stop_at=0, start_decompressed_offset=0) -> list[_XzBlockBounds]` — backward scan walking all streams; handles null padding; calls 1.3–1.5; raises `ArchiveCorruptedError` on any structural failure; returns blocks in forward order with correct absolute `compressed_start` and `decompressed_start`

## 2. _XzState — forward streaming state machine

- [x] 2.1 Add `_XzState` class to `xz_stream.py` with states `NEED_HEADER` / `IN_STREAM`; internal fields: `_buf: bytearray`, `_dec: LZMADecompressor | None`, `_bytes_fed: int`, `_streams_seen: int`, `_finished: bool`
- [x] 2.2 Implement `_XzState.feed(data) -> tuple[bytes, list[tuple[int, int]]]` — returns `(decompressed_bytes, new_streams)` where each entry in `new_streams` is `(decompressed_size, compressed_size)` for each completed stream; mirrors `_LzipState.feed` signature
- [x] 2.3 Implement `_XzState.flush() -> tuple[bytes, list[tuple[int, int]]]` — handles end-of-input; succeeds if cleanly between streams (no bytes or non-XZ trailing bytes after at least one stream); raises `ArchiveEOFError` if truncated mid-stream; raises `ArchiveCorruptedError` if no streams seen at all
- [x] 2.4 Implement `_XzState.is_finished() -> bool`
- [x] 2.5 Handle stream padding in `NEED_HEADER`: when accumulated bytes start with `\x00`, consume 4-byte-aligned null runs before looking for the `\xfd7zXZ\x00` magic
- [x] 2.6 Handle trailing non-XZ bytes after valid streams: detect in `NEED_HEADER`, set `_finished = True`, clear buffer (same graceful-stop as `_LzipState`)
- [x] 2.7 Add `_XzBlockChain` class to `xz_stream.py`: takes a list of `_XzBlockBounds` (from the current block onward) and the inner stream; exposes `feed(chunk)`, `flush()`, and `is_finished()` with the same signature as `_XzState`; internally manages a per-block `LZMADecompressor` and synthetic stream wrapper; limits each `feed` call to at most `round_up(unpadded_size) - bytes_fed_so_far` bytes for the current block, injects synthetic header (pre-fed at block start) and footer (injected when block bytes are exhausted), then advances to the next block; `is_finished()` returns True only when the last block in the list is exhausted

## 3. XzDecompressorStream — DecompressorStream subclass

- [x] 3.1 Add `XzDecompressorStream` class in `decompressor_stream.py`; pre-declare `_comp_cursor: int` and `_decomp_cursor: int` in `__init__` (same pattern as `LzipDecompressorStream`)
- [x] 3.2 Implement `_create_decompressor(point: SeekPoint) -> _XzState | _XzBlockChain` — if `point.state` is `None`, return a fresh `_XzState` (stream-level fallback, only used for `SeekPoint(0, 0)`); if `point.state` is a `(check, unpadded_size, uncompressed_size)` tuple, collect all subsequent block-level seek points from `_seek_points` and return `_XzBlockChain(blocks_from_here, self._inner)`
- [x] 3.3 Implement `_decompress_chunk(chunk: bytes) -> bytes` — call `self._decompressor.feed(chunk)`, call `_update_index(new_streams)` if decompressor is `_XzState`
- [x] 3.4 Implement `_flush_decompressor() -> bytes` — call `self._decompressor.flush()`, call `_update_index(new_streams)` if decompressor is `_XzState`
- [x] 3.5 Implement `_is_decompressor_finished() -> bool`
- [x] 3.6 Implement `_update_index(new_streams: list[tuple[int, int]])`: for each completed stream, (a) add a stream-boundary `SeekPoint(decomp_cursor, comp_cursor, state=None)` (skipped for stream 0 since `SeekPoint(0, 0)` already covers it); (b) if inner is seekable and `_index_built` is False, save `_inner.tell()`, call `_read_xz_index_backwards` for just this stream's compressed range, add resulting block-level `SeekPoint`s, restore `_inner.tell()`; on `ArchiveCorruptedError`, log and skip; advance `_comp_cursor` and `_decomp_cursor`
- [x] 3.7 Implement `_build_index(last_known: SeekPoint) -> tuple[list[SeekPoint], int | None]` — call `_read_xz_index_backwards(inner, file_size, stop_at=last_known.compressed_offset, start_decompressed_offset=last_known.decompressed_offset)`; on `ArchiveCorruptedError`, log warning and return `([], None)`; convert `_XzBlockBounds` to `SeekPoint` objects with block metadata in `.state`; return total decompressed size

## 4. Integration — compressed_streams.py and config

- [x] 4.1 Add `open_xz_stream(path) -> BinaryIO` to `compressed_streams.py` that returns `XzDecompressorStream(path)`
- [x] 4.2 Add `_translate_xz_exception(e) -> Optional[ArchiveError]` — maps `lzma.LZMAError` → `ArchiveCorruptedError`, `EOFError` → `ArchiveEOFError`
- [x] 4.3 Update `get_stream_open_fn` for `StreamFormat.XZ`: always return `(open_xz_stream, _translate_xz_exception)`; remove the `config.use_python_xz` branch
- [x] 4.4 Remove `open_python_xz_stream`, `_translate_python_xz_exception`, and the `xz` import guard from `compressed_streams.py`
- [x] 4.5 Remove `use_python_xz` field from `ArchiveyConfig` in `config.py` and from `ConfigOverrides` TypedDict
- [x] 4.6 Remove python-xz from optional dependencies in `pyproject.toml`; update `dependency_checker.py` if it references `xz`

## 5. Read file_size from stream in single_file_reader.py (XZ and lzip)

- [x] 5.1 In `SingleFileReader.__init__`, after opening `self.fileobj`, if the stream is seekable: call `self.fileobj.seek(0, io.SEEK_END)` (triggers backwards index scan) then `self.fileobj.seek(0)`; set `member.file_size = self.fileobj._size`; apply for both `ArchiveFormat.XZ` and `ArchiveFormat.LZIP`
- [x] 5.2 Remove `read_xz_metadata` function from `single_file_reader.py` entirely
- [x] 5.3 Remove the now-unused `_read_xz_multibyte_integer` helper, `XZ_MAGIC_FOOTER`, and `XZ_STREAM_HEADER_MAGIC` constants from `single_file_reader.py`

## 6. Tests

- [x] 6.1 Create `tests/archivey/test_xz_stream.py` mirroring the structure of `test_lzip_stream.py`; add helpers `make_multi_stream(parts)` (concatenates `lzma.compress(p, format=FORMAT_XZ)` for each part) and `open_xz(data)` / `open_xz_counting(data)`
- [x] 6.2 Test basic read: single-stream, multi-stream, chunked reads
- [x] 6.3 Test SEEK_END: correct total size returned, zero bytes decompressed, `seek(-N, SEEK_END)` reads last N bytes
- [x] 6.4 Test forward seeking: seek past blocks, verify jump uses seek points (check `_decomp_cursor`)
- [x] 6.5 Test backward seeking: seek backward to earlier block, verify nearest seek point used (not byte 0)
- [x] 6.6 Test non-seekable stream: `seekable()` returns False, `read()` succeeds
- [x] 6.7 Test stream padding: file with null padding between streams decompresses correctly
- [x] 6.8 Test trailing non-XZ data: silently ignored; size and content correct
- [x] 6.9 Test `_read_xz_index_backwards` directly: block offsets, multi-stream, corrupt footer, corrupt index CRC
- [x] 6.10 Test corruption detection: bad magic, truncated mid-stream, bad index MBI
- [x] 6.11 Test `SingleFileReader` size for multi-stream XZ file: `member.file_size` equals sum of both streams' sizes; also test lzip `file_size` is now populated
- [x] 6.12 Update `test_missing_packages.py`: confirm python-xz absence no longer raises `PackageNotInstalledError` for XZ
- [x] 6.13 Update any existing tests that set `use_python_xz=True` (search `test_open_compressed_stream.py` and others)

## 7. Cleanup and verification

- [ ] 7.1 Run the full test suite; confirm no regressions in existing XZ tests
- [ ] 7.2 Verify `python-xz>=0.5.0` is present in both `optional` and `optional-freethreaded` dep groups in `pyproject.toml`, and `use_python_xz` config field is restored
- [ ] 7.3 Update `CLAUDE.md` or changelog if the project maintains one; no breaking change for `use_python_xz` (re-added); note that default XZ backend is now `XzDecompressorStream` (stdlib only)

## 8. Re-add python-xz as optional backend

- [ ] 8.1 Re-add `python-xz>=0.5.0` to `optional` and `optional-freethreaded` dep groups in `pyproject.toml`
- [ ] 8.2 Re-add `use_python_xz: bool = False` field to `ArchiveyConfig` in `config.py` (with docstring: "Use python-xz library for XZ streams if installed; raises PackageNotInstalledError if enabled but not installed. Default backend is XzDecompressorStream (stdlib only).")
- [ ] 8.3 Re-add `open_python_xz_stream(path)` and `_translate_python_xz_exception(e)` to `compressed_streams.py`; guard the `import xz` with a try/except as before
- [ ] 8.4 Update `get_stream_open_fn` for `StreamFormat.XZ`: if `config.use_python_xz` is True, use `open_python_xz_stream` / `_translate_python_xz_exception` (raises `PackageNotInstalledError` if not installed); otherwise use `open_xz_stream` / `_translate_xz_exception`
- [ ] 8.5 Add/update test: `use_python_xz=True` with python-xz not installed raises `PackageNotInstalledError`; `use_python_xz=False` (default) always uses `XzDecompressorStream` regardless of whether python-xz is installed

## 9. Benchmark script

- [ ] 9.1 Create `benchmarks/` directory with `benchmarks/bench_xz.py`; add a `benchmarks/README.md` documenting how to run and what each variant tests
- [ ] 9.2 Add seeded synthetic test-data generator: 80% limited-charset text (word-like repetitions from a small vocabulary), 20% random bytes; target 100 MB uncompressed; reproducible via fixed seed
- [ ] 9.3 Add XZ file-variant generators (all on-the-fly, no stored files):
  - `single_block`: standard `lzma.compress(FORMAT_XZ)` — one stream, one block
  - `multi_block_1mb`: subprocess `xz --block-size=1MiB` — one stream, blocks every 1 MB; skip gracefully if `xz` binary absent
  - `multi_stream`: concatenate 100 × `lzma.compress(1 MB chunk)` — 100 streams of 1 MB each
  - `trailing_data`: `multi_block_1mb` output + 4 KB of `os.urandom` appended — index scan fails, fallback path exercised
- [ ] 9.4 Implement three benchmark operations for each file variant × library:
  - **open_size**: time from open to `try_get_size()` returning (index scan cost); report ms
  - **sequential**: read to EOF in 64 KB chunks; report MB/s
  - **seek_4x**: seek to 10 / 30 / 60 / 90 % offsets, read 1 MB at each; report total ms
- [ ] 9.5 Wire up three XZ libraries per variant:
  - `lzma.open()` — stdlib baseline (no seeking; mark seek_4x as N/A)
  - `python-xz` (`xz.open()`) — old library; skip gracefully if not installed, print a note
  - `XzDecompressorStream` — our implementation
- [ ] 9.6 Add tar.xz benchmark: create a tar.xz with 50 members × 2 MB each (100 MB total, multi-block); benchmark (a) extract member 0 (first), (b) extract member 49 (last); compare `python-xz` vs `XzDecompressorStream`; report seconds and MB decompressed
- [ ] 9.7 Add output: print a formatted markdown table per operation group; save full results to `benchmarks/results/YYYY-MM-DD.json`; include Python version, platform, and library versions in JSON header
