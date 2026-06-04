## 1. New xz_stream.py module — data structures and backward index scan

- [ ] 1.1 Create `src/archivey/formats/xz_stream.py` with `_XzBlockBounds` dataclass (fields: `compressed_start`, `decompressed_start`, `unpadded_size`, `uncompressed_size`, `check`); add `decompressed_end` property
- [ ] 1.2 Implement MBI helpers `_encode_mbi` / `_decode_mbi` (variable-length integer encoding used in XZ index); these can be thin wrappers or inline in the index parsing function
- [ ] 1.3 Implement `_parse_xz_index(data: bytes) -> list[tuple[int, int]]` — decodes MBI-encoded `(unpadded_size, uncompressed_size)` records from raw index bytes; verifies CRC32 and index indicator byte; raises `ArchiveCorruptedError` on any validation failure
- [ ] 1.4 Implement `_parse_xz_footer(data: bytes) -> tuple[int, int]` — returns `(check, backward_size)` from a 12-byte footer; verifies `YZ` magic and CRC32
- [ ] 1.5 Implement `_parse_xz_header(data: bytes) -> int` — returns `check` from a 12-byte header; verifies `\xfd7zXZ\x00` magic and CRC32
- [ ] 1.6 Implement `_read_xz_index_backwards(stream, file_size, stop_at=0, start_decompressed_offset=0) -> list[_XzBlockBounds]` — backward scan walking all streams; handles null padding; calls 1.3–1.5; raises `ArchiveCorruptedError` on any structural failure; returns blocks in forward order with correct absolute `compressed_start` and `decompressed_start`

## 2. _XzState — forward streaming state machine

- [ ] 2.1 Add `_XzState` class to `xz_stream.py` with states `NEED_HEADER` / `IN_STREAM`; internal fields: `_buf: bytearray`, `_dec: LZMADecompressor | None`, `_bytes_fed: int`, `_streams_seen: int`, `_finished: bool`
- [ ] 2.2 Implement `_XzState.feed(data) -> tuple[bytes, list[tuple[int, int]]]` — returns `(decompressed_bytes, new_streams)` where each entry in `new_streams` is `(decompressed_size, compressed_size)` for each completed stream; mirrors `_LzipState.feed` signature
- [ ] 2.3 Implement `_XzState.flush() -> tuple[bytes, list[tuple[int, int]]]` — handles end-of-input; succeeds if cleanly between streams (no bytes or non-XZ trailing bytes after at least one stream); raises `ArchiveEOFError` if truncated mid-stream; raises `ArchiveCorruptedError` if no streams seen at all
- [ ] 2.4 Implement `_XzState.is_finished() -> bool`
- [ ] 2.5 Handle stream padding in `NEED_HEADER`: when accumulated bytes start with `\x00`, consume 4-byte-aligned null runs before looking for the `\xfd7zXZ\x00` magic
- [ ] 2.6 Handle trailing non-XZ bytes after valid streams: detect in `NEED_HEADER`, set `_finished = True`, clear buffer (same graceful-stop as `_LzipState`)
- [ ] 2.7 Add `_XzBlockChain` class to `xz_stream.py`: takes a list of `_XzBlockBounds` (from the current block onward) and the inner stream; exposes `feed(chunk)`, `flush()`, and `is_finished()` with the same signature as `_XzState`; internally manages a per-block `LZMADecompressor` and synthetic stream wrapper; limits each `feed` call to at most `round_up(unpadded_size) - bytes_fed_so_far` bytes for the current block, injects synthetic header (pre-fed at block start) and footer (injected when block bytes are exhausted), then advances to the next block; `is_finished()` returns True only when the last block in the list is exhausted

## 3. XzDecompressorStream — DecompressorStream subclass

- [ ] 3.1 Add `XzDecompressorStream` class in `decompressor_stream.py`; pre-declare `_comp_cursor: int` and `_decomp_cursor: int` in `__init__` (same pattern as `LzipDecompressorStream`)
- [ ] 3.2 Implement `_create_decompressor(point: SeekPoint) -> _XzState | _XzBlockChain` — if `point.state` is `None`, return a fresh `_XzState` (stream-level fallback, only used for `SeekPoint(0, 0)`); if `point.state` is a `(check, unpadded_size, uncompressed_size)` tuple, collect all subsequent block-level seek points from `_seek_points` and return `_XzBlockChain(blocks_from_here, self._inner)`
- [ ] 3.3 Implement `_decompress_chunk(chunk: bytes) -> bytes` — call `self._decompressor.feed(chunk)`, call `_update_index(new_streams)` if decompressor is `_XzState`
- [ ] 3.4 Implement `_flush_decompressor() -> bytes` — call `self._decompressor.flush()`, call `_update_index(new_streams)` if decompressor is `_XzState`
- [ ] 3.5 Implement `_is_decompressor_finished() -> bool`
- [ ] 3.6 Implement `_update_index(new_streams: list[tuple[int, int]])`: for each completed stream, (a) add a stream-boundary `SeekPoint(decomp_cursor, comp_cursor, state=None)` (skipped for stream 0 since `SeekPoint(0, 0)` already covers it); (b) if inner is seekable and `_index_built` is False, save `_inner.tell()`, call `_read_xz_index_backwards` for just this stream's compressed range, add resulting block-level `SeekPoint`s, restore `_inner.tell()`; on `ArchiveCorruptedError`, log and skip; advance `_comp_cursor` and `_decomp_cursor`
- [ ] 3.7 Implement `_build_index(last_known: SeekPoint) -> tuple[list[SeekPoint], int | None]` — call `_read_xz_index_backwards(inner, file_size, stop_at=last_known.compressed_offset, start_decompressed_offset=last_known.decompressed_offset)`; on `ArchiveCorruptedError`, log warning and return `([], None)`; convert `_XzBlockBounds` to `SeekPoint` objects with block metadata in `.state`; return total decompressed size

## 4. Integration — compressed_streams.py and config

- [ ] 4.1 Add `open_xz_stream(path) -> BinaryIO` to `compressed_streams.py` that returns `XzDecompressorStream(path)`
- [ ] 4.2 Add `_translate_xz_exception(e) -> Optional[ArchiveError]` — maps `lzma.LZMAError` → `ArchiveCorruptedError`, `EOFError` → `ArchiveEOFError`
- [ ] 4.3 Update `get_stream_open_fn` for `StreamFormat.XZ`: always return `(open_xz_stream, _translate_xz_exception)`; remove the `config.use_python_xz` branch
- [ ] 4.4 Remove `open_python_xz_stream`, `_translate_python_xz_exception`, and the `xz` import guard from `compressed_streams.py`
- [ ] 4.5 Remove `use_python_xz` field from `ArchiveyConfig` in `config.py` and from `ConfigOverrides` TypedDict
- [ ] 4.6 Remove python-xz from optional dependencies in `pyproject.toml`; update `dependency_checker.py` if it references `xz`

## 5. Read file_size from stream in single_file_reader.py (XZ and lzip)

- [ ] 5.1 In `SingleFileReader.__init__`, after opening `self.fileobj`, if the stream is seekable: call `self.fileobj.seek(0, io.SEEK_END)` (triggers backwards index scan) then `self.fileobj.seek(0)`; set `member.file_size = self.fileobj._size`; apply for both `ArchiveFormat.XZ` and `ArchiveFormat.LZIP`
- [ ] 5.2 Remove `read_xz_metadata` function from `single_file_reader.py` entirely
- [ ] 5.3 Remove the now-unused `_read_xz_multibyte_integer` helper, `XZ_MAGIC_FOOTER`, and `XZ_STREAM_HEADER_MAGIC` constants from `single_file_reader.py`

## 6. Tests

- [ ] 6.1 Create `tests/archivey/test_xz_stream.py` mirroring the structure of `test_lzip_stream.py`; add helpers `make_multi_stream(parts)` (concatenates `lzma.compress(p, format=FORMAT_XZ)` for each part) and `open_xz(data)` / `open_xz_counting(data)`
- [ ] 6.2 Test basic read: single-stream, multi-stream, chunked reads
- [ ] 6.3 Test SEEK_END: correct total size returned, zero bytes decompressed, `seek(-N, SEEK_END)` reads last N bytes
- [ ] 6.4 Test forward seeking: seek past blocks, verify jump uses seek points (check `_decomp_cursor`)
- [ ] 6.5 Test backward seeking: seek backward to earlier block, verify nearest seek point used (not byte 0)
- [ ] 6.6 Test non-seekable stream: `seekable()` returns False, `read()` succeeds
- [ ] 6.7 Test stream padding: file with null padding between streams decompresses correctly
- [ ] 6.8 Test trailing non-XZ data: silently ignored; size and content correct
- [ ] 6.9 Test `_read_xz_index_backwards` directly: block offsets, multi-stream, corrupt footer, corrupt index CRC
- [ ] 6.10 Test corruption detection: bad magic, truncated mid-stream, bad index MBI
- [ ] 6.11 Test `SingleFileReader` size for multi-stream XZ file: `member.file_size` equals sum of both streams' sizes; also test lzip `file_size` is now populated
- [ ] 6.12 Update `test_missing_packages.py`: confirm python-xz absence no longer raises `PackageNotInstalledError` for XZ
- [ ] 6.13 Update any existing tests that set `use_python_xz=True` (search `test_open_compressed_stream.py` and others)

## 7. Cleanup and verification

- [ ] 7.1 Run the full test suite; confirm no regressions in existing XZ tests
- [ ] 7.2 Verify `pyproject.toml` has no remaining reference to `python-xz` or `xz` optional dep group
- [ ] 7.3 Update `CLAUDE.md` or changelog if the project maintains one; note `use_python_xz` removal as a breaking change
