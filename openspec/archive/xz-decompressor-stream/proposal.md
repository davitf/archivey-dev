## Why

XZ files have the same multi-stream structure as lzip: each stream ends with a footer and an index that records every block's compressed and uncompressed sizes, enabling size calculation and random access without decompression. The current default (`lzma.open`) cannot seek efficiently — SEEK_END requires decompressing the entire file. The optional `python-xz` library fixes this but adds an external dependency and requires a seekable stream. We can implement an `XzDecompressorStream` in our existing `DecompressorStream` framework that eliminates both limitations, exactly as we did for lzip.

## What Changes

- **New**: `XzDecompressorStream` — a `DecompressorStream` subclass for XZ that supports efficient SEEK_END, backward seeks, and multi-stream random access with no external dependency
- **New**: `_XzState` — streaming state machine (NEED_HEADER → IN_STREAM cycle) analogous to `_LzipState`; handles multi-stream concatenation and 4-byte-aligned stream padding
- **New**: `_read_xz_index_backwards()` — backward index scanner analogous to `_read_index_backwards` for lzip; reads footers + MBI-encoded block indices without decompression
- **New**: Block-level seek points via `SeekPoint.state` carrying `(check, unpadded_size, uncompressed_size)`; uses synthetic XZ stream wrapper for per-block decompression (same trick python-xz uses)
- **Modified**: `get_stream_open_fn` for `StreamFormat.XZ` — uses `XzDecompressorStream` always; `use_python_xz` flag deprecated/removed
- **Modified**: `read_xz_metadata` in `single_file_reader.py` — rewritten to use `_read_xz_index_backwards`, fixing the multi-stream bug where only the last stream's size was counted
- **Removed**: `open_python_xz_stream` and `_translate_python_xz_exception` (python-xz library support dropped)
- `use_python_xz` config field: **BREAKING** — removed from `ArchiveyConfig`; setting it raises a deprecation warning or error

## Capabilities

### New Capabilities

- `xz-decompressor-stream`: Seekable XZ decompressor stream with backwards index scan, stream-level and block-level random access, and correct multi-stream size reporting

### Modified Capabilities

- (none — existing XZ stream behaviour is preserved; the new stream is a drop-in replacement)

## Impact

- **Files changed**: `formats/decompressor_stream.py`, `formats/compressed_streams.py`, `formats/single_file_reader.py`, `config.py`
- **Dependency removed**: `python-xz` (optional dep in `pyproject.toml`)
- **Config API**: `use_python_xz` field removed from `ArchiveyConfig`
- **Test additions**: `tests/archivey/test_xz_stream.py` (mirroring `test_lzip_stream.py`)

---

## Archive Information

**Archived:** 2026-06-06
**Outcome:** Successfully implemented

### Specs Updated
- `openspec/specs/xz-decompressor-stream/spec.md` — complete live spec created

### Implementation Summary
All 9 task groups (94 tasks) completed:
1. `xz_stream.py` — `_XzBlockBounds`, MBI helpers, XZ index/footer/header parsers, `_read_xz_index_backwards`
2. `_XzState` — streaming state machine (NEED_HEADER ↔ IN_STREAM)
3. `_XzBlockChain` — block-level decompressor using synthetic XZ stream wrapper
4. `XzDecompressorStream` — subclass of `_SegmentedDecompressorStream`
5. `compressed_streams.py` + config — wired XzDecompressorStream as default; python-xz as optional backend
6. `single_file_reader.py` — `file_size` via stream seek, removed `read_xz_metadata`
7. Tests — `test_xz_stream.py` + all integration test updates
8. Refactor — `_SegmentedDecompressorStream` base class; Lzip/XZ stream classes co-located with format modules
9. Benchmarks — `benchmarks/bench_xz.py`
