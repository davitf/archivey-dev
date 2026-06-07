## Why

The 7z reader reaches into py7zr's private surface for **metadata**
(`ArchiveFile`, `_get_property`, `SupportedMethods.needs_password`, the
`archiveinfo()` empty-archive crash guard) while py7zr also drives decompression.
`docs/sevenzip-native-reader-design.md` shows that a native 7z **header** parser is
tractable and, beyond removing the private-API coupling, unlocks metadata py7zr
discards today: per-member `compression_method`, archive `comment`, and
access/creation times.

This change replaces py7zr for **metadata parsing only**. Decompression continues to
use py7zr's `extract()` + `StreamingFactory` machinery (already O(N) for solid
archives — design §4.4), so the riskier filter/AES decompression code is untouched.

## What Changes

- **New** `src/archivey/formats/sevenzip_parser.py`: `SevenZipParser` reading the
  signature header, (encoded/encrypted) end header, `FILES_INFO`, and
  `MAIN_STREAMS_INFO` (folders, coders, substreams); plus a `SevenZipMemberInfo`
  dataclass (design §4.2) as the `raw_info` payload.
- **Modified** `src/archivey/formats/sevenzip_reader.py`: build the member list from
  the native parser; keep the thread+queue streaming extractor, `_temporary_password`,
  and duplicate-name mapping (still py7zr-driven for decompression).
- **New metadata** the native parser exposes (design §4.7):
  - per-member `compression_method` (e.g. `"LZMA2"`, `"LZMA2 + BCJ"`) — currently `None`;
  - archive `comment` — currently discarded by py7zr;
  - `atime`/`ctime` from `FILES_INFO` when present.
- **Replaced internals**: `_is_member_encrypted` (private py7zr API) becomes a direct
  coder-list check; `is_solid` comes from `num_unpackstreams_folders` directly
  (drops the empty-archive crash guard); `filetime_to_dt` is inlined.
- **Dependency shift**: py7zr is no longer needed for metadata; it remains required
  for decompression (phase 1). Removing py7zr entirely (external `7z`/native
  decompression) is a documented phase 2, out of scope here.
- **Co-iteration cleanup (folded in)**: since this change rewrites the reader's
  iteration anyway, it also adopts the base `_iter_members_and_streams_internal`
  hook (overriding it instead of the public `iter_members_with_streams`), which is
  §8.A from `base-reader-architecture-extensions`. Behavior-preserving.

## Capabilities

### New Capabilities

- (none — the `sevenzip-format` capability is preserved and extended with additive
  metadata requirements)

### Modified Capabilities

- `sevenzip-format`: the package requirement is scoped to decompression; member
  metadata gains `compression_method`; archive metadata gains `comment`.

## Non-Goals

- Replacing py7zr for **decompression** (LZMA2/BCJ/Delta filter chain, AES) — that is
  phase 2 in the design doc.
- Multi-volume 7z and anti-file support (raise a clean error / warn).
- Changing the public `ArchiveReader` API.

## Impact

- **Files**: new `formats/sevenzip_parser.py`; `formats/sevenzip_reader.py`;
  `internal/dependency_checker.py` (py7zr = decompression-only); no `pyproject.toml`
  change in phase 1 (py7zr still required for decompression).
- **Live specs touched**: `sevenzip-format` (and `archive-metadata` is already broad
  enough to cover the newly-populated fields).
- **Risk**: encoded/encrypted header decode and the coder-id → method-name mapping are
  the new logic; all existing 7z test archives must keep passing. See
  `docs/sevenzip-native-reader-design.md` §7.
- **Design reference**: `docs/sevenzip-native-reader-design.md`.
