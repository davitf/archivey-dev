## Why

The 7z reader reaches into py7zr's private surface for **metadata** (`ArchiveFile`,
`_get_property`, `SupportedMethods.needs_password`, the `archiveinfo()` empty-archive
crash guard) while py7zr also drives **decompression** through a push-based
`WriterFactory` API. To expose archivey's pull-based iterator on top of that push
model, the reader runs a background thread feeding two queues, plus a class-level
`_password_lock` + `_temporary_password` mutation, `reset()` before every extract, and
a duplicate-name round-trip map. All of that scaffolding exists *only* to bridge
py7zr's push API.

`docs/sevenzip-native-reader-design.md` shows that both halves are tractable to
replace. A native **header** parser is a few hundred lines, and — the key finding —
native **decompression** needs almost no new codec code: stdlib `lzma` (FORMAT_RAW)
already implements LZMA2/LZMA1 **and** the whole BCJ branch-filter family **and**
Delta (verified), `zlib`/`bz2` cover Deflate/BZip2, and Zstd/Brotli/AES are existing
optional deps. Only PPMd and Deflate64 need extra packages — and those already ship
transitively via py7zr. Driving decompression ourselves makes the iterator naturally
pull-based and deletes the entire thread/queue/lock apparatus.

## What Changes

- **Drop py7zr as a runtime dependency.** Replace it for metadata parsing *and*
  decompression. (The earlier metadata-only/phase-2 split is collapsed into this one
  change: keeping the split would leave the messy push-model scaffolding in place and
  require a second migration later — messier and slower than doing it once.)
- **Rollout follows the parallel-reader strategy**
  (`docs/format-architecture-comparison.md` §10, adopted 2026-06-10): the native
  reader is a **separate reader class** and the default; the existing py7zr-backed
  reader is kept reachable behind a transitional config flag and exercised by a
  **differential test** comparing both readers across the whole 7z corpus (member
  lists, all metadata fields, decompressed bytes, error types). Discrepancies are
  fixed or documented as intentional (the legacy reader can be wrong — e.g. py7zr
  mishandles LZMA1+IA64). py7zr leaves the `optional` extra now but stays a dev/test
  dependency; the legacy reader path and flag are deleted in a follow-up cleanup
  change once parity is confirmed.
- **New** `src/archivey/formats/sevenzip_parser.py`: `SevenZipParser` reading the
  signature header, (encoded/encrypted) end header, `FILES_INFO`, `PACK_INFO`,
  `UNPACK_INFO` (folders, coders, bind pairs), and `SUBSTREAMS_INFO`; plus a
  `SevenZipMemberInfo` dataclass (design §4.2) as the `raw_info` payload.
- **New** native decompression: a coder-chain → pipeline builder that reads a folder's
  packed bytes and exposes them through the existing `DecompressorStream` wrapper
  (pull-based). Solid folders are decompressed once and sliced by substream — O(N),
  no threads (design §4.3–§4.4). AES-256 is a per-folder decryptor stage (design §4.5).
- **New shared stream decompressors** in `src/archivey/formats/compressed_streams.py`:
  `pyppmd` (PPMd var.H) and `inflate64` (Deflate64), added alongside the existing
  gzip/bz2/lzma/zstd/brotli openers so they are **reusable by the planned native ZIP
  reader** (stdlib `zipfile` cannot do Deflate64 — ZIP method 9 raises
  `NotImplementedError`).
- **BCJ2** (`0x0303011B`): detect and raise a clean `UnsupportedCompressionMethodError`.
  This matches py7zr, which cannot decode BCJ2 either, so behavior is strictly
  equal-or-better than the current baseline.
- **Modified** `src/archivey/formats/sevenzip_reader.py`: the native reader is built
  fresh (no thread+queue extractor, no `_temporary_password` + class lock, no
  `reset()`, no `WriterFactory`/`StreamingFile` machinery — design §5); the legacy
  py7zr reader (with all that scaffolding) moves aside unchanged (e.g.
  `sevenzip_reader_legacy.py`) until the follow-up deletion change. Duplicate-name
  handling: registration-time renaming behavior is preserved; only the
  rename-reversal round-trip map (a py7zr artifact) disappears in the native path.
- **New metadata** the native parser exposes (design §4.7): per-member
  `compression_method` (typed primary codec, full filter chain in
  `compression_method_detail`); archive `comment`; `atime`/`ctime`.
- **Per-member passwords now work** (fixes a skipped test): the native per-folder AES
  decryptor is built from the `pwd` passed to `open(member, pwd=...)` (or the
  archive-wide default), the same per-call mechanism RAR and the other readers already
  use. py7zr's global `folder.password` mutation made differing per-member passwords
  impossible; the native design removes that limitation with no new public API.
- **Replaced internals**: `_is_member_encrypted` → direct coder-list check; `is_solid`
  from `num_unpackstreams_folders` directly; `filetime_to_dt` inlined.
- **Co-iteration cleanup (folded in)**: adopt the base
  `_iter_members_and_streams_internal` hook (overriding it instead of the public
  `iter_members_with_streams`), which is §8.A from
  `base-reader-architecture-extensions`. Behavior-preserving.

## Capabilities

### New Capabilities

- (none — the `sevenzip-format` capability is preserved and extended with additive
  metadata requirements; the package requirement is replaced, not removed.)

### Modified Capabilities

- `sevenzip-format`: the py7zr package requirement is **replaced** by native
  decompression plus standard codec packages (PPMd/Deflate64/Zstd/Brotli/AES gated on
  their own optionals); member metadata gains `compression_method`; archive metadata
  gains `comment`.

## Non-Goals

- A native **BCJ2** decoder. Detect-and-raise matches py7zr; a real decoder (or
  external `7z` fallback) is deferred.
- Newer **ARM64 / RISC-V** BCJ filters not exposed by the installed liblzma:
  detect-and-raise until a newer liblzma or fallback is available.
- Multi-volume 7z and anti-file support (raise a clean error / warn).
- Changing the public `ArchiveReader` API (per-member passwords use the existing
  per-call `pwd` parameter — no new surface).

## Dependencies / Sequencing

**Land third** (in parallel with `rar-native-metadata-reader`, after
`base-reader-architecture-extensions`).

- `test-suite-parametrization` should land first so new parser/decompression tests
  benefit from the declarative harness.
- `base-reader-architecture-extensions` §8.D (the `CompressionMethod` enum) must land
  first: the native parser emits `compression_method` as the typed enum, and §8.E's
  `AccessCost` is how solid folders report `EXPENSIVE`.
- The §8.A co-iteration migration is included here (task 2.x) and does not need a
  separate change.
- `unify-junction-handling` comes after: 7z junction detection will be wired into the
  native parser built here.

## Impact

- **Files**: new `formats/sevenzip_parser.py` (+ optional `formats/sevenzip_codecs.py`
  for the pipeline builder/AES stage); `formats/compressed_streams.py` (shared
  `pyppmd`/`inflate64` openers); `formats/sevenzip_reader.py` (native decompression,
  delete thread/queue/lock); `internal/dependency_checker.py` (drop py7zr; gate
  PPMd/Deflate64/AES/zstd/brotli on their own packages); `pyproject.toml` (remove
  `py7zr`, add `pyppmd` + `inflate64` to `optional` and `optional-freethreaded`).
- **Live specs touched**: `sevenzip-format` (and `archive-metadata` is already broad
  enough to cover the newly-populated fields).
- **Risk**: the coder-chain → pipeline builder, pack-stream location, AES stage, and
  PPMd/Deflate64 wrappers are the new logic; the LZMA1+BCJ special case and
  BCJ2/ARM64 detection are the sharp edges. All existing 7z test archives must keep
  passing. See `docs/sevenzip-native-reader-design.md` §7.
- **Design reference**: `docs/sevenzip-native-reader-design.md`.
