# Design

The full design lives in **`docs/sevenzip-native-reader-design.md`** (7z header
structures, folder/coder model, encoded/encrypted header decode, AES key derivation,
the codec landscape table, the native decompression pipeline, and the risk list).
This file records only what affects the live specs.

## Scope decision: full native reader (py7zr removed)

Replace py7zr for **both** metadata parsing **and** decompression, dropping the
dependency entirely. The earlier plan kept py7zr for decompression as a "phase 2"; we
collapse that into one change because keeping a metadata/decompression split would
leave the messy push-model scaffolding (background thread, two queues,
`_temporary_password` + class lock, `reset()`, the duplicate-name round-trip map) in
place and add a second migration later. One change is cleaner and faster overall.

This is feasible because the codec inventory mostly already exists in archivey
(design §4.3):

- **stdlib covers the common path**: `lzma` FORMAT_RAW handles LZMA2/LZMA1 *and* the
  BCJ branch-filter family (x86/ARM/ARMT/PPC/SPARC/IA64) *and* Delta — verified — so
  the 7z "preprocessors" need no bespoke code; `zlib`/`bz2` cover Deflate/BZip2.
- **existing optionals cover the rest of the common path**: `zstandard`, `brotli`,
  and the `cryptography`/`pycryptodome` already pulled in for RAR (AES-256).
- **two new optionals** for what stdlib lacks: `pyppmd` (PPMd var.H) and `inflate64`
  (Deflate64). Both already ship transitively via py7zr, so this is re-labeling, not
  added weight. They are added as **shared stream decompressors** in
  `compressed_streams.py`, not 7z-local, because the planned native ZIP reader needs
  `inflate64` too (stdlib `zipfile` raises `NotImplementedError` on ZIP method 9).
- **BCJ2** (`0x0303011B`) is the one coder we don't support — and **py7zr doesn't
  either** (it raises). We detect it and raise the same kind of clean error, so we
  are strictly equal-or-better than the py7zr baseline.

## Spec cross-check (docs vs live `sevenzip-format` spec)

| Live requirement | Effect |
|---|---|
| 7z requires a seekable source | unchanged |
| reports solidity and encryption | unchanged (computed directly: `num_unpackstreams_folders`; coder-list check) |
| member metadata mapped | **extended** — add `compression_method` |
| link targets resolved during reading | unchanged |
| read/extracted via a batch model | **changed** — native pull-based per-folder decompression replaces the py7zr push/thread model (still O(N) for solid, design §4.4) |
| 7z errors are translated | unchanged |
| **7z requires the py7zr package** | **removed** — replaced by native decompression + standard codec packages |

## Decisions

- **py7zr is dropped entirely.** Member data is decompressed by a native pipeline
  built from each folder's coder chain and exposed through the existing
  `DecompressorStream` wrapper (pull-based), eliminating the thread, the queues, the
  `_temporary_password` lock, `reset()`, and the duplicate-name map (design §5).
- **Codecs reuse existing infrastructure.** LZMA2/LZMA1/BCJ/Delta via stdlib `lzma`;
  Deflate/BZip2 via stdlib; Zstd/Brotli/AES via existing optionals; PPMd/Deflate64 via
  new optional packages exposed as **shared** decompressors in `compressed_streams.py`.
- **BCJ2: detect and raise.** Acceptable because it matches py7zr (which cannot decode
  BCJ2). Revisit later (a dedicated BCJ2 decoder, or external `7z` for that case) if
  real archives need it.
- **Per-folder AES decryptors** remove the password lock and **fix the skipped
  multi-password test** using the existing per-call `pwd` parameter — no new API. The
  decryptor is built from the `pwd` passed to `open(member, pwd=...)` (or the
  archive-wide default), so members needing different passwords just work, unlike
  py7zr's global `folder.password` mutation (design §4.5).
- **`compression_method`**: typed primary codec onto the §8.D `CompressionMethod`
  enum (e.g. `LZMA2`), full chain (e.g. `"LZMA2 + BCJ"`) into
  `compression_method_detail`. Depends on `base-reader-architecture-extensions` §8.D.
- **Archive `comment`** from `FILES_INFO` (0x16), surfaced in `ArchiveInfo.comment`.
- **`atime`/`ctime`** populated from `FILES_INFO` when present (design §4.7).
- **Empty-archive / multi-volume / anti-files**: parser handles empty `FILES_INFO`
  directly, raises a clean `ArchiveError` for multi-volume, warns on anti-files.
- **§8.A co-iteration**: this change rewrites the reader's iteration anyway, so it
  adopts the base `_iter_members_and_streams_internal` hook (dropping the public
  `iter_members_with_streams` override).
- **`raw_info`** becomes `SevenZipMemberInfo`; with native decompression the reader
  maps members to `(folder_index, file_in_folder)` directly — no py7zr extract-target
  filenames and no dedup round-trip.
