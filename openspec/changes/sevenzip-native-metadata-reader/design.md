# Design

The full design lives in **`docs/sevenzip-native-reader-design.md`** (7z header
structures, folder/coder model, encoded/encrypted header decode, AES key derivation,
the `SevenZipMemberInfo` dataclass, and the risk list). This file records only what
affects the live specs.

## Scope decision: metadata-only (phase 1)

Replace py7zr for header parsing; keep py7zr for decompression (design §4.1, §4.3).
The solid-extraction strategy is already O(N) via py7zr's `extract(targets=[...])`
(design §4.4), so there is nothing to gain by touching the decompression path now.

## Spec cross-check (docs vs live `sevenzip-format` spec)

Most live requirements are **unchanged** by a native parser:

| Live requirement | Effect |
|---|---|
| 7z requires a seekable source | unchanged |
| reports solidity and encryption | unchanged (computed directly: `num_unpackstreams_folders`; coder-list check) |
| member metadata mapped | **extended** — add `compression_method` |
| link targets resolved during reading | unchanged |
| read/extracted via a batch model | unchanged (py7zr extraction retained) |
| 7z errors are translated | unchanged |
| **7z requires the py7zr package** | **narrowed** to decompression — see delta |

New additive requirements: `compression_method` per member, archive `comment`.

## Decisions

- **`compression_method`**: derived from the folder's coder chain (e.g. `0x21` →
  `LZMA2`, prepended `BCJ`/`Delta`), giving values like `"LZMA2"` or `"LZMA2 + BCJ"`.
  Pairs naturally with the typed `CompressionMethod` enum proposed in
  `base-reader-architecture-extensions` (§8.D) but does not depend on it.
- **Archive `comment`**: read from `FILES_INFO` (0x16) which py7zr ignores; surfaced
  in `ArchiveInfo.comment`.
- **`atime`/`ctime`**: populated from `FILES_INFO` when present (design §4.7 gap).
- **Empty-archive / multi-volume / anti-files**: parser handles the empty
  `FILES_INFO` case directly, and raises a clean `ArchiveError` for multi-volume;
  anti-files are warned about (design §7.3–§7.5).
- **`raw_info`** becomes `SevenZipMemberInfo`; the extractor only needs `filename`
  (for building py7zr extract targets) plus the dedup `extract_filename` (design §4.2).
