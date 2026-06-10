## Why

The RAR reader depends on `rarfile` purely for **metadata parsing** — it already
delegates all decompression to the `unrar` binary (directly via `RarStreamReader`,
or through `rarfile` which itself shells out to `unrar`). `rarfile` is a heavy,
GPL-adjacent dependency whose internals we already reach into (private
`file_redir`, `file_encryption`, `_file_parser.has_header_encryption()`), and it
brings RAR2/RARVM/multi-tool machinery we don't use.

`docs/rar-native-reader-design.md` shows that a native RAR3/RAR5 header parser is
tractable (~the structures `rarfile` exposes), and would let us drop `rarfile`
from the dependency set while preserving every behavior the live `rar-format`
spec already requires. Decompression stays exactly as it is today (`unrar`).

## What Changes

- **New** `src/archivey/formats/rar_parser.py`: `NativeRar3Parser`,
  `NativeRar5Parser`, and a `RarMemberInfo` dataclass replacing `Rar3Info`/`Rar5Info`
  as the `raw_info` payload. Parses the block sequence, decodes all member metadata,
  detects `is_solid` / `needs_password` / header encryption, derives the AES key and
  decrypts headers when a password is supplied.
- **Modified** `src/archivey/formats/rar_reader.py`: build the member list from the
  native parser instead of `rarfile.RarFile`. Keep `RarStreamReader`,
  `RarStreamMemberFile`, and the existing CRC/encryption helpers
  (`verify_rar5_password`, `convert_crc_to_encrypted`, `RarEncryptionInfo`) — they
  only need the encryption tuple, which the native parser provides as a first-class
  field.
- **Decompression unchanged**: stored members read raw bytes; everything else uses
  `unrar`. The extract-hack (minimal temp archive per member) is re-implemented
  natively using stored block offsets.
- **Rollout follows the parallel-reader strategy**
  (`docs/format-architecture-comparison.md` §10, adopted 2026-06-10): the native
  parser backs a **separate reader path**, default-on, with the rarfile-backed reader
  kept reachable behind a transitional config flag and exercised by a **differential
  test** across the whole RAR corpus (member lists, all metadata fields, decompressed
  bytes, error types). Discrepancies are fixed or documented as intentional. The
  legacy path + flag are deleted in a follow-up change once parity is confirmed.
- **Dependency shift**: `rarfile` is removed from the `optional` extra (kept as a
  dev/test dependency for the differential tests until the follow-up deletion
  change); the `unrar` binary becomes the documented requirement for decompression; a
  cryptography backend remains required only for encrypted headers.
- **New behaviors** the native parser makes explicit: clean `ArchiveError` for
  multi-volume and RAR2 archives instead of relying on `rarfile`; Blake2sp-only RAR5
  members report `crc32 = None`.
- **Co-iteration cleanup (folded in)**: while in this reader, the `use_rar_stream`
  solid path adopts the base `_iter_members_and_streams_internal` hook (instead of
  overriding the public `iter_members_with_streams`), which is §8.A from
  `base-reader-architecture-extensions`. Behavior-preserving.

This is a **metadata-only** rewrite (design §4.1). Decompression backends are out of
scope.

## Capabilities

### New Capabilities

- (none — behavior of the `rar-format` capability is preserved; this is an
  implementation change with a few additive edge-case requirements)

### Modified Capabilities

- `rar-format`: the package requirement flips from `rarfile` to the `unrar` binary;
  multi-volume / RAR2 / Blake2sp edge cases get explicit requirements.

## Non-Goals

- Replacing `unrar` for decompression (a native RAR decompressor / RARVM).
- Multi-volume RAR support (still raises a clean error; just no longer via `rarfile`).
- Any change to the public `ArchiveReader` API or `ArchiveMember` fields.

## Dependencies / Sequencing

**Land third** (in parallel with `sevenzip-native-reader`, after
`base-reader-architecture-extensions`).

- `test-suite-parametrization` should land first so new parser tests benefit from
  the declarative harness.
- `base-reader-architecture-extensions` §8.D (the `CompressionMethod` enum) is
  independent of RAR but good to have available before writing new parsers.
- The §8.A co-iteration migration (task 2.5) is included here and does not need a
  separate change.
- `unify-junction-handling` comes after: RAR junction detection will be wired into
  the native parser built here rather than the old rarfile facade.

## Impact

- **Files**: new `formats/rar_parser.py`; `formats/rar_reader.py`;
  `internal/dependency_checker.py` (rarfile optional, unrar required);
  `pyproject.toml` (drop `rarfile`).
- **Live spec touched**: `rar-format`.
- **Risk**: header-encryption key derivation and RAR3 Unicode-name decoding are the
  trickiest pieces (design §4.3, §4.6); all existing RAR test archives must keep
  passing, plus new parser unit tests. See `docs/rar-native-reader-design.md` §7 for
  the full risk list.
- **Design reference**: `docs/rar-native-reader-design.md`.
