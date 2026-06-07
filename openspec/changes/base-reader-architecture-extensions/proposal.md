## Why

`docs/format-architecture-comparison.md` §8 identifies five places where the
`BaseArchiveReader` contract creaks once all formats (incl. the native RAR/7z
readers) are in place. None changes externally-observable archive behavior much,
but together they clean up the reader contract and make a couple of useful
capabilities first-class for callers.

This change covers the four contract/spec items **§8.B–§8.E**. The fifth, **§8.A**
(migrating the 7z/RAR solid readers onto the existing
`_iter_members_and_streams_internal()` hook), is a pure internal refactor with no
spec delta, so it is **folded into the native-reader changes**
(`rar-native-metadata-reader` / `sevenzip-native-metadata-reader`), which already
rewrite those files — see their tasks.

**Current state (verified):** §8.B–E are not yet implemented —
`compression_method` is still a plain `str`, `members_list_supported` is still a
constructor argument, and there is no `_format_supports_random_access` ClassVar or
`supports_*` properties (only the existing `has_random_access()` method).

## What Changes

- **§8.B — format capability vs user preference** *(internal)*: add
  `_format_supports_random_access` (ClassVar) so "format cannot random-access" (a
  non-seekable compressed TAR) is distinct from "user asked for streaming". The
  runtime `streaming_only` becomes "user requested OR format can't".
- **§8.C — `members_list_supported` as a ClassVar** *(internal)*: it's a format-level
  fact, so declare it per reader class instead of passing it through `__init__`.
- **§8.D — typed `CompressionMethod` enum** *(public)*: a `StrEnum` of known methods
  (`STORED`, `DEFLATE`, `LZMA`, `LZMA2`, `ZSTD`, `BZIP2`, `PPMD`, `BCJ2`, …, plus
  `UNKNOWN`) so callers can branch on compression without parsing free-form strings.
  Stays string-compatible.
- **§8.E — capability introspection** *(public)*: `supports_random_access` and
  `supports_member_list` properties so callers stop probing-and-catching `ValueError`.

## Capabilities

### New Capabilities

- (none new; refines existing capabilities)

### Modified Capabilities

- `archive-reading`: adds capability-introspection properties (§8.E).
- `archive-metadata`: adds the typed `CompressionMethod` enum (§8.D).

## Non-Goals

- Any change to what archives can be read or how members decode (§8.B/C are
  internal refactors that keep observable behavior identical).
- The §8.A co-iteration migration — folded into the native-reader changes, since
  those rewrite the same `sevenzip_reader.py` / `rar_reader.py` iteration code.
- Renaming or removing `has_random_access()` (the new properties complement it).

## Dependencies / Sequencing

**Land second** (after `test-suite-parametrization`, before the native readers).

§8.D (the `CompressionMethod` enum) must land **before** the 7z native reader so
that reader can emit typed compression methods directly. §8.B/§8.C/§8.E are
independent and can ship anytime. (The §8.A migration lives in the native-reader
changes.)

Recommended order across all pending changes:
1. `test-suite-parametrization` — verification harness
2. **this change** — §8.B–§8.E (§8.D enum prerequisite for 7z native)
3. `rar-native-metadata-reader` + `sevenzip-native-metadata-reader` (in parallel; also run junction Windows spike)
4. `unify-junction-handling` — after native readers (junction detection in native parsers)

## Impact

- **Files**: `internal/base_reader.py` (ClassVars, properties),
  `formats/*_reader.py` (`members_list_supported` ClassVar), `types.py`
  (`CompressionMethod`), `archive_reader.py` (property declarations).
- **Live specs touched**: `archive-reading`, `archive-metadata`.
- **Design reference**: `docs/format-architecture-comparison.md` §8–§9.
