## Why

`docs/format-architecture-comparison.md` §8 identifies five places where the
`BaseArchiveReader` contract creaks once all formats (incl. the native RAR/7z
readers) are in place. None changes externally-observable archive behavior much,
but together they clean up the reader contract and make a couple of useful
capabilities first-class for callers. §9 sequences these *after* the native readers.

## What Changes

- **§8.A — solid-archive co-iteration hook** *(internal)*: add a protected
  `_iter_members_and_streams()` that `iter_members_with_streams()` calls, with a base
  implementation that opens each file member. Solid readers (7z, RAR `use_rar_stream`)
  override this one hook instead of overriding the whole public method, removing the
  duplicated registration/iteration/filter logic.
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

- `archive-reading`: adds capability-introspection properties (§8.E); the
  co-iteration refactor (§8.A) preserves existing observable behavior.
- `archive-metadata`: adds the typed `CompressionMethod` enum (§8.D).

## Non-Goals

- Any change to what archives can be read or how members decode (§8.A/B/C are
  internal refactors that keep observable behavior identical).
- Renaming or removing `has_random_access()` (the new properties complement it).

## Dependencies / Sequencing

Per §9, land **after** `rar-native-metadata-reader` and
`sevenzip-native-metadata-reader` so the co-iteration hook (§8.A) can unify both
solid-archive paths in their final form. §8.D/§8.E are independent and could ship
earlier.

## Impact

- **Files**: `internal/base_reader.py` (hook, ClassVars, properties),
  `formats/*_reader.py` (adopt the hook + ClassVars), `types.py` (`CompressionMethod`),
  `archive_reader.py` (property declarations).
- **Live specs touched**: `archive-reading`, `archive-metadata`.
- **Design reference**: `docs/format-architecture-comparison.md` §8–§9.
