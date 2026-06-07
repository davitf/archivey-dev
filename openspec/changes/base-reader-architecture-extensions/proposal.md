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

- **§8.B — format capability vs user preference** *(internal)*: add a
  `_format_supports_random_access` flag so "format cannot random-access" (a
  non-seekable compressed TAR) is distinct from "user asked for streaming". This is
  **per-instance, not a ClassVar** — whether a compressed TAR can random-access
  depends on the runtime stream/backend (stdlib gzip on a pipe is non-seekable;
  stdlib on a file rewinds; rapidgzip/indexed_bzip2 are always seekable), so it is
  set in `__init__`. The runtime streaming flag becomes "user requested OR format
  can't".
- **§8.C — `members_list_supported` as a ClassVar** *(internal)*: it's a format-level
  fact, so declare it per reader class instead of passing it through `__init__`.
- **§8.D — typed `CompressionMethod` enum + lossless detail** *(public)*: a `StrEnum`
  of known methods (`STORED`, `DEFLATE`, `LZMA`, `LZMA2`, `ZSTD`, `BZIP2`, `PPMD`,
  `BCJ2`, …, plus `UNKNOWN`) so callers can branch on compression without parsing
  free-form strings. Stays string-compatible. `compression_method` holds the typed
  **primary** codec (`UNKNOWN` if reported-but-unmapped, `None` if unreported); a new
  free-form `compression_method_detail: Optional[str]` preserves the full,
  lossless description — 7z filter chains (`"LZMA2 + BCJ2"`) and third-party readers'
  own codec names that a closed enum can't represent.
- **§8.E — capability introspection (redesigned surface)** *(public)*: replace the
  confusing tangle of `streaming` / `has_random_access()` / "member list supported"
  with two clearly-scoped introspection properties on two independent axes:
  - `supports_random_access: bool` — can members be opened individually / out of
    order (seekable source + format support + not streaming). **Replaces**
    `has_random_access()` (one name for one concept).
  - `member_listing: MemberListing` — a 3-state enum (`INDEXED` / `SCAN_REQUIRED` /
    `SEQUENTIAL_ONLY`) for *how cheaply* the full member list can be obtained, so the
    "one bounded seek (ZIP catalog)" case is no longer conflated with the "O(N) full
    pass (seekable TAR)" case. `get_members_if_available()` is tightened to return the
    list only for `INDEXED` (or already-known) and never to trigger a scan.

## Capabilities

### New Capabilities

- (none new; refines existing capabilities)

### Modified Capabilities

- `archive-reading`: adds the `MemberListing` enum and the `supports_random_access`
  / `member_listing` introspection properties, tightens `get_members_if_available`
  to never scan, and removes `has_random_access()` (superseded) (§8.E).
- `archive-metadata`: adds the typed `CompressionMethod` enum and the lossless
  `compression_method_detail` field (§8.D).

## Non-Goals

- Any change to what archives can be read or how members decode (§8.B/C are
  internal refactors that keep observable behavior identical).
- The §8.A co-iteration migration — folded into the native-reader changes, since
  those rewrite the same `sevenzip_reader.py` / `rar_reader.py` iteration code.
- A "how expensive is *content* access" type (solid-archive decompression cost, the
  TAR scan cost beyond the `SCAN_REQUIRED` flag): these are documented as cost
  caveats rather than encoded into the introspection surface, to keep it coarse and
  explainable.

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

- **Files**: `internal/base_reader.py` (per-instance random-access flag,
  `member_listing`, introspection properties, tightened `get_members_if_available`,
  removed `has_random_access`), `formats/*_reader.py` (`members_list_supported`
  ClassVar, `member_listing` reporting), `types.py` (`CompressionMethod`,
  `compression_method_detail`, `MemberListing`), `archive_reader.py` (property
  declarations).
- **Live specs touched**: `archive-reading`, `archive-metadata`.
- **Design reference**: `docs/format-architecture-comparison.md` §8–§9.
