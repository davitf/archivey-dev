## Why

`docs/format-architecture-comparison.md` §8 identifies five places where the
`BaseArchiveReader` contract creaks once all formats (incl. the native RAR/7z
readers) are in place. None changes externally-observable archive behavior much,
but together they clean up the reader contract and make a couple of useful
capabilities first-class for callers.

This change covers the four contract/spec items **§8.B–§8.E**. The fifth original
item, **§8.A** (migrating the 7z/RAR solid readers onto the existing
`_iter_members_and_streams_internal()` hook), is a pure internal refactor with no spec
delta, so it is **folded into the native-reader changes**
(`rar-native-metadata-reader` / `sevenzip-native-reader`), which already rewrite those
files — see their tasks. The originally-grouped **§8.F — access intent** is split into
its own **`access-intent`** change: it is a larger, externally-facing redesign (a new
open-time input, removal of the `streaming` parameter, tri-state backend config) that
*builds on* the cost surface added here, so the two land in sequence rather than as one.

§8.E exposes **cost introspection** (how expensive is it to list members / reach a
member / seek within one). This is the foundation the `access-intent` change builds on:
the cost properties are the **receipt** (what archivey actually achieved), and the
later `access_intent` input is the **request** (how the caller intends to use the
archive). Today both are missing — to *get* cheap random access on a `tar.gz` you must
already know to set `use_rapidgzip=True`, so the low-level backend flags
(`use_rapidgzip`, `use_indexed_bzip2`, `use_python_xz`, …) leak archivey's cost model.
This change adds the receipt; the request follows in `access-intent`.

**Current state (verified):** §8.B–E are not yet implemented —
`compression_method` is still a plain `str`, `members_list_supported` is still an
`__init__` argument, and there is no `_format_supports_random_access` flag or
`*_cost` properties (only the existing `has_random_access()` method).

## What Changes

- **§8.B — format capability vs user preference** *(internal)*: add a
  `_format_supports_random_access` flag so "format cannot random-access" (a
  non-seekable compressed TAR) is distinct from "user asked for streaming". This is
  **per-instance, not a ClassVar** — whether a compressed TAR can random-access
  depends on the runtime stream/backend (stdlib gzip on a pipe is non-seekable;
  stdlib on a file rewinds; rapidgzip/indexed_bzip2 are always seekable), so it is
  set in `__init__`. The runtime streaming flag becomes "user requested OR format
  can't".
- **§8.C — catalog location (replaces the `members_list_supported` argument)**
  *(internal)*: *where* an archive keeps an upfront member catalog — header-resident
  (read while streaming forward), tail-resident (ZIP's end-of-file central directory), a
  single-known-member stream, or none (members found only by scanning, as in TAR) —
  decides whether it is reachable without seeking. This is richer than PR #221's bool
  `has_central_directory`. Crucially it is **not purely a `ClassVar`**: for some formats
  the location is per-file. RAR is the case — member headers precede each file (no front
  index), and an upfront catalog exists only when the optional "quick open" service block
  (an index at the *end*) is present, so a given `.rar` is tail-indexed or scan-only
  depending on the file. A `ClassVar` carries a baseline where the layout is uniform
  (ZIP=TRAILER, TAR=NONE); otherwise the reader determines the location at open. The
  *realized* listing cost is computed **per instance** from that location **and**
  seekability (§8.E): `INDEXED` when the catalog (or single member) is reachable without
  a backward seek, so a `.gz` single-member stream is `INDEXED` even on a pipe. (PR #221
  derived `INDEXED` from a "has a catalog" bool only — wrong on all of seekability,
  location, and per-file variation.) The standalone `members_list_supported` boolean is
  dropped; it is now exactly `member_listing_cost == INDEXED`.
- **§8.D — typed `CompressionMethod` enum + lossless detail** *(public)*: a `StrEnum`
  of known codecs (plus `UNKNOWN`) so callers can branch on compression without parsing
  free-form strings. Stays string-compatible. The enum is **exhaustive over the formats
  Archivey handles, not a sample**: it carries a value for **every `StreamFormat`** —
  `STORED`, `DEFLATE` (gzip/zlib), `BZIP2`, `LZMA2`/`XZ`, `LZMA` (lzip), `ZSTD`, `LZ4`,
  `BROTLI`, `LZW` (Unix `compress`) — **plus** container-internal codecs (`PPMD`, `BCJ2`,
  `DEFLATE64`, `DELTA`, …), with a test asserting every `StreamFormat` maps to a
  non-`UNKNOWN` value (PR #221 only added the example codecs and missed e.g. brotli).
  `compression_method` holds the typed **primary** codec (`UNKNOWN` if
  reported-but-unmapped, `None` if unreported); a new free-form
  `compression_method_detail: Optional[str]` preserves the full, lossless description —
  7z filter chains (`"LZMA2 + BCJ2"`) and third-party readers' own codec names that a
  closed enum can't represent.
- **§8.E — cost introspection (redesigned surface)** *(public)*: replace the
  confusing tangle of `streaming` / `has_random_access()` / "member list supported"
  with introspection on two orthogonal axes, each a cost-classifying enum so the
  surface never lies about expense the way a boolean does:
  - `member_listing_cost: MemberListingCost` (`INDEXED` / `SCAN_REQUIRED` / `SEQUENTIAL_ONLY`)
    — *how cheaply the full member list is obtainable*, so "one bounded seek (ZIP
    catalog)" is no longer conflated with "O(N) full pass (seekable TAR)". Computed
    per instance from catalog location and seekability (see §8.C).
    `get_members_if_available()` is tightened to return the list only for `INDEXED`
    (or already-known) and never to trigger a scan.
  - `member_access_cost: AccessCost` (`DIRECT` / `LIMITED` / `EXPENSIVE` / `UNAVAILABLE`)
    — *what it costs to open an arbitrary member out of order*. **Replaces** both
    `has_random_access()` and a plain `supports_random_access` boolean: "can I?" is
    `member_access_cost != UNAVAILABLE`, and the enum additionally says how expensive it
    is (cheap ZIP vs bounded rapidgzip `tar.gz` vs O(N) solid 7z).
  - `AccessCost` is shared: the same scale also reports member-stream **seek cost** via
    a new `seek_cost` property. The protocol-required `seekable(): bool` is kept as-is;
    `seek_cost` is an additional property alongside it (the two stay consistent), so
    callers can tell a true random-access stream from one that is seekable only by
    re-decompressing.
  - **`seek_cost` is owned by the seekable-stream abstraction, not re-derived by each
    reader.** Each decompressor/seekable stream (stdlib rewind wrapper, rapidgzip,
    indexed_bzip2, `XzDecompressorStream`, lzip, a plain file) exposes its own
    `seek_cost`. A TAR reader's `member_access_cost` is then read from the `seek_cost` of
    the decompressed stream it opens (reaching a member is a seek on that stream) instead
    of re-inferring it from config flags. **Because not every stream is an Archivey type
    yet** (some are returned raw by third-party libraries; the public `ArchiveyStream`
    base is the separate `public-stream-interface` change), this read goes through a
    tolerant `seek_cost_of(stream)` helper — returns `stream.seek_cost` when present, else
    a conservative estimate from type/`seekable()` (seekable-unknown → `EXPENSIVE`,
    non-seekable → `UNAVAILABLE`). The helper collapses into a direct read once all
    streams expose the property. (The PR-#221 TarReader
    re-derived the cost by inspecting `config.use_rapidgzip` etc. — duplicating
    backend-selection logic and already mis-reporting multi-block `tar.xz` as
    `EXPENSIVE`. Making the stream the single source of truth fixes that.)
  - `seek_cost` is added to archivey's own stream classes here. Guaranteeing it on
    **every** returned stream (including those handed back raw by third-party libraries)
    via a shared **public** `ArchiveyStream` base is split into the separate
    `public-stream-interface` change (exploration stage).
## Capabilities

### New Capabilities

- (none new; refines existing capabilities)

### Modified Capabilities

- `archive-reading`: adds the `MemberListingCost` and shared `AccessCost` enums and the
  `member_listing_cost` / `member_access_cost` introspection properties, adds an `AccessCost`
  `seek_cost` property alongside the protocol-required `seekable()` on member streams
  **and on the decompressor-stream abstraction** (with TAR deriving its access cost
  from it), tightens `get_members_if_available` to never scan, and removes
  `has_random_access()` (superseded) (§8.E).
- `archive-metadata`: adds the typed `CompressionMethod` enum and the lossless
  `compression_method_detail` field (§8.D).

## Non-Goals

- Any change to what archives can be read or how members decode (§8.B/C are
  internal refactors that keep observable behavior identical).
- *Measured*, per-call cost. `AccessCost` is a coarse mechanism-based hint
  (worst-case tier), not a predicted running time; wall-clock cost (e.g. whether a
  `SCAN_REQUIRED` seek beats decompression on a given disk/network) is left
  unmodeled.
- **Access intent (§8.F)** — the `access_intent` *input* (and the removal of the
  `streaming` parameter and the tri-state backend config) is split into the separate
  `access-intent` change; this change only adds the cost *receipt* it builds on.
- The §8.A co-iteration migration — folded into the native-reader changes, since
  those rewrite the same `sevenzip_reader.py` / `rar_reader.py` iteration code.

## Dependencies / Sequencing

**Land second** (after `test-suite-parametrization`, before the native readers).

§8.D (the `CompressionMethod` enum) must land **before** the 7z native reader so
that reader can emit typed compression methods directly. §8.B/§8.C/§8.E are
independent and can ship anytime. The `access-intent` change (§8.F) depends on §8.E
(it reports realized cost through `member_access_cost`/`seek_cost`), so it lands after
this one. (The §8.A migration lives in the native-reader changes.)

Recommended order across all pending changes:
1. `test-suite-parametrization` — verification harness
2. **this change** — §8.B–§8.E (§8.D enum prerequisite for 7z native; the foundation
   both `access-intent` and `public-stream-interface` build on)
3. `access-intent` — §8.F (depends on §8.E; sequence it early since it is the breaking
   API change — removing `streaming` — and the longer it waits the more call sites accrete)
4. `rar-native-metadata-reader` + `sevenzip-native-reader` (in parallel; also run junction Windows spike)
5. `unify-junction-handling` — after native readers (junction detection in native parsers)
6. `public-stream-interface` — implemented **last**, after its own exploration; it hoists
   the per-class `seek_cost` added here onto the shared `ArchiveyStream` base (a small,
   anticipated refactor that also lets the `seek_cost_of` helper collapse into a direct read)

## Impact

- **Files**: `internal/base_reader.py` (`member_listing_cost` / `member_access_cost`
  introspection properties computed per-instance, tightened `get_members_if_available`,
  removed `has_random_access`), `formats/*_reader.py` (`has_central_directory`
  ClassVar, `member_listing_cost` / `member_access_cost` reporting — TAR reads
  `member_access_cost` from its decompressed stream's `seek_cost`), the member-stream
  wrapper (`archive_stream.py`, adds `seek_cost` alongside the existing `seekable()`),
  the decompressor-stream classes (`formats/decompressor_stream.py`,
  `formats/compressed_streams.py`, `formats/xz_stream.py`, `formats/lzip_stream.py` —
  each exposes its own `seek_cost`),
  `types.py` (`CompressionMethod` — exhaustive over `StreamFormat` —,
  `compression_method_detail`, `MemberListingCost`, `AccessCost`),
  `archive_reader.py` (property declarations).
- **Live specs touched**: `archive-reading`, `archive-metadata`.
- **Design reference**: `docs/format-architecture-comparison.md` §8–§9.
