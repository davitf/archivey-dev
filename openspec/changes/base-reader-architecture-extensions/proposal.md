## Why

`docs/format-architecture-comparison.md` §8 identifies five places where the
`BaseArchiveReader` contract creaks once all formats (incl. the native RAR/7z
readers) are in place. None changes externally-observable archive behavior much,
but together they clean up the reader contract and make a couple of useful
capabilities first-class for callers.

This change covers the four contract/spec items **§8.B–§8.E**, plus a new
**§8.F — access intent**. The fifth original item, **§8.A** (migrating the 7z/RAR
solid readers onto the existing `_iter_members_and_streams_internal()` hook), is a
pure internal refactor with no spec delta, so it is **folded into the
native-reader changes** (`rar-native-metadata-reader` / `sevenzip-native-reader`),
which already rewrite those files — see their tasks.

§8.E exposes **cost introspection** (how expensive is it to list members / reach a
member / seek within one). On its own that pushes a burden onto callers: to *get*
cheap random access on a `tar.gz` today you must already know to set
`use_rapidgzip=True` — the low-level backend flags (`use_rapidgzip`,
`use_indexed_bzip2`, `use_python_xz`, …) leak archivey's cost model. §8.F adds the
**dual**: an `access_intent` *input* at open time where the caller declares how
they will use the archive (forward iteration vs. out-of-order / repeated access),
and archivey maps that intent onto the right backends. Intent is the request; the
§8.E cost properties are the **receipt** — what archivey actually achieved, since
intent cannot always be honored (a solid 7z, a single-block xz, or a missing
optional package). The two are complementary, not redundant.

**Current state (verified):** §8.B–F are not yet implemented —
`compression_method` is still a plain `str`, `members_list_supported` is still an
`__init__` argument, there is no `_format_supports_random_access` flag or
`*_cost` properties (only the existing `has_random_access()` method), and there is
no access-intent input (callers must hand-pick the `use_*` backend flags).

## What Changes

- **§8.B — format capability vs user preference** *(internal)*: add a
  `_format_supports_random_access` flag so "format cannot random-access" (a
  non-seekable compressed TAR) is distinct from "user asked for streaming". This is
  **per-instance, not a ClassVar** — whether a compressed TAR can random-access
  depends on the runtime stream/backend (stdlib gzip on a pipe is non-seekable;
  stdlib on a file rewinds; rapidgzip/indexed_bzip2 are always seekable), so it is
  set in `__init__`. The runtime streaming flag becomes "user requested OR format
  can't".
- **§8.C — `has_central_directory` ClassVar (replaces the `members_list_supported`
  argument)** *(internal)*: "does this *format* have a catalog/central directory?"
  is a genuine format-level fact (ZIP/7z/RAR/ISO/folder yes, TAR no), so it becomes
  a `ClassVar[bool]` named `has_central_directory` — a clearer name than
  `members_list_supported`. But the *realized* listing cost is **not** read from
  that flag alone: a catalog at end-of-file is only reachable when the source is
  seekable, so `member_listing_cost` (§8.E) is computed **per instance** from
  `has_central_directory` **and** seekability. (The PR-#221 implementation derived
  `INDEXED` from the ClassVar only — wrong for a catalog format on a non-seekable
  source.) The standalone `members_list_supported` boolean is dropped; it is now
  exactly `member_listing_cost == INDEXED`.
- **§8.D — typed `CompressionMethod` enum + lossless detail** *(public)*: a `StrEnum`
  of known methods (`STORED`, `DEFLATE`, `LZMA`, `LZMA2`, `ZSTD`, `BZIP2`, `PPMD`,
  `BCJ2`, …, plus `UNKNOWN`) so callers can branch on compression without parsing
  free-form strings. Stays string-compatible. `compression_method` holds the typed
  **primary** codec (`UNKNOWN` if reported-but-unmapped, `None` if unreported); a new
  free-form `compression_method_detail: Optional[str]` preserves the full,
  lossless description — 7z filter chains (`"LZMA2 + BCJ2"`) and third-party readers'
  own codec names that a closed enum can't represent.
- **§8.E — cost introspection (redesigned surface)** *(public)*: replace the
  confusing tangle of `streaming` / `has_random_access()` / "member list supported"
  with introspection on two orthogonal axes, each a cost-classifying enum so the
  surface never lies about expense the way a boolean does:
  - `member_listing_cost: MemberListingCost` (`INDEXED` / `SCAN_REQUIRED` / `SEQUENTIAL_ONLY`)
    — *how cheaply the full member list is obtainable*, so "one bounded seek (ZIP
    catalog)" is no longer conflated with "O(N) full pass (seekable TAR)". Computed
    per instance from `has_central_directory` and seekability (see §8.C).
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
    `seek_cost`. A TAR reader's `member_access_cost` is then read directly from the
    `seek_cost` of the decompressed stream it opens (reaching a member is a seek on
    that stream) instead of re-inferring it from config flags. (The PR-#221 TarReader
    re-derived the cost by inspecting `config.use_rapidgzip` etc. — duplicating
    backend-selection logic and already mis-reporting multi-block `tar.xz` as
    `EXPENSIVE`. Making the stream the single source of truth fixes that.)
- **§8.F — access intent** *(public, new)*: an `AccessIntent` `StrEnum`
  (`AUTO` (default) / `SEQUENTIAL` / `RANDOM`) and an `access_intent` parameter on
  `open_archive(...)`. It is a high-level declaration of how the caller will use the
  archive that archivey **resolves into the existing low-level backend flags**:
  - `AUTO` — preserve current behavior; honor the explicit `use_*` config flags and
    pick no optional backend on the caller's behalf.
  - `SEQUENTIAL` — caller iterates forward; prefer the cheapest streaming backend and
    skip building seek indexes.
  - `RANDOM` — caller will reach members out of order and/or seek within members,
    possibly repeatedly; prefer seekable/indexed backends (rapidgzip, indexed_bzip2,
    multi-block xz) **when installed**.

  Intent is **best-effort**: an explicit `use_*` flag remains a hard requirement
  (raises if its package is missing), but `RANDOM` falls back to the stdlib backend
  when an optional package is absent and lets the §8.E cost properties report the
  realized (`EXPENSIVE`) outcome rather than raising. `streaming=True` together with
  `RANDOM` is contradictory and raises `ValueError`.
- **§8.G — inefficient-access warnings** *(public, opt-in)*: an **off-by-default**
  config flag (`warn_on_inefficient_access`) and a dedicated `InefficientAccessWarning`
  category. When enabled, archivey emits a Python warning when usage is inefficient
  relative to the declared intent or the cost tier:
  - at **open**, when `access_intent=RANDOM` was requested but the realized
    `member_access_cost` is `EXPENSIVE`/`UNAVAILABLE` (preferred backend unavailable, or
    the format cannot random-access cheaply) — the cheap, primary signal;
  - at **runtime** (the lighter, secondary detector), when repeated out-of-order member
    access or repeated re-decompressing backward seeks occur on an `EXPENSIVE` target
    (the O(N²) trap).

  Warnings never change which bytes are returned and are silent when the flag is off
  (the default). The runtime detector requires per-archive access-pattern tracking and
  MAY be split into a follow-up change if it grows; the open-time intent warning is the
  core of §8.G.

## Capabilities

### New Capabilities

- (none new; refines existing capabilities)

### Modified Capabilities

- `archive-reading`: adds the `MemberListingCost` and shared `AccessCost` enums and the
  `member_listing_cost` / `member_access_cost` introspection properties, adds an `AccessCost`
  `seek_cost` property alongside the protocol-required `seekable()` on member streams
  **and on the decompressor-stream abstraction** (with TAR deriving its access cost
  from it), tightens `get_members_if_available` to never scan, removes
  `has_random_access()` (superseded) (§8.E), adds the `AccessIntent` enum and the
  `access_intent` open parameter that selects backends to honor the requested access
  pattern (§8.F), and adds the opt-in `warn_on_inefficient_access` flag with the
  `InefficientAccessWarning` category (§8.G).
- `archive-metadata`: adds the typed `CompressionMethod` enum and the lossless
  `compression_method_detail` field (§8.D).

## Non-Goals

- Any change to what archives can be read or how members decode (§8.B/C are
  internal refactors that keep observable behavior identical; §8.F only changes
  *which backend* is selected, not decoded output).
- *Measured*, per-call cost. `AccessCost` is a coarse mechanism-based hint
  (worst-case tier), not a predicted running time; wall-clock cost (e.g. whether a
  `SCAN_REQUIRED` seek beats decompression on a given disk/network) is left
  unmodeled. §8.G warnings are triggered by the coarse tier, not by measurement.
- The §8.A co-iteration migration — folded into the native-reader changes, since
  those rewrite the same `sevenzip_reader.py` / `rar_reader.py` iteration code.

## Dependencies / Sequencing

**Land second** (after `test-suite-parametrization`, before the native readers).

§8.D (the `CompressionMethod` enum) must land **before** the 7z native reader so
that reader can emit typed compression methods directly. §8.B/§8.C/§8.E/§8.F/§8.G are
independent and can ship anytime (§8.G depends on §8.E/§8.F within this change). (The
§8.A migration lives in the native-reader changes.)

Recommended order across all pending changes:
1. `test-suite-parametrization` — verification harness
2. **this change** — §8.B–§8.G (§8.D enum prerequisite for 7z native)
3. `rar-native-metadata-reader` + `sevenzip-native-reader` (in parallel; also run junction Windows spike)
4. `unify-junction-handling` — after native readers (junction detection in native parsers)

## Impact

- **Files**: `internal/base_reader.py` (`member_listing_cost` / `member_access_cost`
  introspection properties computed per-instance, tightened `get_members_if_available`,
  removed `has_random_access`), `formats/*_reader.py` (`has_central_directory`
  ClassVar, `member_listing_cost` / `member_access_cost` reporting — TAR reads
  `member_access_cost` from its decompressed stream's `seek_cost`), the member-stream
  wrapper (`archive_stream.py`, adds `seek_cost` alongside the existing `seekable()`),
  the decompressor-stream classes (`formats/decompressor_stream.py`,
  `formats/compressed_streams.py`, `formats/xz_stream.py`, `formats/lzip_stream.py` —
  each exposes its own `seek_cost`), `core.py` + `config.py` (`access_intent`
  parameter on `open_archive`, resolved into the effective backend selection;
  `warn_on_inefficient_access` config flag), `types.py` (`CompressionMethod`,
  `compression_method_detail`, `MemberListingCost`, `AccessCost`, `AccessIntent`),
  `exceptions.py` (or `types.py`) (`InefficientAccessWarning` category),
  `archive_reader.py` (property declarations).
- **Live specs touched**: `archive-reading`, `archive-metadata`.
- **Design reference**: `docs/format-architecture-comparison.md` §8–§9.
