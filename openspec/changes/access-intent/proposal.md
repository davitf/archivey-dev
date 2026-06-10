# Access intent (§8.F)

## Why

`open_archive` today makes the caller speak in two awkward vocabularies at once:

- a `streaming` boolean (plus its deprecated `streaming_only` alias) that **bundles two
  unrelated things** — "I will only iterate forward" (an access *intent*) and "accept a
  non-seekable source" (a fact about the input); and
- low-level backend booleans (`use_rapidgzip`, `use_indexed_bzip2`, `use_python_xz`, …)
  that leak archivey's cost model: to get cheap random access on a `tar.gz` you must
  already know to set `use_rapidgzip=True`.

The `base-reader-architecture-extensions` change adds the **cost receipt** —
`member_access_cost` / `seek_cost` tell a caller *what they got*. This change adds the
matching **request**: a single `access_intent` declaring *how the caller will use the
archive*, and a **tri-state** backend config so an `AUTO` backend can be turned on (or
not) to honor that intent. Intent is resolved into the existing backend-selection path,
not a parallel one, and is also handed to the reader so a reader can adapt its own
strategy (e.g. build vs. skip a seek-point index) — not just pick a backend.

This replaces `streaming`/`streaming_only` outright: forward-only use becomes
`access_intent=SEQUENTIAL`, and the random-access default becomes `access_intent=AUTO`.

**Current state (verified):** `open_archive(streaming=False)` requires a seekable source
(raises `ArchiveStreamNotSeekableError` otherwise) and passes `streaming_only=streaming`
into the reader (`core.py:212`); `streaming_only` is already a deprecated alias
(`archive-opening` spec). The `use_*` flags are plain `bool`, default `False` — there is
no auto-detection and no `access_intent` input.

## What Changes

- **`AccessIntent` enum — one axis** *(public, new)*: a `StrEnum`
  `AUTO` (default) / `SEQUENTIAL` / `RANDOM`. `RANDOM` covers **both** reaching members
  out of order **and** seeking within a member's content — the two are treated as one
  intent for now. (Splitting out a within-member "content seek" facet — relevant once a
  native ZIP reader can opt into rapidgzip *per member* — is a future, purely additive
  extension; see Non-Goals.)
- **`access_intent` replaces `streaming` and `streaming_only`** *(public, breaking)*: both
  parameters are **removed** from `open_archive`. The mapping is exact:
  - old `streaming=True`  → `access_intent=SEQUENTIAL` (forward-only; non-seekable
    sources accepted; random-access methods restricted as in streaming mode);
  - old `streaming=False` (default) → `access_intent=AUTO` (random access on a seekable
    source; a non-seekable source still raises `ArchiveStreamNotSeekableError`).
- **`AUTO` vs `RANDOM`** — both expose random-access methods on a seekable source and
  both raise on a non-seekable one. `AUTO` (the default) uses the **best installed
  backend**: on a seekable source it enables an installed rapidgzip / indexed_bzip2 —
  these are both *faster* than the stdlib backends (parallel decompression) *and*
  indexed (cheap random access), so zero-config gets the good backend, per the
  "most correct available backend" design principle. (XZ needs no activation: the
  default `XzDecompressorStream` already block-seeks.) `RANDOM` additionally declares
  that out-of-order access *will* happen, so the reader keeps/builds seek points
  eagerly rather than lazily. `SEQUENTIAL` favors the cheapest streaming path: it
  activates `use_rar_stream` for solid RAR iteration (single `unrar p` pass, O(N)
  instead of O(N²); `open()` is restricted in this mode anyway) and skips eager index
  building. Whether `SEQUENTIAL` should also use rapidgzip/indexed_bzip2 for raw
  decompression speed is an open question in design.md (pending benchmarks).
- **Intent is carried into the reader**, generalizing today's `streaming_only=streaming`
  hand-off: the reader receives `access_intent` and may adapt its strategy (eager vs
  lazy, build vs skip a seek index), in addition to backend selection.
- **Tri-state backend config** *(public)*: each optional-backend flag (`use_rapidgzip`,
  `use_indexed_bzip2`, `use_zstandard`, `use_python_xz`, `use_rar_stream`) accepts
  `True` / `False` / `"auto"`, **default `"auto"`**:
  - `True` = always use, raise `PackageNotInstalledError` if its package is missing
    (unchanged from today's explicit `True`);
  - `False` = never use;
  - `"auto"` = use iff the package is installed **and** the resolved intent makes it
    worthwhile (per-flag mapping in the configuration delta: rapidgzip/indexed_bzip2
    under `AUTO`/`RANDOM` on a seekable source; `use_rar_stream` under `SEQUENTIAL`;
    `use_python_xz`/`use_zstandard` never implicitly).
  - **The new default is deliberately not behavior-preserving**: `"auto"` + default
    intent `AUTO` enables installed rapidgzip/indexed_bzip2, where today's all-`False`
    default never did. Decided 2026-06-10 — zero-config should get the faster, indexed
    backend when it's installed; users who want stdlib-only behavior set the flag to
    `False`. With none of the optional packages installed, behavior is unchanged.
  - **Prerequisite for the default flip**: rapidgzip/indexed_bzip2 must be verified
    safe for multiple concurrently-open member streams (see the
    `concurrent-member-access` exploration) before they are enabled by default.
- **`RANDOM` is best-effort and reported through cost**: if a preferred package is absent
  or the format cannot random-access cheaply (solid 7z, single-block xz), archivey falls
  back and the cost properties report the realized (`EXPENSIVE`) outcome rather than
  raising. It raises only when random access is *impossible* (a non-seekable source).

## Capabilities

### Modified Capabilities

- `archive-opening`: adds the `access_intent` parameter and the `AccessIntent` enum,
  removes `streaming` and `streaming_only`, and rebinds the non-seekable-source rule to
  intent (`SEQUENTIAL` accepts a non-seekable source; `AUTO`/`RANDOM` raise).
- `configuration`: the optional-backend flags become tri-state (`True`/`False`/`"auto"`,
  default `"auto"`); adds the rule that `access_intent` resolves into that same backend
  selection (`RANDOM` activates `"auto"` seekable/indexed backends; explicit `True`/`False`
  override intent).
- `archive-reading`: the forward-only "streaming mode" is now entered via
  `access_intent=SEQUENTIAL` (or a non-seekable source) rather than `streaming=True`; the
  mode's behavior is otherwise unchanged.

## Non-Goals

- **A separate within-member "content seek" facet.** `RANDOM` means both out-of-order
  member access and within-member seeking for now. A second facet (e.g. choosing
  rapidgzip *per ZIP member* only when the caller will seek inside files) becomes
  load-bearing only when a reader differentiates on it (the planned native ZIP reader);
  adding it later is additive (a new parameter / enum), not a breaking change.
- **Access-pattern warnings.** Warning when realized usage contradicts the declared
  intent (e.g. repeated backward seeks on an `EXPENSIVE` stream) needs runtime tracking
  and is left for a possible future change.
- **Measured cost or per-call prediction** — owned by the cost surface in
  `base-reader-architecture-extensions`; intent is resolved by mechanism, not measurement.

## Dependencies / Sequencing

**Lands after `base-reader-architecture-extensions`** — `RANDOM`'s best-effort contract
reports the realized outcome through `member_access_cost` / `seek_cost`, which that
change introduces. No other change depends on this one; the native readers are
unaffected (they gain intent handling for free via the shared reader hand-off).

Recommended order: `test-suite-parametrization` → `base-reader-architecture-extensions`
→ **this change** → native readers (`rar-native-metadata-reader`, `sevenzip-native-reader`)
→ `unify-junction-handling` → `public-stream-interface`. Sequence this change **early**
(right after the cost foundation): it is the one **breaking** API change here (removing
`streaming` / `streaming_only`), so the sooner it lands the fewer `streaming=` call sites
accumulate to migrate.

## Impact

- **Files**: `core.py` (`open_archive`: remove `streaming`/`streaming_only`, add
  `access_intent`, resolve it into an effective config, pass it to the reader; the
  non-seekable check at `core.py:145` keys on intent), `config.py` (backend fields become
  `bool | Literal["auto"]` default `"auto"`; `ConfigOverrides`; literal conversion),
  `types.py` (`AccessIntent` enum), the backend-selection sites that read `config.use_*`
  (`formats/compressed_streams.py`, `formats/xz_stream.py`, the gzip/bzip2/zstd backend
  picks) so `"auto"` consults the resolved intent, `internal/base_reader.py` + the format
  readers (accept and act on `access_intent`).
- **Live specs touched**: `archive-opening`, `configuration`, `archive-reading`.
- **Design reference**: `docs/format-architecture-comparison.md` §8.E–§8.F.
