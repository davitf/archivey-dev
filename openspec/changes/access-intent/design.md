# Design

Full rationale and per-format fit are in **`docs/format-architecture-comparison.md`**
§8.E–§8.F. This file records the spec-facing decisions for the access-intent change.

## Scope

This is **§8.F**, split out of `base-reader-architecture-extensions` because it is a
larger, externally-facing redesign (a new open-time input, removal of two parameters,
and a tri-state config) that builds on the cost surface (`member_access_cost` /
`seek_cost`) added there. It lands after that change.

## Starting point (verified against current code)

- `open_archive(streaming=False)` (the default) requires a seekable source and raises
  `ArchiveStreamNotSeekableError` otherwise (`core.py:145`); `streaming=True` accepts a
  non-seekable source and opens in forward-only "streaming mode".
- `streaming` is already passed into the reader as `streaming_only=streaming`
  (`core.py:212`), so a reader already receives the access mode — this change generalizes
  that hand-off to the full intent.
- `streaming_only` is already a deprecated alias (`archive-opening` spec).
- The `use_*` flags are plain `bool`, default `False`; there is no auto-detection.

## What is observable vs internal

| Item | Observable? | Spec impact |
|---|---|---|
| `AccessIntent` enum + `access_intent` parameter (one axis) | **Yes** (new public input) | `archive-opening` |
| Removal of `streaming` / `streaming_only` | **Yes** (breaking) | `archive-opening`, `archive-reading` |
| Non-seekable-source rule rebinds to intent | **Yes** | `archive-opening` |
| Tri-state backend flags (`True`/`False`/`"auto"`, default `"auto"`) | **Yes** | `configuration` |
| `access_intent` → backend resolution | **Yes** | `configuration` |
| Intent passed to the reader for strategy | No (internal hand-off; behavior captured by cost) | none directly |

## Decisions

- **One axis now; `RANDOM` means both** (member-level out-of-order access *and*
  within-member seeking). The cost surface already separates the two
  (`member_access_cost` vs `seek_cost`), so the model *can* grow a second intent facet,
  but no current reader differentiates on within-member seeking — a ZIP gives cheap
  across-member access from its central directory regardless of whether you seek inside
  an entry. The case that would need the split (use rapidgzip *per member* only when the
  caller will seek within files) arrives with the planned native ZIP reader; adding a
  `content`-level facet then is a purely additive parameter, not a break. Building it now
  would be speculative surface with no consumer.

- **`AUTO` uses the best installed backend; `RANDOM` declares the access pattern.**
  *(Revised 2026-06-10 — an earlier draft had `AUTO` never enable an optional backend,
  which preserved today's default but contradicted design principle 4: a user with
  rapidgzip installed would still hit the stdlib rewind trap by default.)* Both allow
  random-access methods on a seekable source and both raise on a non-seekable one (so
  the default's footgun-guard is preserved). The semantics:
  - `AUTO` (default): on a **seekable** source, enables an installed rapidgzip /
    indexed_bzip2 — they are both faster than the stdlib backends (parallel
    decompression) and indexed, so this is a strict improvement, not a trade. On a
    non-seekable source the optional backends are not engaged. (XZ needs no activation:
    the default `XzDecompressorStream` already block-seeks.)
  - `RANDOM`: same backend selection as `AUTO`, plus a declared pattern: the reader
    keeps/builds seek points eagerly (rather than lazily) because out-of-order access
    *will* happen.
  - `SEQUENTIAL`: forward-only; accepts a non-seekable source (like today's
    `streaming=True`); skips eager index building; activates `use_rar_stream` for
    solid RAR (see resolved question below).

- **Remove `streaming` and `streaming_only` outright** (not a deprecation alias —
  `streaming_only` was already the deprecation step). The forward-only mode is unchanged;
  only its entry point moves. Mapping is exact: `streaming=True` → `SEQUENTIAL`,
  `streaming=False` → `AUTO`. The internal "streaming mode" concept and all its
  `archive-reading` requirements (single-pass iteration, restricted `open()`/`extract()`)
  stay; they are now entered by `SEQUENTIAL` or a non-seekable source. The existing
  `archive-reading` scenarios that invoke `streaming=True`/`streaming=False` are reworded
  mechanically to `access_intent="sequential"` / the default (see tasks).

- **Tri-state backend config as `bool | Literal["auto"]`, default `"auto"`.** This matches
  the requested "AUTO / true / false" ergonomically (`True`/`False` keep their meaning;
  `"auto"` is the new default) without forcing callers onto an enum. `True` = force, raise
  `PackageNotInstalledError` if missing (today's explicit-`True` behavior, unchanged);
  `False` = never; `"auto"` = use iff installed and intent warrants it (per-flag mapping
  in the configuration delta). **The new default is deliberately not
  behavior-preserving** *(decided 2026-06-10)*: `"auto"` + default intent `AUTO` enables
  installed rapidgzip/indexed_bzip2 on seekable sources, where today's all-`False`
  default never did. Zero-config should get the faster, indexed backend when installed;
  stdlib-only behavior remains one `False` away, and with no optional packages installed
  nothing changes. Prerequisite: the multi-stream thread-safety of these backends must
  be verified first (see the `concurrent-member-access` exploration). (A `LibraryUsage`
  `StrEnum` was considered; the `bool | "auto"` literal is less ceremony and round-trips
  through the existing string-literal config conversion.)

- **Intent resolves into the existing backend selection, and is also handed to the
  reader.** `open_archive` computes an effective config from `access_intent` + the
  tri-state flags and passes it down the unchanged stream-opening path (one selection
  mechanism, not two). Separately, `access_intent` is passed to the reader constructor
  (replacing the `streaming_only=streaming` hand-off) so a reader may adapt strategy —
  e.g. build a seek-point index under `RANDOM`, skip it under `SEQUENTIAL`.

- **Best-effort, with one hard failure.** Explicit `True` on a flag whose package is
  missing still raises `PackageNotInstalledError`. `RANDOM` is otherwise a preference: a
  missing optional package or a format that cannot random-access cheaply (solid 7z,
  single-block xz) falls back, and `member_access_cost` / `seek_cost` report the realized
  (`EXPENSIVE`) cost. The single intent that *raises* is random access on a **non-seekable
  source** (`AUTO` or `RANDOM`), because it is genuinely impossible — preserving today's
  `streaming=False` + non-seekable behavior.

## Resolved: SEQUENTIAL activates `use_rar_stream` *(decided 2026-06-10)*

With `use_rar_stream` off, iterating a solid RAR via rarfile shells out to `unrar`
once per member (O(N²) total); with it on, a single `unrar p` pass serves all members
in order (O(N)). Its known limitation — `open()` on an individual member still costs
O(N) — is moot under `SEQUENTIAL`, where random-access methods are restricted anyway.
Both paths require the same `unrar` binary, so `"auto"` here is not about an extra
package. **Decision: `"auto"` + `SEQUENTIAL` activates `use_rar_stream`.** This changes
behavior relative to today's `streaming=True` (which used the O(N²) path unless the
flag was set) — an intended improvement, with a scenario in the configuration delta.

Expected lifetime note: `use_rar_stream` will likely **disappear when the native RAR
reader lands** — the single-pass `unrar p` strategy becomes the standard behavior for
sequential extraction rather than an opt-in backend. The intent mapping here is the
transitional behavior.

## Open question: should SEQUENTIAL (and AUTO on non-indexed paths) use
rapidgzip/indexed_bzip2 for raw speed?

`AUTO`/`RANDOM` enable them for cost-class reasons; under `SEQUENTIAL` there is no
random access to make cheap, but rapidgzip's parallel decompression may still be a
large wall-clock win for a pure forward pass. Needs a benchmark (the `benchmarks/`
suite is the place) and a look at memory overhead before deciding; until then
`SEQUENTIAL` keeps the stdlib streaming backends.

A related point, now reflected in the configuration delta: `use_python_xz` and
`use_zstandard` provide no cost-class improvement over the defaults (the native
`XzDecompressorStream` already block-seeks; the zstandard wrapper rewinds like the
default), so their `"auto"` never activates implicitly under any intent.
