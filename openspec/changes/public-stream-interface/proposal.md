# Public stream interface — `ArchiveyStream` (exploration)

## Status

**Exploration — the surface is not finalized.** This change records a direction and its
open questions; the spec delta is deliberately minimal. It is split out of
`base-reader-architecture-extensions` (which adds `seek_cost` to archivey's *own* stream
classes) so that the broader question — *how archivey constructs, wraps, and hands back
streams in general* — can be analyzed on its own before we commit to a public API.

## Why

`base-reader-architecture-extensions` adds `seek_cost` (and the `AccessCost` scale) to
archivey's member-stream and decompressor-stream classes. But the introspection is only
as good as its reach:

- archivey has several stream classes that each independently subclass
  `io.RawIOBase, BinaryIO` (`ArchiveStream`, `DecompressorStream`, `BinaryIOWrapper`,
  `StatsIO`, `ConcatenationStream`, `RecordableStream`, `ErrorIOStream`), with **no shared
  archivey base**; and
- a stream handed back from a third-party library (py7zr / rarfile / zipfile) may not be
  an archivey type at all, so nothing guarantees `seek_cost` / metadata is present on
  **every** stream archivey returns.

If `seek_cost` and friends are worth exposing, a caller should be able to read them on any
returned stream without special-casing the concrete type — which means a **public** base
type that every returned stream is an instance of. That public type is also the natural
seed for a wider review of archivey's stream handling.

## What we already know (decided)

- **`ArchiveyStream` is public API.** The whole point of attaching cost + metadata is for
  callers to use them; an internal-only base would defeat that. It is *also* a normal
  binary stream (`io.RawIOBase` / `BinaryIO`), usable anywhere a file object is.
- It carries at least `seek_cost: AccessCost` (consistent with `seekable()`) and a
  `name: str | None` (mirroring stdlib file objects' `.name`).
- Streams from third-party libraries are normalized into it (the existing
  `ensure_binaryio()` / `BinaryIOWrapper` path is the natural seam); the working
  recommendation is to **wrap** rather than `setattr` onto the raw object.

## Open questions (to explore)

- **Full metadata set** — beyond `seek_cost` + `name`: a back-reference to the
  `ArchiveMember`? the member's `CompressionMethod`? the source `StreamFormat`? a size?
- **Base class vs mixin vs `Protocol`** — should every internal stream class inherit one
  concrete base, or should the public surface be a `Protocol` plus a mixin, given the
  classes already carry divergent `io.RawIOBase` plumbing?
- **Wrapping cost** — is the extra wrapper layer ever a measurable hotspot (archives with
  very many small members)? If so, when is annotate-in-place justified?
- **Scope of "streams in general"** — recording, concatenation, stats, rewindable
  wrappers, and error streams may want a more unified model. This is the analysis to do
  before locking the API.
- **Write paths** — does the public base cover only readable streams, or also writable
  ones?
- **Separability** *(constraint, decided 2026-06-10)*: the generic stream plumbing
  (wrappers, combinators, multiplexer, `seek_cost`) must stay free of archive-specific
  imports, keeping open the option of extracting it as a standalone streams library —
  see `docs/streams-library-idea.md`. Archive semantics (member back-reference, etc.)
  layer on top.

## Capabilities

### New Capabilities

- `archivey-stream`: a public `ArchiveyStream` stream type that every returned stream is
  an instance of, exposing `seek_cost` and `name` (with the surface to be expanded after
  the exploration above).

## Dependencies / Sequencing

Depends on `base-reader-architecture-extensions` (the `AccessCost` / `seek_cost` surface).
Should be scheduled **after** a dedicated exploration of archivey's stream usage; the spec
here is intentionally minimal until that analysis is done.

Recommended order: implemented **last** of the pending changes
(`test-suite-parametrization` → `base-reader-architecture-extensions` → `access-intent`
→ native readers → `unify-junction-handling` → **this change**). It is sequenced last on
purpose — blocking the cost foundation on this exploration would stall everything, and
the cost of waiting is small and known: this change hoists the per-class `seek_cost` that
`base-reader-architecture-extensions` adds onto the shared `ArchiveyStream` base, at which
point that change's tolerant `seek_cost_of` helper collapses into a direct property read.

## Impact

- **Files (anticipated)**: `internal/io_helpers.py` (the `ArchiveyStream` base + the
  normalizing wrapper), the existing stream classes (inherit it), `core.py` (return-type
  guarantee), public exports in `__init__.py`.
- **Live specs touched**: new `archivey-stream` capability.
- **Design reference**: `docs/format-architecture-comparison.md` §8.E.
