# Concurrent member access — independent, thread-safe streams (exploration)

## Status

**Exploration — the contract is not finalized.** This change records the problem, what
must be investigated, and a minimal spec direction. It was split out on 2026-06-10
because two other efforts depend on its findings: the `access-intent` default flip
(enabling rapidgzip/indexed_bzip2 under `AUTO` requires knowing they tolerate multiple
concurrently-open streams) and the `public-stream-interface` exploration (the
shared-source multiplexer is a stream-construction concern).

## Why

Nothing today specifies what happens when a caller opens **several member streams at
once** — two different members, or the same member twice — or touches them from
**multiple threads**. For a "best it can be" reading library this contract matters:

- Streams must be **independent**: each open stream has its own position; reading one
  must not corrupt or reposition another. Opening the same member twice must yield two
  independent positions.
- Streams must be **thread-safe** at least to the extent that different threads may
  each own a different stream (the stdlib norm). Whether one stream object may be
  shared between threads is a separate, weaker question.
- **The shared-source-stream problem**: when an archive is opened from a
  caller-provided *stream* (not a path), there is exactly one underlying file position.
  Two member streams decompressing concurrently will interleave reads and seeks and
  silently corrupt each other. Supporting this needs a **multiplexer** over the source:
  a lock-based wrapper that, per read, seeks the underlying stream to that consumer's
  position before reading (pread-style). stdlib `zipfile` solves exactly this with
  `_SharedFile` (a per-reader position + a shared lock) — good prior art. For archives
  opened by *path*, an alternative is opening independent file handles per member
  stream.
- **Backend risk**: rapidgzip and indexed_bzip2 manage native threads and indexes; it
  is unverified whether multiple concurrently-open member streams over one decompressor
  object (or multiple decompressor objects over one source) are safe — crashes are
  plausible. This must be answered empirically before `access-intent` enables these
  backends by default.
- **Format limits**: a solid 7z/RAR stream served during co-iteration is inherently
  sequential; concurrent `open()` of two members in one solid folder may require
  independent decompression passes (correct but expensive) — the contract must say
  which it is, per cost tier, rather than leaving it undefined.
- **Free-threaded Python**: the project ships an `optional-freethreaded` extra, so the
  answer should be stated for no-GIL builds too, not assumed from the GIL.

## What we already know (direction)

- The target contract, per the 2026-06-10 decision: *all open member streams work
  independently and are thread-safe* (different threads may each use a different
  stream; same member may be open multiple times). Reader-level methods (`open`,
  `get_members`) should also be callable concurrently or documented otherwise.
- Path-opened archives can get independence via per-stream file handles; stream-opened
  archives need the lock-based multiplexer (there is no other way to share one
  position).
- The multiplexer belongs at the stream-construction seam (`ensure_binaryio()` /
  the `public-stream-interface` wrapper), not inside each format reader.

## Open questions (to explore)

- Current behavior inventory: which readers/backends already tolerate concurrent
  streams, which break, and how (wrong bytes vs exception vs crash)?
- rapidgzip / indexed_bzip2: multi-stream and multi-thread behavior, including one
  decompressor shared across streams vs one decompressor per stream (memory cost of
  the latter — duplicate indexes?).
- Contract strength: per-stream thread confinement (stdlib norm) vs fully shareable
  streams; what do we promise on free-threaded builds?
- Solid archives: is concurrent `open()` allowed at `EXPENSIVE` cost (independent
  passes), or restricted? How does this interact with streaming mode?
- Multiplexer design: fairness, lock granularity, interaction with `closefd`, and
  whether buffered read-ahead per consumer is needed to avoid seek thrashing.
- Subprocess-backed streams (`unrar p` pipes): these cannot be multiplexed — what is
  the contract there?

## Capabilities

### Modified Capabilities

- `archive-reading`: adds the independent-streams requirement (minimal version now;
  strengthened after the exploration).

## Dependencies / Sequencing

Exploration can start immediately and should produce findings **before**
`access-intent` is implemented (its task 3.4 consumes the rapidgzip/indexed_bzip2
verdict). Implementation of the multiplexer should be coordinated with
`public-stream-interface` (same seam). The native readers should state their
concurrency behavior as they land.

## Impact

- **Files (anticipated)**: `internal/io_helpers.py` (multiplexer), `internal/
  base_reader.py` (open-stream tracking), backend selection sites, new stress tests
  in `tests/archivey/`.
- **Live specs touched**: `archive-reading`.
- **Design reference**: `docs/format-architecture-comparison.md` §11.
