# Idea: a general-purpose binary-streams library (spin-out candidate)

**Status: parked idea, not scheduled.** Recorded 2026-06-10 so the thought isn't
lost. Revisit when the `public-stream-interface` exploration settles the
`ArchiveyStream` surface and `concurrent-member-access` produces the shared-source
multiplexer — at that point the generic layer is at its clearest and the
spin-out/keep decision can be made with real shapes in hand.

## The observation

While building archivey's stream layer, it became clear we were effectively
implementing a small general-purpose streams library — functionality Python's `io`
module implies but doesn't deliver. The `io` ABCs are loose contracts: `seekable()`
can lie (or be expensive to answer honestly), `read(n)` short-read semantics differ
between implementations, there is no standard way to wrap a partial file-like object
into one that honors the full contract, no positional-read (`pread`) abstraction, no
standard concatenation/slicing/observation wrappers, and no way to ask a stream what
seeking *costs*. Every project that processes byte streams seriously (fsspec,
urllib3, smart_open, ratarmount's utilities) rebuilds some subset of this.

## What archivey has already built (the inventory)

Generic — no archive semantics involved:

- `ensure_binaryio()` / `BinaryIOWrapper` — normalize any file-like into a
  full-contract binary stream (`internal/io_helpers.py`).
- Seekability probing (`is_seekable`) that doesn't trust `seekable()` blindly.
- `RewindableStreamWrapper` — buffer the head of a non-seekable stream so format
  detection can peek and rewind.
- `ConcatenationStream` — present several streams as one (will also serve
  multi-volume 7z, which is a plain byte-split).
- `StatsIO` — observation/instrumentation wrapper (bytes read, seeks, patterns).
- `RecordableStream` — record consumed bytes for replay.
- `ErrorIOStream` — inject/translate errors at the stream boundary.
- Lazy-opening streams with exception translation (`internal/archive_stream.py`) —
  the open is deferred until first read, errors mapped at the boundary.
- `DecompressorStream` + `_SegmentedDecompressorStream` — a framework for making
  forward-only decompressors seekable via rewind and registered seek points, with
  native XZ (block index) and lzip (trailer scan) implementations on top.

Generic, planned:

- `seek_cost: AccessCost` introspection (`base-reader-architecture-extensions`) —
  arguably the most broadly useful piece: a vocabulary for "seekable, but at what
  price" that nothing in the Python ecosystem offers.
- The shared-source multiplexer (`concurrent-member-access`) — N independent
  positions over one underlying stream, lock + seek-per-read; stdlib `zipfile` has a
  private version (`_SharedFile`) that everyone else reimplements or lacks.

Archive-specific (would stay in archivey regardless): member metadata on streams,
the `ArchiveMember` back-reference, format detection, the reader machinery.

## What a spin-out could be

A small dependency-free library ("full-contract binary streams"): wrappers +
combinators (normalize, rewindable head, concatenate, slice, observe, record,
multiplex), honest capability probing, `seek_cost`-style cost introspection, a
contract test-kit (assert an object really honors the IO protocol — itself valuable,
we'd use it for our own wrappers), and possibly the seekable-decompressor framework.

## Pros / cons of spinning out

**Pros:** reusable beyond archives (anyone wrapping HTTP/process/decompressor
streams); a crisp API boundary forces the generic/archive split we want anyway; the
contract test-kit and multiplexer could attract users and outside hardening;
archivey's own surface shrinks.

**Cons:** API-stability burden of a second public package while archivey itself is
pre-1.0 and the stream surface is still moving; release/version coordination; risk
of premature abstraction — the multiplexer and `ArchiveyStream` don't exist yet, so
extracting now would freeze guesses.

## Decision criteria (when revisiting)

1. `public-stream-interface` has settled the base type and the wrap-vs-annotate
   question, **designed for separability**: generic wrappers must not import archive
   types (this costs nothing now and keeps the option open — the only action this
   idea requires today).
2. The multiplexer exists and has survived the concurrency stress tests.
3. There is at least one concrete external consumer or strong signal of demand;
   otherwise keep it as a well-factored internal package (`archivey.streams`?) —
   spin-out is then a rename, not a rewrite.
