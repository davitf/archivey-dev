# Test strategy — snapshots, ground truth, conformance, robustness (exploration)

## Status

**Exploration / adoption plan.** The `test-suite-parametrization` change (archived)
gave the suite a declarative *selection* mechanism; this change is about what the
tests *assert* and where expectations come from. It bundles six pillars that share
infrastructure (one structured-dump format serves three of them). Each pillar can be
adopted incrementally and mostly independently.

## Why

The current suite is corpus-based with hand-written assertions: each test encodes a
slice of expected behavior, so coverage of the real risk surface — a matrix of
format-features × readers × backends × source kinds × intents — is implicit and
uneven, and full-fidelity metadata checks exist only where someone wrote them. The
upcoming native readers (RAR, 7z, later ZIP) raise the stakes: archivey will parse
untrusted binary headers itself, and each rewrite needs a machine-checked parity
story (`docs/format-architecture-comparison.md` §10).

## The six pillars

1. **Golden metadata snapshots.** For every sample archive, a checked-in canonical
   dump (JSON) of the full expected output: member list in order, every
   `ArchiveMember` field, `ArchiveInfo`, and per-member content digests. Tests diff
   reader output against the snapshot, so every field is asserted everywhere, a
   reader change shows up as a reviewable snapshot diff, and new fixtures get
   expectations by regenerate-and-review instead of hand-writing. **The same dump
   format is the comparator for the native-vs-legacy differential tests** (rollout
   §10) — one canonicalization, two uses.

2. **External-tool ground truth.** When `create_archives.py` builds a fixture with an
   external tool (`7z`, `rar`, `bsdtar`, …), capture the tool's own listing
   (`7z l -slt`, `unrar vt`, `bsdtar -tvf`) alongside the fixture and assert archivey
   agrees on the overlapping fields. This catches the failure mode snapshots cannot:
   our reader and our snapshot being wrong together (both produced by us). Recorded
   at creation time, so CI does not need the tools installed.

3. **Reader conformance suite.** One format-agnostic test module that runs the live
   spec scenarios against *every* reader: context-manager contract, member-id
   stability, `get_members_if_available` never scanning, streaming-mode
   restrictions, error taxonomy (always `ArchiveError` subclasses), stream
   independence (from `concurrent-member-access`), cost-property consistency (from
   `base-reader-architecture-extensions`). New readers — including each native
   rewrite — get a conformance gate for free, and the specs stay honest because
   their scenarios are executable.

4. **Randomized robustness testing (fuzzing, layered).** The existing corrupted/
   truncated-archive tests are deterministic and narrow: a few fixed corruption
   kinds (`random`-range, zeroes, ffs), one mutation per kind per archive, seeded
   from the data. They are good regression tests but explore a tiny corner of the
   input space and rarely hit deep parser logic. Keep them, and add layers above
   (see design discussion in tasks):
   - **Seeded mutation sweep** in the normal suite: many mutations per archive per
     run (bit flips, truncations at every structural boundary, length/offset/count
     field tampering), driven by a fixed seed so CI is reproducible; a nightly job
     can rotate seeds and report the failing seed.
   - **Structure-aware mutations** for the native parsers: corrupt specific parsed
     fields (vints, sizes, offsets, counts, CRCs) rather than random bytes — huge
     declared member counts, overflowing sizes, cyclic bind pairs and the like are
     what random flips almost never produce.
   - **Coverage-guided fuzzing** (e.g. atheris) per native parser, run out-of-band
     (scheduled job, not per-PR), corpus seeded from the sample archives.
   - **Promotion rule:** every input that crashes/hangs/misbehaves is minimized and
     checked in as a deterministic regression fixture — the random layers feed the
     deterministic layer.
   - **The assertion is the safety contract**, not specific errors: any input either
     parses correctly or raises an `ArchiveError` subclass — never a crash, hang,
     uncontrolled memory growth, or silent garbage — within bounded time/memory
     (catching algorithmic blowups, not just segfaults).

5. **Coverage matrix report.** Generate, from the sample-archive registry and the
   markers, a table of format-features (encryption, links, unicode names, solid,
   duplicates, timestamps, sparse, comments, …) × readers × backends × path/stream
   source × intent, showing which combinations have fixtures and which tests touch
   them. Makes "are we happy with testing?" answerable — holes become line items.

6. **Property-based round-trips** (hypothesis). Generate random file trees (names
   with unicode/edge cases, sizes around block boundaries, links), archive them via
   the existing writers in `create_archives.py`, read back, and compare against the
   source tree. Finds the cases a curated corpus never includes; failing examples
   are promoted to fixtures like fuzz findings.

## Open questions (to explore)

- Snapshot format: one JSON per archive vs one per (archive, reader)? How to express
  *expected differences* between readers/backends (e.g. streaming mode lacking
  central-directory-only fields) without duplicating whole snapshots — overlay/patch
  files, or annotated fields?
- Canonicalization details: timestamp/timezone normalization, bytes vs str fields,
  content digest algorithm, ordering guarantees for formats with unordered listings.
- Mutation budget in per-PR CI vs nightly (suite runtime is a real constraint).
- Whether structure-aware mutation lives in the parser tests (white-box, using the
  parser's own field map) or as annotated offsets in fixtures.
- hypothesis strategy scope: which writers are deterministic enough; shrinking
  behavior on archives built by external tools.
- Where the coverage matrix is published (CI artifact, committed doc, or both).

## Capabilities

### Modified Capabilities

- `test-harness`: adds snapshot verification, external ground truth, the conformance
  suite, the layered robustness contract, and the coverage matrix report
  (direction-level requirements now; refined as pillars are adopted).

## Dependencies / Sequencing

Adopt **pillars 1–3 before the native readers land** — the snapshots and conformance
suite are the parity instruments the rollout strategy (§10) assumes, and ground truth
recorded now validates the snapshots themselves. Pillar 4's structure-aware and
coverage-guided layers only become possible *with* the native parsers; the seeded
mutation sweep can start immediately. Pillars 5–6 are independent and incremental.

## Impact

- **Files (anticipated)**: `tests/archivey/snapshots/` (new), `create_archives.py`
  (ground-truth capture), a new conformance test module, `create_corrupted_archives.py`
  (mutation layers), a matrix-report script, hypothesis strategies module,
  `pyproject.toml` (dev deps: hypothesis, optionally atheris).
- **Live specs touched**: `test-harness`.
- **Design references**: `docs/format-architecture-comparison.md` §10–§11.
