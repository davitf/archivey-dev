# Implementation Tasks: Test strategy

> Pillars are adoptable incrementally; 1–3 should be in place before the native
> readers land (they are the parity instruments the §10 rollout assumes).

## 1. Golden metadata snapshots (pillar 1)

- [ ] 1.1 Define the canonical dump format: ordered member list with every
      `ArchiveMember` field, `ArchiveInfo`, per-member content digest; settle
      canonicalization (timestamps/tz, bytes vs str, digest algorithm)
- [ ] 1.2 Decide how reader/backend-expected differences are expressed (overlays vs
      annotated fields) without duplicating snapshots
- [ ] 1.3 Add a `--snapshot-update` flow (regenerate + human review in PR diff)
- [ ] 1.4 Generate snapshots for the existing corpus; review once, carefully — they
      are the new source of truth
- [ ] 1.5 Convert metadata-assertion tests to snapshot comparison; keep behavioral
      tests (errors, streaming restrictions) as code
- [ ] 1.6 Reuse the dump format as the native-vs-legacy differential comparator
      (sevenzip task 5.1a, rar task 4.1a)

## 2. External-tool ground truth (pillar 2)

- [ ] 2.1 Capture external listings (`7z l -slt`, `unrar vt`, `bsdtar -tvf`) at
      fixture-creation time in `create_archives.py`; store alongside fixtures
- [ ] 2.2 Map each tool's fields onto `ArchiveMember` fields; assert agreement on the
      overlap (document known tool quirks as explicit exceptions)
- [ ] 2.3 Backfill ground truth for committed fixtures where the tools are available

## 3. Reader conformance suite (pillar 3)

- [ ] 3.1 Build a format-agnostic conformance module parametrized over every reader
      via the sample-archives marker: context manager, member-id stability,
      `get_members_if_available` no-scan, streaming restrictions, ArchiveError-only
      errors, link resolution
- [ ] 3.2 Extend with the cost-property consistency checks when
      `base-reader-architecture-extensions` lands, and stream-independence checks
      from `concurrent-member-access`
- [ ] 3.3 Wire it into the native-reader changes as their conformance gate

## 4. Randomized robustness (pillar 4)

- [ ] 4.1 Keep the deterministic corruption tests as-is (regression layer)
- [ ] 4.2 Add a seeded mutation sweep: N mutations per archive per run (bit flips,
      truncation at structural boundaries, length/offset/count tampering) with a
      fixed seed in per-PR CI; nightly rotates seeds and reports the failing seed
- [ ] 4.3 Define the safety-contract assertion helper: parse correctly **or** raise
      an `ArchiveError` subclass, within a time and memory bound — never a crash,
      hang, or silent garbage
- [ ] 4.4 With the native parsers: structure-aware mutations (vints, sizes, offsets,
      counts, CRCs, bind pairs — white-box, using the parser's field map)
- [ ] 4.5 Coverage-guided fuzz harnesses (atheris) for the native RAR/7z parsers,
      corpus seeded from the sample archives, run as a scheduled job
- [ ] 4.6 Promotion rule: minimize every finding and check it in as a deterministic
      fixture with a regression test

## 5. Coverage matrix (pillar 5)

- [ ] 5.1 Script that derives the matrix (format-features × readers × backends ×
      source kind × intent) from the registry + marker usage; decide publication
      (CI artifact and/or committed report)
- [ ] 5.2 Review the first matrix; file the holes it exposes as fixtures/tests

## 6. Property-based round-trips (pillar 6)

- [ ] 6.1 hypothesis strategies for file trees (unicode/edge-case names, sizes at
      block boundaries, links, empty files, deep paths)
- [ ] 6.2 Round-trip via the writers in `create_archives.py`; compare read-back
      against the source tree; promote failing examples to fixtures
- [ ] 6.3 Bound runtime for per-PR CI (small example budget; nightly larger)

## 7. Validation

- [ ] 7.1 `openspec validate test-strategy --type change --strict`
- [ ] 7.2 `hatch run lint` and `hatch run test`
