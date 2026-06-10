## ADDED Requirements

### Requirement: Sample-archive expectations are golden snapshots

Each sample archive SHALL have a checked-in canonical snapshot of its expected
reading outcome — the ordered member list with every `ArchiveMember` field,
`ArchiveInfo`, and per-member content digests — and metadata tests SHALL compare
reader output against the snapshot rather than hand-written per-field assertions.
Snapshots SHALL be regenerable via an explicit update flow whose diff is reviewed.
The same canonical dump format SHALL serve as the comparator for native-vs-legacy
differential tests.

#### Scenario: Reader change surfaces as a snapshot diff
- **WHEN** a reader change alters any reported metadata field for a sample archive
- **THEN** the snapshot comparison fails, identifying the exact archive, member, and
  field

#### Scenario: New fixture gains full-fidelity expectations
- **WHEN** a new sample archive is added and the snapshot update flow is run
- **THEN** a complete snapshot is generated for review, with no hand-written
  assertions required

### Requirement: External tool listings serve as ground truth

The archive-creation flow SHALL capture, for every sample archive created with an
external tool, the tool's own listing at creation time, stored alongside the
fixture; a test SHALL assert that archivey's reported metadata agrees with the
tool's on the overlapping fields (with documented exceptions for known tool quirks).
CI SHALL NOT require the external tools (the listings are recorded, not
regenerated).

#### Scenario: Disagreement with the creating tool fails
- **WHEN** archivey reports a member size that differs from the recorded `7z l -slt`
  listing for that fixture
- **THEN** the ground-truth test fails, distinguishing "our reader is wrong" from
  "our snapshot is wrong"

### Requirement: Every reader passes a shared conformance suite

A format-agnostic conformance suite SHALL run the cross-format behavioral contract —
context-manager protocol, member-id stability, `get_members_if_available` never
scanning, streaming-mode restrictions, errors as `ArchiveError` subclasses, link
resolution, and (as they land) cost-property consistency and stream independence —
against every reader, parametrized over the sample-archive registry.

#### Scenario: A new reader is gated on conformance
- **WHEN** a new reader (e.g. a native RAR/7z/ZIP reader) is added
- **THEN** it is covered by the conformance suite without writing new per-contract
  tests

### Requirement: Corrupted-input handling is tested in layers up to a safety contract

Robustness testing SHALL be layered: the deterministic corruption fixtures are kept
as regressions; a seeded randomized mutation sweep (reproducible from its reported
seed) runs in the regular suite; structure-aware mutations and coverage-guided
fuzzing target the native parsers out-of-band. Every distinct finding SHALL be
minimized and promoted to a deterministic regression fixture. The assertion at every
layer is the **safety contract**: for any input, archivey either parses it correctly
or raises an `ArchiveError` subclass within bounded time and memory — never a crash,
hang, uncontrolled allocation, or silent garbage.

#### Scenario: Random mutation produces a clean failure
- **WHEN** a mutated archive from the seeded sweep is opened and fully read
- **THEN** the result is either correct parsing or an `ArchiveError` subclass, within
  the layer's time and memory bounds

#### Scenario: A fuzz finding becomes a regression test
- **WHEN** an out-of-band fuzzing run finds an input that violates the safety
  contract
- **THEN** the minimized input is checked in as a fixture with a deterministic test

### Requirement: Test coverage is reported as a feature matrix

The suite SHALL be able to generate a coverage matrix derived from the
sample-archive registry and marker usage — format features × readers × backends ×
source kind × access intent — showing which combinations have fixtures and tests, so
coverage gaps are visible line items rather than implicit.

#### Scenario: A gap is visible
- **WHEN** no fixture exercises encrypted RAR over a non-seekable source
- **THEN** the matrix shows that cell as uncovered
