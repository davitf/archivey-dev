## ADDED Requirements

### Requirement: Returned streams are public ArchiveyStream instances

Archivey SHALL provide a public stream type (`ArchiveyStream`) that is a normal binary
stream (`io.RawIOBase` / `BinaryIO`) and additionally exposes `seek_cost` (an
`AccessCost`, consistent with `seekable()`) and a `name: str | None`. Every stream
Archivey returns from `open()` and `iter_members_with_streams()` SHALL be an
`ArchiveyStream`, including streams whose bytes originate from a third-party library; such
streams SHALL be normalized into the type rather than returned raw, so callers SHALL NOT
have to special-case library-specific stream types to read `seek_cost` or `name`.

This capability is at **exploration stage**: the metadata surface beyond `seek_cost` and
`name`, the choice between a base class / mixin / protocol, and read-vs-write scope are
open and will be refined before implementation. The requirement below fixes only the
parts already decided.

#### Scenario: A returned member stream is an ArchiveyStream
- **WHEN** a member stream is obtained via `open()`
- **THEN** it is an `ArchiveyStream` exposing `seek_cost` and `name`, and is usable as a
  normal binary stream

#### Scenario: A library-backed stream is normalized
- **WHEN** a member's bytes are produced by a third-party library that returns its own
  stream object
- **THEN** the stream Archivey returns is still an `ArchiveyStream` exposing `seek_cost`
  and `name`
