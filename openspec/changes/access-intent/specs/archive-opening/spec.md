## ADDED Requirements

### Requirement: open_archive accepts an access_intent

`open_archive()` SHALL accept an `access_intent` parameter — an `AccessIntent` value or
the equivalent string literal (`"auto"`, `"sequential"`, `"random"`) — defaulting to
`AccessIntent.AUTO`, declaring how the caller intends to access the archive. The value
SHALL be passed to the reader so that both backend selection and reader strategy can
honor it.

- `AUTO` (default) — no declared pattern. Random-access methods are available when the
  source is seekable; archivey SHALL use the best installed backend the configuration
  permits (on a seekable source, an installed faster/indexed backend such as rapidgzip
  or indexed_bzip2 — see the `configuration` per-flag mapping).
- `SEQUENTIAL` — the caller will iterate forward only. The archive opens in streaming
  (forward-only) mode; random-access methods are restricted as in streaming mode;
  archivey SHALL prefer the cheapest single-pass strategy the configuration permits
  (e.g. the unrar streaming reader for solid RAR).
- `RANDOM` — the caller will reach members out of order and/or seek within members.
  Backend selection matches `AUTO`; additionally archivey SHALL signal the reader to
  retain/build seek points eagerly.

The `streaming` and `streaming_only` parameters SHALL NOT exist; their behavior is
expressed through `access_intent` (`streaming=True` → `SEQUENTIAL`; `streaming=False`,
the previous default → `AUTO`).

#### Scenario: Default access intent is AUTO
- **WHEN** `open_archive(seekable_source)` is called without `access_intent`
- **THEN** the archive opens with `AccessIntent.AUTO` and random-access methods are
  available, using the best installed backend the configuration permits

#### Scenario: Sequential intent opens streaming mode
- **WHEN** `open_archive(source, access_intent="sequential")` is called
- **THEN** the archive opens in forward-only streaming mode

#### Scenario: String literal is accepted
- **WHEN** `open_archive(source, access_intent="random")` is called
- **THEN** it is treated as `AccessIntent.RANDOM`

#### Scenario: Removed streaming parameter is rejected
- **WHEN** `open_archive(path, streaming=True)` is called
- **THEN** a `TypeError` is raised (unexpected keyword argument), because `streaming`
  has been removed in favor of `access_intent`

## MODIFIED Requirements

### Requirement: Non-seekable sources require streaming mode

`open_archive()` SHALL raise `ArchiveStreamNotSeekableError` when opening from a stream
that is not seekable while `access_intent` is `AUTO` (the default) or `RANDOM`, rather
than attempting random access. Under `access_intent=SEQUENTIAL` a non-seekable source is
accepted, except that it SHALL still raise `ArchiveStreamNotSeekableError` when the
resolved format cannot operate on a non-seekable source (e.g. ZIP, which requires the
end-of-central-directory record at the tail of the stream).

#### Scenario: Non-seekable source without sequential intent
- **WHEN** `open_archive(non_seekable_stream)` is called with the default intent (`AUTO`)
  or `access_intent="random"`
- **THEN** `ArchiveStreamNotSeekableError` is raised

#### Scenario: Non-seekable source with sequential intent, format supports sequential reading
- **WHEN** `open_archive(non_seekable_stream, access_intent="sequential")` is called and
  the format supports sequential reading
- **THEN** an `ArchiveReader` is returned in streaming mode

#### Scenario: Non-seekable source with sequential intent, format cannot stream
- **WHEN** `open_archive(non_seekable_stream, access_intent="sequential")` is called and
  the resolved format cannot operate on a non-seekable source (such as ZIP)
- **THEN** `ArchiveStreamNotSeekableError` is raised

#### Scenario: Seekable source is rewound
- **WHEN** a seekable stream is passed to `open_archive()`
- **THEN** the stream is seeked to position 0 before reading begins

## REMOVED Requirements

### Requirement: streaming_only is a deprecated alias for streaming

**Reason**: the `streaming` parameter is removed in favor of `access_intent`, so its
already-deprecated alias `streaming_only` is removed together with it.

**Migration**: replace `streaming_only=True` (or `streaming=True`) with
`access_intent="sequential"`; the previous default `streaming=False` is the default
`access_intent="auto"`.
