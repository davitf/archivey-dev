## ADDED Requirements

### Requirement: Streaming mode is selected by access intent

The archive SHALL enter forward-only "streaming mode" when `access_intent=SEQUENTIAL` or
when the source is non-seekable, and SHALL provide random access when `access_intent` is
`AUTO` or `RANDOM` and the source is seekable. In streaming mode
`iter_members_with_streams()` is usable for a single pass and the random-access methods
`open()` / `extract()` / `get_members()` are restricted. The behavior of streaming mode
SHALL be otherwise unchanged; all existing streaming-mode requirements apply regardless
of how the mode was entered.

This replaces `streaming=True` / `streaming=False` as the way streaming mode is selected;
existing scenarios that referenced those parameters are reworded to the corresponding
`access_intent` value.

#### Scenario: Sequential intent enters streaming mode
- **WHEN** an archive is opened with `access_intent="sequential"` from a seekable source
- **THEN** it is in streaming mode: iteration is single-pass and `open()` raises
  `ValueError`

#### Scenario: Default intent provides random access
- **WHEN** an archive is opened with the default intent (`AUTO`) from a seekable source
- **THEN** random-access methods (`open()`, `get_members()`) are available

#### Scenario: Non-seekable source forces streaming mode
- **WHEN** an archive is opened with `access_intent="sequential"` from a non-seekable
  source whose format supports sequential reading
- **THEN** it is in streaming mode
