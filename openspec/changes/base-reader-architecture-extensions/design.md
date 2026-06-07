# Design

Full rationale and the per-format fit are in
**`docs/format-architecture-comparison.md`** §7–§9. This file records the spec-facing
decisions.

## Starting point (verified against current code)

- **§8.A**: the hook `_iter_members_and_streams_internal()` exists in
  `base_reader.py` (added in #209) and `iter_members_with_streams()` already routes
  through it, applying the member/filter selection centrally. However
  `SevenZipReader` and `RarReader` still override the **public**
  `iter_members_with_streams`, so the duplication this item targets is still present.
  Remaining work = migrate those two readers to the hook.
- **§8.B–E**: not implemented. `compression_method` is `Optional[str]`,
  `members_list_supported` is a constructor argument, there is no
  `_format_supports_random_access` ClassVar, and the only capability accessor is the
  `has_random_access()` method.

## What is observable vs internal

| Item | Observable? | Spec impact |
|---|---|---|
| §8.A adopt co-iteration hook | No (same `(member, stream)` output) | none — refactor only |
| §8.B `_format_supports_random_access` | No (same errors/modes) | none |
| §8.C `members_list_supported` as ClassVar | No | none |
| §8.D `CompressionMethod` enum | **Yes** (`compression_method` value type) | `archive-metadata` |
| §8.E capability properties | **Yes** (new public properties) | `archive-reading` |

So only §8.D and §8.E get delta specs; §8.A–C are captured as tasks because they
must not change behavior (the existing `archive-reading` requirements are the
regression contract).

## Decisions

- **`CompressionMethod` is a `StrEnum`** with an `UNKNOWN` fallback, so existing
  string comparisons keep working while callers gain typed values. `compression_method`
  remains optional (`None` when the format doesn't report it). The 7z native reader's
  new method names (see `sevenzip-native-metadata-reader`) should map onto this enum.
- **Capability properties are derived**, not stored: `supports_random_access` ==
  `not streaming_only`; `supports_member_list` == early-list-supported or random
  access. They complement (don't replace) `has_random_access()`.
- **Co-iteration**: the base hook already opens each file member via
  `_open_member(..., for_iteration=True)` and yields `None` for non-files, with
  filtering/selection handled in the base `iter_members_with_streams()`. The 7z and
  RAR solid paths should move into `_iter_members_and_streams_internal` overrides so
  they stop re-implementing filter/selection — that is the entirety of §8.A's
  remaining work.
