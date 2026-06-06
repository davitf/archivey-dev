# Design

Full rationale and the per-format fit are in
**`docs/format-architecture-comparison.md`** §7–§9. This file records the spec-facing
decisions.

## What is observable vs internal

| Item | Observable? | Spec impact |
|---|---|---|
| §8.A co-iteration hook | No (same `(member, stream)` output) | none — refactor only |
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
- **Co-iteration default** opens each file member via `_open_member(..., for_iteration=True)`;
  solid readers override the single hook. Filtering/selection stay in the base
  `iter_members_with_streams()`, fixing the current duplication where 7z/RAR each
  re-implement it.
