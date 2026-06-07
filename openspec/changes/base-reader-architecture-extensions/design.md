# Design

Full rationale and the per-format fit are in
**`docs/format-architecture-comparison.md`** §7–§9. This file records the spec-facing
decisions.

## Scope (and what moved out)

This change is **§8.B–§8.E**. §8.A (the 7z/RAR co-iteration migration) is a pure
refactor with no spec delta and is handled inside the native-reader changes, which
already rewrite those readers; see the note under Decisions.

## Starting point (verified against current code)

- **§8.B–E**: not implemented. `compression_method` is `Optional[str]`,
  `members_list_supported` is a constructor argument, there is no
  `_format_supports_random_access` flag, and the only capability accessor is the
  `has_random_access()` method.
- **§8.A** (for reference): the hook `_iter_members_and_streams_internal()` already
  exists in `base_reader.py` (added in #209) and `iter_members_with_streams()` routes
  through it with central filtering; `SevenZipReader`/`RarReader` still override the
  public method. Migrating them onto the hook is owned by the native-reader changes.

## What is observable vs internal

| Item | Observable? | Spec impact |
|---|---|---|
| §8.B `_format_supports_random_access` (per-instance flag) | No (same errors/modes) | none |
| §8.C `members_list_supported` as ClassVar | No | none |
| §8.D `CompressionMethod` enum | **Yes** (`compression_method` value type) | `archive-metadata` |
| §8.E capability properties | **Yes** (new public properties) | `archive-reading` |

So only §8.D and §8.E get delta specs; §8.B/§8.C are captured as tasks because they
must not change behavior (the existing `archive-reading` requirements are the
regression contract).

## Decisions

- **`CompressionMethod` is a `StrEnum`** with an `UNKNOWN` fallback, so existing
  string comparisons keep working while callers gain typed values. `compression_method`
  remains optional (`None` when the format doesn't report it). The enum names the
  recognized **primary** codec; when the format reports a codec we don't map, it is
  `UNKNOWN` rather than `None`.
- **Lossless detail is preserved separately (Design A).** A closed enum can't
  represent 7z filter chains (`"LZMA2 + BCJ2 + Delta"`) or a third-party reader's
  own codec name, so the verbatim/full description is kept in a free-form
  `compression_method_detail: Optional[str]` field. `compression_method` stays
  branchable (typed enum, `UNKNOWN` for unmapped); `compression_method_detail`
  carries the full chain / custom name without loss. The 7z native reader populates
  both: the primary coder maps onto the enum, the full chain goes in the detail.
- **`_format_supports_random_access` is a per-instance flag, not a `ClassVar`**
  (§8.B). Whether a compressed TAR can random-access is decided at construction by
  the decompressor backend (stdlib-on-pipe vs stdlib-on-file vs
  rapidgzip/indexed_bzip2), so it is set in `__init__`, not declared on the class.
- **Capability properties are derived**, not stored: `supports_random_access` ==
  `not streaming_only`; `supports_member_list` == early-list-supported or random
  access. They complement (don't replace) `has_random_access()`.
- **§8.A is folded into the native readers**: because `rar-native-metadata-reader`
  and `sevenzip-native-metadata-reader` already rewrite those readers, each adopts the
  existing `_iter_members_and_streams_internal` hook (dropping its public
  `iter_members_with_streams` override) as part of that work, rather than as a
  separate pass here.
