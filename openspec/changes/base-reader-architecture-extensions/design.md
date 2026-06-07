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
| §8.E capability introspection (`MemberListing` + `AccessCost` enums, `member_listing` / `member_access`, add stream `seek_cost` alongside `seekable()`, drop `has_random_access`) | **Yes** (public API surface change) | `archive-reading` |

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
- **Capability introspection is redesigned around two orthogonal axes, each a
  cost-classifying enum** (§8.E), because the old `streaming` / `has_random_access()`
  / "member list supported" set conflated listing cost, access cost, and the user's
  streaming preference — and used booleans, which forced genuinely different costs to
  share a value:
  - `member_listing: MemberListing` (`INDEXED` / `SCAN_REQUIRED` / `SEQUENTIAL_ONLY`)
    — *listing cost*. A boolean here is what made TAR lie: "one bounded seek to a
    catalog" (ZIP) and "O(N) full pass" (seekable TAR) are different costs, so they
    get different values. `INDEXED` means the list is readable with ≤ one seek without
    exhausting the stream.
  - `member_access: AccessCost` (`DIRECT` / `LIMITED` / `EXPENSIVE` / `UNAVAILABLE`)
    — *cost of opening a member out of order*. **Replaces** both `has_random_access()`
    and a `supports_random_access` boolean: the boolean equalled `not streaming` for
    every openable archive (non-seekable sources are forced to streaming), so it
    carried no information beyond the preference flag. The enum does: "can I?" is
    `!= UNAVAILABLE`, and `DIRECT` (ZIP) vs `LIMITED` (rapidgzip `tar.gz`) vs
    `EXPENSIVE` (solid 7z, rewind `tar.gz`) tells a caller whether random access in a
    loop is fine or an O(N²) trap.
  - **`AccessCost` is one shared scale** reused for member-stream *seek cost*. The
    stream protocol requires `seekable(): bool`, so it stays exactly as-is (the IO
    contract callers and `tarfile`/`zipfile` depend on); `seek_cost: AccessCost` is an
    *additional, separate* property layered alongside it, not a replacement. `seek_cost`
    refines the bool — letting callers tell a true random-access stream (`DIRECT`) from
    one seekable only by re-decompressing (`EXPENSIVE`) — and the two MUST stay
    consistent (`seekable()` is `False` iff `seek_cost` is `UNAVAILABLE`). Listing keeps
    its own vocabulary because enumerating a catalog is a different operation from
    reaching bytes; the two reach-bytes operations share `AccessCost`.
  - **A TAR reader derives its own `member_access` from the seek cost of the
    decompressed stream it opens.** Reaching a member out of order means seeking that
    inner stream to the member's offset, so the archive's access cost *is* the stream's
    `seek_cost`: an uncompressed seekable tar inherits `DIRECT`, a rapidgzip-backed
    `tar.gz` inherits `LIMITED`, a rewind-from-start `tar.gz` inherits `EXPENSIVE`, and a
    non-seekable piped stream inherits `UNAVAILABLE` (forcing streaming). This is the
    concrete payoff of sharing one scale across both axes — the archive-level tier is
    computed from the stream-level tier rather than re-derived. (Today `TarReader` only
    checks the inner stream's `seekable()` bool at `tar_reader.py:112`; the tiers refine
    that same decision.)
  - Both enums are classified **per archive/stream by mechanism** (worst-case tier),
    derived at open time, never measured per call and never performing I/O to answer.
    `get_members_if_available()` is tightened to honor `member_listing`: it returns
    the list only when `INDEXED` or members are already registered, and **never
    triggers a `SCAN_REQUIRED` pass** (that is `get_members()`'s job) — aligning the
    method with its already-documented "avoids full traversal" contract, which the
    current code violates for seekable TAR.
- **§8.A is folded into the native readers**: because `rar-native-metadata-reader`
  and `sevenzip-native-metadata-reader` already rewrite those readers, each adopts the
  existing `_iter_members_and_streams_internal` hook (dropping its public
  `iter_members_with_streams` override) as part of that work, rather than as a
  separate pass here.
