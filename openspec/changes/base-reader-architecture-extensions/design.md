# Design

Full rationale and the per-format fit are in
**`docs/format-architecture-comparison.md`** §7–§9. This file records the spec-facing
decisions.

## Scope (and what moved out)

This change is **§8.B–§8.E**. §8.F (access intent) is split into its own `access-intent`
change, which depends on the cost surface added here. §8.A (the 7z/RAR co-iteration
migration) is a pure refactor with no spec delta and is handled inside the native-reader
changes, which already rewrite those readers; see the note under Decisions.

## Starting point (verified against current code)

- **§8.B–E**: not implemented. `compression_method` is `Optional[str]`,
  `members_list_supported` is a constructor argument, there is no
  `_format_supports_random_access` flag, the only capability accessor is the
  `has_random_access()` method, and there is no `access_intent` input (callers set the
  `use_*` backend flags by hand).
- **§8.A** (for reference): the hook `_iter_members_and_streams_internal()` already
  exists in `base_reader.py` (added in #209) and `iter_members_with_streams()` routes
  through it with central filtering; `SevenZipReader`/`RarReader` still override the
  public method. Migrating them onto the hook is owned by the native-reader changes.
- **PR #221 (`claude/gracious-tesla-lg0VK`)** is a draft implementation reviewed while
  writing this change. Two defects it surfaced are now pinned down by the decisions
  below: (1) it made `members_list_supported` a `ClassVar` and derived `INDEXED` from
  it alone, ignoring seekability; (2) it added `seek_cost` to the per-member
  `ArchiveStream` but **not** to the underlying decompressor streams, forcing
  `TarReader` to re-derive the cost from `config.use_*` flags (which already
  mis-reports multi-block `tar.xz`).

## What is observable vs internal

| Item | Observable? | Spec impact |
|---|---|---|
| §8.B `_format_supports_random_access` (per-instance flag) | No (same errors/modes) | none |
| §8.C `has_central_directory` ClassVar (replaces `members_list_supported` arg) | No (listing cost unchanged where reachable) | none directly; feeds §8.E |
| §8.D `CompressionMethod` enum | **Yes** (`compression_method` value type) | `archive-metadata` |
| §8.E cost introspection (`MemberListingCost` + `AccessCost` enums, `member_listing_cost` / `member_access_cost`, stream + decompressor `seek_cost` alongside `seekable()`, drop `has_random_access`) | **Yes** (public API surface change) | `archive-reading` |

So §8.D and §8.E get delta specs; §8.B/§8.C are captured as tasks because they
must not change behavior (the existing `archive-reading` requirements are the
regression contract). §8.F (access intent) is specified in the separate
`access-intent` change.

## Decisions

- **`CompressionMethod` is a `StrEnum`** with an `UNKNOWN` fallback, so existing
  string comparisons keep working while callers gain typed values. `compression_method`
  remains optional (`None` when the format doesn't report it). The enum names the
  recognized **primary** codec; when the format reports a codec we don't map, it is
  `UNKNOWN` rather than `None`.
- **The enum is exhaustive over the formats Archivey handles, not a sample.** PR #221
  added only the codecs that appeared as examples; this is a bug-in-waiting. The enum
  SHALL carry a value for **every `StreamFormat` member** — `STORED`/uncompressed,
  `DEFLATE` (gzip, zlib), `BZIP2`, `XZ`/`LZMA2` (xz), `LZMA` (lzip), `ZSTD`, `LZ4`,
  `BROTLI` (the one PR #221 missed), `LZW` (Unix `compress`, `StreamFormat.UNIX_COMPRESS`)
  — **plus** the container-internal codecs that never appear as a standalone stream
  (`LZMA`, `PPMD`, `BCJ2`, `DEFLATE64`, `DELTA`, …) used by ZIP/7z/RAR. A regression test
  SHALL assert that every `StreamFormat` resolves to a non-`UNKNOWN` `CompressionMethod`,
  so adding a new `StreamFormat` without its codec fails CI. (Note the mapping is
  many-to-one: gzip and zlib both decode to `DEFLATE`; `StreamFormat` is the wrapper,
  `CompressionMethod` is the codec.)
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
- **`has_central_directory` is a `ClassVar`; the realized listing cost is not**
  (§8.C). This splits a capability from a runtime fact, exactly as §8.B does for
  random access:
  - `has_central_directory: ClassVar[bool]` names the pure **format** fact — does this
    format carry a catalog/central directory at all? `True` for ZIP/7z/RAR/ISO/folder,
    `False` for TAR. As a class fact it is honestly a `ClassVar`, and the name says
    what it means (unlike `members_list_supported`, which sounded like it returned the
    list and hid the seekability dependency).
  - **`member_listing_cost` is computed per instance**, because a catalog sitting at
    end-of-file is only `INDEXED` if the source is *seekable*. The rule is
    `INDEXED` when `has_central_directory` **and the source is seekable**;
    `SCAN_REQUIRED` when there is no catalog but the source is seekable (a TAR that can
    be scanned); `SEQUENTIAL_ONLY` when the source is non-seekable. The user's
    `streaming=True` preference does **not** by itself downgrade listing for a seekable
    catalog format (the catalog is still one seek away — see the independence scenario
    in the spec).
  - The standalone `members_list_supported` boolean is dropped: it is now exactly
    `member_listing_cost == INDEXED`. Deriving `INDEXED` from the `ClassVar` alone (as
    PR #221 did) is the bug this split fixes.
- **Cost introspection is redesigned around two orthogonal axes, each a
  cost-classifying enum** (§8.E), because the old `streaming` / `has_random_access()`
  / "member list supported" set conflated listing cost, access cost, and the user's
  streaming preference — and used booleans, which forced genuinely different costs to
  share a value:
  - `member_listing_cost: MemberListingCost` (`INDEXED` / `SCAN_REQUIRED` / `SEQUENTIAL_ONLY`)
    — *listing cost* (computed as above). A boolean here is what made TAR lie: "one
    bounded seek to a catalog" (ZIP) and "O(N) full pass" (seekable TAR) are different
    costs, so they get different values.
  - `member_access_cost: AccessCost` (`DIRECT` / `LIMITED` / `EXPENSIVE` / `UNAVAILABLE`)
    — *cost of opening a member out of order*. **Replaces** both `has_random_access()`
    and a `supports_random_access` boolean: the boolean equalled `not streaming` for
    every openable archive (non-seekable sources are forced to streaming), so it
    carried no information beyond the preference flag. The enum does: "can I?" is
    `!= UNAVAILABLE`, and `DIRECT` (ZIP) vs `LIMITED` (rapidgzip `tar.gz`) vs
    `EXPENSIVE` (solid 7z, rewind `tar.gz`) tells a caller whether random access in a
    loop is fine or an O(N²) trap. **`LIMITED` is defined in amortized terms**: it is
    bounded *per access* (back to the nearest seek/index point), and a one-time O(N)
    index build (rapidgzip building its seek index, eagerly or on first seek) is folded
    in rather than broken out — it does not change the loop-vs-iterate decision (a
    *single* random open is O(N) on any non-`DIRECT` backend; the tier is about repeated
    access). The `LIMITED`/`EXPENSIVE` line is therefore "are there usable intermediate
    seek points?": a multi-block `tar.xz`/`tar.lz` is `LIMITED` (bounded by block size),
    a single-block one is `EXPENSIVE` (the only seek point is the start).
  - **`AccessCost` is one shared scale** reused for member-stream *seek cost*. The
    stream protocol requires `seekable(): bool`, so it stays exactly as-is (the IO
    contract callers and `tarfile`/`zipfile` depend on); `seek_cost: AccessCost` is an
    *additional, separate* property layered alongside it, not a replacement. `seek_cost`
    refines the bool — letting callers tell a true random-access stream (`DIRECT`) from
    one seekable only by re-decompressing (`EXPENSIVE`) — and the two MUST stay
    consistent (`seekable()` is `False` iff `seek_cost` is `UNAVAILABLE`). Listing keeps
    its own vocabulary because enumerating a catalog is a different operation from
    reaching bytes; the two reach-bytes operations share `AccessCost`.
  - **`seek_cost` is a property of the seekable-stream abstraction, and readers read it
    rather than re-deriving it.** Each decompressor/seekable stream knows its own seek
    capability: a plain file or stored member → `DIRECT`; rapidgzip / indexed_bzip2 /
    multi-block `XzDecompressorStream` → `LIMITED`; a single xz block or a
    rewind-from-start stdlib wrapper → `EXPENSIVE`; a forward-only stream →
    `UNAVAILABLE`. A `TarReader` then sets its `member_access_cost` (and the
    `seek_cost` of the member streams it serves) **from the decompressed stream's
    `seek_cost`** — reaching a member out of order is a seek on that stream, so the
    archive tier *is* the stream tier. This is the concrete payoff of one shared scale,
    and it keeps the single source of truth in the stream layer (where backend
    selection actually happens) instead of duplicating `config.use_*` logic in each
    reader. (PR #221 added `seek_cost` only to the per-member `ArchiveStream` and made
    `TarReader` re-derive the cost from config — already wrong for multi-block
    `tar.xz`, which it reported `EXPENSIVE` despite `XzDecompressorStream`'s block-level
    seeking. Hoisting `seek_cost` onto the decompressor streams removes the
    duplication and the bug.)
  - Both enums are classified **per archive/stream by mechanism** (worst-case tier),
    decided **at open** from what the backend/format header reveals (a footer block
    index read during construction is fine) and conservatively (report the worse tier)
    when the distinguishing structure isn't known without extra work. *Reading* the
    property never performs I/O or raises and is never measured per call.
    `get_members_if_available()` is tightened to honor `member_listing_cost`: it returns
    the list only when `INDEXED` or members are already registered, and **never
    triggers a `SCAN_REQUIRED` pass** (that is `get_members()`'s job) — aligning the
    method with its already-documented "avoids full traversal" contract, which the
    current code violates for seekable TAR.
  - **Computing the cost properties (pseudocode).** All three are *derived from
    mechanism*, never measured; reading them does no I/O. The algorithm:

    ```text
    # member_listing_cost — per reader instance, from the §C ClassVar + seekability
    def member_listing_cost(self):
        if not self.source.seekable():        return SEQUENTIAL_ONLY
        if type(self).has_central_directory:  return INDEXED        # catalog 1 seek away
        return SCAN_REQUIRED                                        # seekable, no catalog (TAR)

    # seek_cost — per decompressor / seekable stream, fixed at construction
    def seek_cost(self):
        if not self.seekable():               return UNAVAILABLE    # forward-only source
        if self.is_plain_file_or_stored:      return DIRECT         # true random access
        if self.has_intermediate_seek_points: return LIMITED        # rapidgzip, indexed_bzip2,
                                                                    #   multi-block xz/lzip
        return EXPENSIVE                                            # rewind-from-start / 1 block

    # member_access_cost — per reader instance; reaching a member out of order
    def member_access_cost(self):
        if not self.member_random_access_possible:  return UNAVAILABLE  # streaming / non-seekable
        if self.serves_members_from_decompressed_stream:               # TAR: a member IS a
            return self.decompressed_stream.seek_cost                  #   seek on that stream
        if self.members_stored_independently_seekably:  return DIRECT  # ZIP / stored entries
        return EXPENSIVE                                               # solid 7z, single stream
    ```

    The single source of truth for `LIMITED`/`EXPENSIVE` is the stream's own `seek_cost`
    (the `has_intermediate_seek_points` line); TAR reads it rather than re-deriving from
    `config.use_*`, which is the PR-#221 bug above.
  - **All returned streams share a common Archivey base carrying `seek_cost` and a
    `name`.** Today each archivey stream class (`ArchiveStream`, `DecompressorStream`,
    `BinaryIOWrapper`, …) independently subclasses `io.RawIOBase, BinaryIO`, and a stream
    handed back from a third-party library (py7zr / rarfile / zipfile) may not be an
    archivey type at all — so nothing *guarantees* `seek_cost` is present on a returned
    stream. This change introduces a shared base — `ArchiveyStream` (subclass of
    `io.RawIOBase, BinaryIO`) — declaring `seek_cost: AccessCost` (kept consistent with
    `seekable()`) and `name: str | None` (the member path / source name, mirroring stdlib
    file objects' `.name`). The existing classes inherit it, and **every** stream archivey
    returns from `open()` / `iter_members_with_streams()` is an `ArchiveyStream`.
    - **Foreign streams are normalised at the existing wrap point.** Library streams
      already pass through `ensure_binaryio()` / `BinaryIOWrapper` (`internal/io_helpers.py`)
      to satisfy the `BinaryIO` protocol; making that wrapper an `ArchiveyStream` means
      carrying `seek_cost` / `name` is free wherever a wrapper is already built.
      **Recommendation: wrap, don't annotate** — `setattr`-ing `seek_cost` onto the raw
      third-party object is fragile (`__slots__`, immutable handles) and only saves one
      allocation; annotating-in-place is left as a profiled optimization if the wrapper
      is ever shown to be a hotspot.
    - **Open questions (for review):** the exact metadata beyond `seek_cost` + `name`
      (candidates: a back-reference to the `ArchiveMember`, the member's
      `CompressionMethod`, the source `StreamFormat`), and whether `ArchiveyStream` is a
      public type or internal-only.
- **§8.A is folded into the native readers**: because `rar-native-metadata-reader`
  and `sevenzip-native-reader` already rewrite those readers, each adopts the
  existing `_iter_members_and_streams_internal` hook (dropping its public
  `iter_members_with_streams` override) as part of that work, rather than as a
  separate pass here.
