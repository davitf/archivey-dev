# Design

Full rationale and the per-format fit are in
**`docs/format-architecture-comparison.md`** §7–§9. This file records the spec-facing
decisions.

## Scope (and what moved out)

This change is **§8.B–§8.G** (§8.F — access intent — and §8.G — inefficient-access
warnings — are new, not in the original doc list). §8.A (the 7z/RAR co-iteration
migration) is a pure refactor with no spec delta
and is handled inside the native-reader changes, which already rewrite those readers;
see the note under Decisions.

## Starting point (verified against current code)

- **§8.B–G**: not implemented. `compression_method` is `Optional[str]`,
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
| §8.F `AccessIntent` enum + `access_intent` open parameter | **Yes** (new public input; affects backend selection) | `archive-reading` |
| §8.G `warn_on_inefficient_access` flag + `InefficientAccessWarning` | **Yes** (opt-in, default off) | `archive-reading` |

So §8.D, §8.E, §8.F and §8.G get delta specs; §8.B/§8.C are captured as tasks because
they must not change behavior (the existing `archive-reading` requirements are the
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
- **Access intent is the input dual of the cost surface** (§8.F). Cost introspection
  alone tells callers *what they got* but makes them responsible for *configuring* the
  backend that produces a good cost (today: knowing to set `use_rapidgzip=True`). The
  `access_intent` parameter lets the caller state the *goal* and have archivey choose:
  - **`AccessIntent` is a `StrEnum`**: `AUTO` (default), `SEQUENTIAL`, `RANDOM`.
    `AUTO` preserves today's behavior exactly — honor the explicit `use_*` config flags
    and select no optional backend on the caller's behalf — so the change is additive
    and the default is unsurprising. `SEQUENTIAL` favors the cheapest streaming backend
    and skips eager index building. `RANDOM` prefers seekable/indexed backends
    (rapidgzip, indexed_bzip2, multi-block xz) **when their packages are installed**.
  - **`access_intent` is resolved into the existing low-level `use_*` flags**, not a
    parallel selection mechanism. `open_archive` computes an effective config from the
    intent and passes it down the unchanged stream-opening path. This keeps one
    backend-selection codepath and means intent is purely a high-level shorthand.
  - **Intent is best-effort; explicit flags are mandatory.** Setting `use_rapidgzip=True`
    explicitly is a hard requirement (raises `PackageNotInstalledError` if rapidgzip is
    absent — unchanged). `access_intent=RANDOM` is a preference: if the preferred
    optional package is missing it falls back to the stdlib backend and lets
    `member_access_cost`/`seek_cost` report the realized (`EXPENSIVE`) outcome rather
    than raising. Likewise a format that simply cannot do cheap random access (solid 7z,
    single-block xz) honors the request as best it can and reports the true cost.
  - **`streaming=True` + `RANDOM` is contradictory → `ValueError`.** `streaming=True`
    asserts forward-only use; `RANDOM` asserts out-of-order use. `streaming=True` with
    `AUTO`/`SEQUENTIAL` is fine (and implies sequential).
- **Inefficient-access warnings are opt-in and off by default** (§8.G). A
  `warn_on_inefficient_access` config flag (default `False`) gates a dedicated
  `InefficientAccessWarning` category, so the default experience is unchanged and there
  is no warning spam. Two triggers, in increasing implementation cost:
  - **Open-time (core):** when `access_intent=RANDOM` was requested but the realized
    `member_access_cost` is `EXPENSIVE`/`UNAVAILABLE`, warn once at open, naming the
    cause (preferred package missing, non-seekable source, or a format that cannot
    random-access cheaply). This is cheap — it reads the cost already computed at open.
  - **Runtime (secondary):** when repeated out-of-order member access, or repeated
    re-decompressing backward seeks within a member, occur on an `EXPENSIVE` target,
    warn about the O(N²) pattern. This needs per-archive access-pattern tracking;
    it is specified at the behavioral level and MAY be split into a follow-up change if
    the bookkeeping grows. The open-time warning is the part that must land here.
  - Warnings are derived from the coarse `AccessCost` tier, **not** from measurement,
    and never change which bytes are returned. The honest cost properties remain the
    primary mechanism; warnings are a convenience for callers who opt in.
- **§8.A is folded into the native readers**: because `rar-native-metadata-reader`
  and `sevenzip-native-reader` already rewrite those readers, each adopts the
  existing `_iter_members_and_streams_internal` hook (dropping its public
  `iter_members_with_streams` override) as part of that work, rather than as a
  separate pass here.
