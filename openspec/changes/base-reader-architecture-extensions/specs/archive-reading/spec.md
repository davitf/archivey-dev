## ADDED Requirements

### Requirement: AccessCost classifies the cost of reaching data

An `AccessCost` enum SHALL classify, on a single shared scale, what it costs to
reach data out of order — used both for reaching a member within the archive and
for seeking within a member's stream:

- `DIRECT` — reach the target by seeking and decoding only it, with no extra
  decompression (a stored member, or a stream backed by a seekable file).
- `LIMITED` — each out-of-order access costs a *bounded* amount of extra
  decompression: back to the nearest seek/index point, never more than the spacing
  between points. A *one-time* index build (e.g. rapidgzip constructing its seek
  index, eagerly or on first seek) MAY precede this; it does not by itself disqualify
  `LIMITED`, because it is paid once and amortized over all accesses rather than per
  access. Random access in a loop is acceptable; in-order iteration is still
  marginally preferable.
- `EXPENSIVE` — each out-of-order access may decode an *unbounded* prefix — a whole
  stream or solid block up to the target, with no usable intermediate seek point.
  Random access in a loop is O(N²) and callers should strongly prefer in-order
  iteration.
- `UNAVAILABLE` — the target cannot be reached out of order at all; data is available
  only along the primary forward path (streaming iteration, or a non-seekable
  stream).

`AccessCost` SHALL be classified per archive/stream **when it is opened**, from what
the backend or format header reveals (an indexed decompressor; an xz/lzip block index
in the footer; a rewind-only stdlib backend) — by *mechanism* and worst case, not
measured per call. It describes *amortized* per-access cost in steady state, so a
one-time O(N) index build does not by itself make a stream `EXPENSIVE`. When the
structure that would distinguish `LIMITED` from `EXPENSIVE` (e.g. how many seek points
a stream has) is not known without extra work, the **worse** tier SHALL be reported.
The value is a documented cost hint, not a guarantee of a specific running time: a
`LIMITED` stream whose seek points happen to be spaced far apart (e.g. an xz file
written as one large block) carries a correspondingly loose bound.

#### Scenario: Direct access
- **WHEN** a member is reached in a ZIP, uncompressed seekable TAR, folder, ISO, or
  non-solid 7z archive
- **THEN** its access cost is `AccessCost.DIRECT`

#### Scenario: Bounded extra decompression
- **WHEN** a member is reached in a `tar.gz` backed by an indexed decompressor
  (e.g. rapidgzip), or a `tar.xz`/`tar.lz` whose block index exposes multiple seek
  points spaced through the stream
- **THEN** its access cost is `AccessCost.LIMITED` (even if a one-time index build is
  required on first use)

#### Scenario: Unbounded prefix decompression
- **WHEN** a member is reached in a solid 7z/RAR archive, a `tar.gz` backed by a
  rewind-from-start decompressor, or a `tar.xz`/`tar.lz` written as a single block
  (no usable intermediate seek point)
- **THEN** its access cost is `AccessCost.EXPENSIVE`

#### Scenario: Out-of-order access impossible
- **WHEN** an archive is opened with `streaming=True`, or from a non-seekable source
- **THEN** the member access cost is `AccessCost.UNAVAILABLE`

### Requirement: MemberListingCost classifies how cheaply members can be listed

A `MemberListingCost` enum SHALL classify how the complete member list can be obtained,
so callers can reason about cost up front instead of discovering it at runtime:

- `INDEXED` — obtainable from a catalog / central directory with at most one bounded
  seek, without scanning member data (ZIP, 7z, RAR, folder, ISO).
- `SCAN_REQUIRED` — obtainable only by a full pass over the archive body, seeking or
  reading past each member's data (a seekable TAR opened with `streaming=False`).
- `SEQUENTIAL_ONLY` — no upfront list; members are discovered only as iteration
  proceeds (a TAR opened with `streaming=True`, or any non-seekable source).

#### Scenario: Catalog format is INDEXED
- **WHEN** a ZIP, 7z, RAR, folder, or ISO archive is opened
- **THEN** `member_listing_cost` is `MemberListingCost.INDEXED`

#### Scenario: Seekable tar requires a scan
- **WHEN** a seekable tar is opened with `streaming=False`
- **THEN** `member_listing_cost` is `MemberListingCost.SCAN_REQUIRED`

#### Scenario: Streaming or non-seekable source is sequential-only
- **WHEN** a tar is opened with `streaming=True`, or any archive is opened from a
  non-seekable source
- **THEN** `member_listing_cost` is `MemberListingCost.SEQUENTIAL_ONLY`

### Requirement: Reader exposes capability-introspection properties

The reader SHALL expose a `member_access_cost` property (an `AccessCost` value) and a
`member_listing_cost` property (a `MemberListingCost` value) so callers can discover what the
archive allows, and at what cost, without invoking an operation and catching an
error. `member_access_cost` SHALL describe the cost of opening an arbitrary member out of
order; it SHALL be `AccessCost.UNAVAILABLE` exactly when out-of-order open is
impossible (streaming mode or a non-seekable source). Reading either property SHALL
NOT perform archive I/O or raise.

#### Scenario: Random-access archive reports a reachable cost
- **WHEN** an archive is opened with `streaming=False` from a seekable source
- **THEN** `member_access_cost` is one of `DIRECT`, `LIMITED`, or `EXPENSIVE` (never
  `UNAVAILABLE`)

#### Scenario: Streaming archive reports UNAVAILABLE access
- **WHEN** an archive is opened with `streaming=True`
- **THEN** `member_access_cost` is `AccessCost.UNAVAILABLE`

#### Scenario: Listing and access are independent
- **WHEN** a ZIP on a seekable stream is opened with `streaming=True`
- **THEN** `member_listing_cost` is `MemberListingCost.INDEXED` (the central directory is one
  bounded seek away) while `member_access_cost` is `AccessCost.UNAVAILABLE` (the forward
  stream cannot be re-entered out of order)

#### Scenario: Introspection does not raise
- **WHEN** `member_access_cost` or `member_listing_cost` is read
- **THEN** the value is returned without performing archive I/O or raising

#### Scenario: TAR member_access_cost derives from the decompressed stream's seek cost
- **WHEN** a TAR reader reports `member_access_cost`
- **THEN** the value equals the `seek_cost` of the decompressed stream it opens
  (reaching a member out of order is a seek on that stream): `DIRECT` for an
  uncompressed seekable tar, `LIMITED` for an indexed-decompressor `tar.gz`,
  `EXPENSIVE` for a rewind-from-start `tar.gz`, and `UNAVAILABLE` for a non-seekable
  source

## MODIFIED Requirements

### Requirement: get_members_if_available avoids full traversal

`get_members_if_available()` SHALL return the complete member list only when it is
already known or obtainable cheaply — that is, when `member_listing_cost` is `INDEXED`
(reading the catalog with at most one bounded seek) or the members have already been
fully registered by a prior iteration. It SHALL return `None` otherwise and SHALL NOT
trigger a full scan: when `member_listing_cost` is `SCAN_REQUIRED` and no prior iteration
has completed, it returns `None` (the scan is the job of `get_members()`).

#### Scenario: Catalog format returns the list cheaply
- **WHEN** `get_members_if_available()` is called on a ZIP archive
- **THEN** the member list is returned without reading all file data

#### Scenario: Scan-required tar returns None instead of scanning
- **WHEN** `get_members_if_available()` is called on a seekable tar opened with
  `streaming=False` that has not been iterated
- **THEN** `None` is returned and no full scan is performed

#### Scenario: Sequential-only tar before iteration returns None
- **WHEN** `get_members_if_available()` is called on a streaming tar that has not
  been iterated
- **THEN** `None` is returned

### Requirement: Member streams add a seek_cost AccessCost alongside seekable()

Streams returned for members SHALL continue to implement the stream protocol's
`seekable(): bool` method unchanged — it is part of the IO contract callers and the
underlying libraries depend on, and SHALL NOT be removed or redefined as a derived
view of another property. In *addition*, member streams SHALL expose a separate
`seek_cost` property (an `AccessCost` value) describing what it costs to seek *within*
the member's content. `seek_cost` refines `seekable()` — letting callers distinguish a
true random-access stream from one that is seekable only by re-decompressing — and the
two SHALL be consistent: `seekable()` returns `False` exactly when `seek_cost` is
`UNAVAILABLE`, and `True` otherwise.

The four tiers carry the same meaning as the archive-level `AccessCost` (above),
applied to within-member seeks:

- `DIRECT` — true random seek in both directions (a stored member, or a stream backed
  by a seekable file).
- `LIMITED` — backward seeks cost a bounded amount, back to the nearest seek/index
  point (an indexed-decompressor backend, or a block-indexed xz/lzip stream with
  usable seek points), possibly after a one-time index build.
- `EXPENSIVE` — backward seeks re-decompress the member from its start (a stream with
  no usable intermediate seek point: a single xz/lzip block, or a rewind-from-start
  backend).
- `UNAVAILABLE` — the stream is forward-only (`seekable()` is `False`); a member
  obtained while iterating a `streaming=True` archive.

#### Scenario: Direct seek in a stored member
- **WHEN** a member stream is obtained from a stored entry in an archive opened with
  `streaming=False`
- **THEN** `seekable()` returns `True` and `seek_cost` is `AccessCost.DIRECT`

#### Scenario: Re-decompressing backward seeks
- **WHEN** a member stream is obtained for a compressed entry in a random-access
  archive whose backend re-decompresses on backward seeks
- **THEN** `seekable()` returns `True` and `seek_cost` is `AccessCost.EXPENSIVE`

#### Scenario: Forward-only stream in streaming mode
- **WHEN** a member stream is obtained while iterating an archive opened with
  `streaming=True`
- **THEN** `seekable()` returns `False` and `seek_cost` is `AccessCost.UNAVAILABLE`

## REMOVED Requirements

### Requirement: has_random_access reports the access mode

**Reason**: superseded by the `member_access_cost` property. The boolean answered only
"can I open out of order?" — which is now `member_access_cost != UNAVAILABLE` — while
`member_access_cost` additionally reports *how expensive* out-of-order access is. Keeping
both a `has_random_access()` method and the property would be two names for an
overlapping concept, the redundancy this change is meant to remove.

**Migration**: replace `reader.has_random_access()` with
`reader.member_access_cost != AccessCost.UNAVAILABLE`.
