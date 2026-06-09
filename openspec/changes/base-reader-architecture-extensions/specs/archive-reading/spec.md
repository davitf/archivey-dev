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

- `INDEXED` — the format carries a catalog / central directory (its
  `has_central_directory` class fact is true) **and that catalog is reachable on this
  instance** (the source is seekable), so the list is obtainable with at most one
  bounded seek without scanning member data (ZIP, 7z, RAR, folder, ISO on a seekable
  source).
- `SCAN_REQUIRED` — the format has no catalog but the source is seekable, so the list
  is obtainable only by a full pass over the archive body, seeking or reading past each
  member's data (a seekable TAR opened with `streaming=False`).
- `SEQUENTIAL_ONLY` — the source is non-seekable, or iteration is forward-only, so
  there is no upfront list; members are discovered only as iteration proceeds (a TAR
  opened with `streaming=True`, or any archive opened from a non-seekable source).

`member_listing_cost` SHALL be computed **per instance** from the format's
`has_central_directory` class fact **and** the runtime seekability of the source. It
SHALL NOT be derived from `has_central_directory` alone: a catalog format opened from a
non-seekable source cannot reach its catalog and is `SEQUENTIAL_ONLY`. The caller's
`streaming=True` preference alone SHALL NOT downgrade `INDEXED` when the catalog
remains reachable (the source is seekable).

#### Scenario: Reachable catalog is INDEXED
- **WHEN** a ZIP, 7z, RAR, folder, or ISO archive is opened from a seekable source
- **THEN** `member_listing_cost` is `MemberListingCost.INDEXED`

#### Scenario: Catalog format on a non-seekable source is sequential-only
- **WHEN** a catalog format is opened from a non-seekable source (its end-of-file
  catalog cannot be reached)
- **THEN** `member_listing_cost` is `MemberListingCost.SEQUENTIAL_ONLY`

#### Scenario: Seekable tar requires a scan
- **WHEN** a seekable tar is opened with `streaming=False`
- **THEN** `member_listing_cost` is `MemberListingCost.SCAN_REQUIRED`

#### Scenario: Streaming tar or non-seekable source is sequential-only
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

### Requirement: Decompressor streams expose seek_cost and readers consume it

Each decompressor/seekable-stream class SHALL expose a `seek_cost` property (an
`AccessCost` value) describing the cost of seeking within that stream, consistent with
its `seekable()` (`seekable()` is `False` exactly when `seek_cost` is `UNAVAILABLE`).
This covers the stdlib rewind-from-start wrapper, rapidgzip, indexed_bzip2, the xz and
lzip decompressor streams, and a plain seekable file. The `seek_cost` SHALL reflect the
stream's own mechanism (a plain file or stored data is
`DIRECT`; rapidgzip / indexed_bzip2 / a multi-block xz reader is `LIMITED`; a single xz
block or a rewind-from-start wrapper is `EXPENSIVE`; a forward-only stream is
`UNAVAILABLE`).

A reader whose out-of-order member access *is* a seek on such a stream — notably
`TarReader` — SHALL derive its `member_access_cost`, and the `seek_cost` of the member
streams it serves, by **reading the decompressed stream's `seek_cost`**, rather than
re-deriving the cost from configuration flags. This keeps a single source of truth in
the stream layer where backend selection happens.

#### Scenario: A decompressor stream reports its own seek cost
- **WHEN** a multi-block xz decompressor stream is opened over a seekable source
- **THEN** its `seek_cost` is `AccessCost.LIMITED`, while a rewind-from-start stdlib
  gzip wrapper opened over the same kind of source reports `AccessCost.EXPENSIVE`

#### Scenario: TAR reads the decompressed stream's seek cost
- **WHEN** a multi-block `tar.xz` is opened with `streaming=False`
- **THEN** `member_access_cost` equals the xz stream's `seek_cost` (`AccessCost.LIMITED`),
  not `AccessCost.EXPENSIVE`

#### Scenario: TAR access cost tracks the decompressor across backends
- **WHEN** a TAR reader reports `member_access_cost`
- **THEN** the value equals the `seek_cost` of the decompressed stream it opens:
  `DIRECT` for an uncompressed seekable tar, `LIMITED` for an indexed-decompressor
  `tar.gz`, `EXPENSIVE` for a rewind-from-start `tar.gz`, and `UNAVAILABLE` for a
  non-seekable source

### Requirement: Returned streams share a common Archivey stream interface

Every stream Archivey returns from `open()` and `iter_members_with_streams()` SHALL be
an instance of a common base type (`ArchiveyStream`) that exposes the `seek_cost`
introspection (above) and a `name: str | None` (the member path or source name,
mirroring stdlib file objects' `.name`). This SHALL hold whether the bytes originate
from an Archivey stream class or from a third-party library's stream object. A stream obtained from an underlying library
SHALL be normalized into this interface (the existing protocol-normalizing wrapper is the
natural place) so that `seek_cost` and `name` are always present; callers SHALL NOT have
to special-case library-specific stream types to read them.

#### Scenario: A library-backed member stream still exposes the interface
- **WHEN** a member stream is obtained for a format whose decoding is delegated to a
  third-party library
- **THEN** the returned object exposes both `seek_cost` (an `AccessCost`) and `name`,
  consistent with its `seekable()`

#### Scenario: Member name is available on the stream
- **WHEN** a member stream is obtained via `open()`
- **THEN** its `name` reflects the member's path within the archive

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
`UNAVAILABLE`, and `True` otherwise. Where the member's bytes come from a decompressor
stream (e.g. a TAR member), the member stream's `seek_cost` SHALL be taken from that
stream's own `seek_cost` rather than re-derived.

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
