## ADDED Requirements

### Requirement: MemberListing classifies how cheaply members can be listed

A `MemberListing` enum SHALL classify how the complete member list can be obtained,
so callers can reason about cost up front instead of discovering it at runtime:

- `INDEXED` — obtainable from a catalog / central directory with at most one bounded
  seek, without scanning member data (ZIP, 7z, RAR, folder, ISO).
- `SCAN_REQUIRED` — obtainable only by a full pass over the archive body, seeking or
  reading past each member's data (a seekable TAR opened with `streaming=False`).
- `SEQUENTIAL_ONLY` — no upfront list; members are discovered only as iteration
  proceeds (a TAR opened with `streaming=True`, or any non-seekable source).

#### Scenario: Catalog format is INDEXED
- **WHEN** a ZIP, 7z, RAR, folder, or ISO archive is opened
- **THEN** `member_listing` is `MemberListing.INDEXED`

#### Scenario: Seekable tar requires a scan
- **WHEN** a seekable tar is opened with `streaming=False`
- **THEN** `member_listing` is `MemberListing.SCAN_REQUIRED`

#### Scenario: Streaming or non-seekable source is sequential-only
- **WHEN** a tar is opened with `streaming=True`, or any archive is opened from a
  non-seekable source
- **THEN** `member_listing` is `MemberListing.SEQUENTIAL_ONLY`

### Requirement: Reader exposes capability-introspection properties

The reader SHALL expose a `supports_random_access` boolean property and a
`member_listing` property (a `MemberListing` value) so callers can discover what the
archive allows without invoking an operation and catching an error.
`supports_random_access` SHALL be `True` exactly when members can be opened
individually and out of order — the source is seekable, the format supports it, and
the archive was not opened in streaming mode. Reading either property SHALL NOT
perform archive I/O or raise.

The cost of opening a member when `supports_random_access` is `True` is not uniform:
in solid archives (some 7z/RAR) opening a member may require decompressing earlier
members in its block. This is documented rather than encoded in the property.

#### Scenario: Random-access archive reports the capability
- **WHEN** an archive is opened with `streaming=False` from a seekable source
- **THEN** `supports_random_access` is `True`

#### Scenario: Streaming archive cannot random-access
- **WHEN** an archive is opened with `streaming=True`
- **THEN** `supports_random_access` is `False`

#### Scenario: Streaming archive with a catalog still lists members cheaply
- **WHEN** a ZIP on a seekable stream is opened with `streaming=True`
- **THEN** `supports_random_access` is `False` but `member_listing` is
  `MemberListing.INDEXED`, because the central directory can be read with a single
  bounded seek without exhausting the stream

#### Scenario: Introspection does not raise
- **WHEN** `supports_random_access` or `member_listing` is read
- **THEN** the value is returned without performing archive I/O or raising

## MODIFIED Requirements

### Requirement: get_members_if_available avoids full traversal

`get_members_if_available()` SHALL return the complete member list only when it is
already known or obtainable cheaply — that is, when `member_listing` is `INDEXED`
(reading the catalog with at most one bounded seek) or the members have already been
fully registered by a prior iteration. It SHALL return `None` otherwise and SHALL NOT
trigger a full scan: when `member_listing` is `SCAN_REQUIRED` and no prior iteration
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

## REMOVED Requirements

### Requirement: has_random_access reports the access mode

**Reason**: superseded by the `supports_random_access` property, which reports the
same fact with a clearer name as part of the unified capability-introspection
surface. Keeping both a `has_random_access()` method and a `supports_random_access`
property would be two names for one concept — the redundancy this change is meant to
remove.

**Migration**: replace `reader.has_random_access()` with the
`reader.supports_random_access` property.
