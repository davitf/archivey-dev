## ADDED Requirements

### Requirement: Link target type is recorded when known

`ArchiveMember` SHALL provide an optional `link_target_type` field holding the
`MemberType` of a link's target (for example `DIR` or `FILE`) when the source
format records it, and `None` when the target type is unknown. This lets callers
and extraction learn that a link points to a directory without resolving the link.

#### Scenario: Junction target type is directory
- **WHEN** a member is a junction
- **THEN** its `link_target_type` is `MemberType.DIR`

#### Scenario: Unknown target type is None
- **WHEN** a symlink's target type is not recorded by the format
- **THEN** its `link_target_type` is `None`

#### Scenario: is_dir is unaffected by link target type
- **WHEN** a member is a symlink or junction whose `link_target_type` is `DIR`
- **THEN** `is_dir` still returns `False` (the member itself is not a directory entry)

### Requirement: Junctions are represented consistently across formats

A Windows NTFS junction SHALL be represented uniformly regardless of source
format: `type == MemberType.SYMLINK`, `extra["is_junction"] == True`,
`link_target_type == MemberType.DIR`, and `link_target` set to the junction's
target as recorded by the format (normalized to forward slashes, with any
NT-path prefix removed). `is_junction` SHALL therefore return `True`.

#### Scenario: Junction is a flagged symlink
- **WHEN** any reader reports a junction member
- **THEN** its `type` is `MemberType.SYMLINK`, `extra["is_junction"]` is `True`,
  `is_junction` returns `True`, and `link_target` holds the target

#### Scenario: Junction filename has no trailing slash
- **WHEN** a junction member is reported
- **THEN** its `filename` has no trailing `/` (it is typed as a link, not a directory)
