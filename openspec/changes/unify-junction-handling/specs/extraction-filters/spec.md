## ADDED Requirements

### Requirement: Junctions with absolute targets are filtered by default

The `DATA` and `TAR` filters SHALL reject junction members whose target is an
absolute or out-of-archive path (the common case for NTFS junctions), via the
existing absolute-path / out-of-destination link-target checks, so that untrusted
archives cannot plant a host mount point on extraction. The `FULLY_TRUSTED`
filter SHALL preserve such junctions unchanged.

#### Scenario: Absolute-target junction rejected by data filter
- **WHEN** the `DATA` filter is applied to a junction whose target is an absolute
  host path
- **THEN** `ArchiveFilterError` is raised (the member is rejected)

#### Scenario: Junction preserved under fully-trusted
- **WHEN** the `FULLY_TRUSTED` filter is applied to a junction
- **THEN** the junction member is returned unchanged
