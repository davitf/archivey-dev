## ADDED Requirements

### Requirement: Reader exposes capability-introspection properties

The reader SHALL expose `supports_random_access` and `supports_member_list`
properties so callers can discover what the archive allows without invoking an
operation and catching an error. `supports_random_access` SHALL be `True` exactly
when random-access methods are usable; `supports_member_list` SHALL be `True` when a
member list can be obtained (from an early/central listing or because random access
is available).

#### Scenario: Random-access archive reports capabilities
- **WHEN** an archive is opened with `streaming=False`
- **THEN** `supports_random_access` is `True` and `supports_member_list` is `True`

#### Scenario: Streaming archive without an early member list reports reduced capabilities
- **WHEN** an archive is opened with `streaming=True` and the format has no early
  member list (such as a compressed TAR)
- **THEN** `supports_random_access` is `False` and `supports_member_list` is `False`

#### Scenario: Streaming archive with a central directory still lists members
- **WHEN** a ZIP on a seekable stream is opened with `streaming=True`
- **THEN** `supports_random_access` is `False` but `supports_member_list` is `True`,
  because the central directory can be read with a single bounded seek without
  exhausting the stream

#### Scenario: Introspection does not raise
- **WHEN** `supports_random_access` or `supports_member_list` is read
- **THEN** the value is returned without performing archive I/O or raising
