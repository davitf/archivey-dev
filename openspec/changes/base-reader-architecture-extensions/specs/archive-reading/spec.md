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

#### Scenario: Streaming archive reports reduced capabilities
- **WHEN** an archive is opened with `streaming=True` and the format has no early
  member list
- **THEN** `supports_random_access` is `False` and `supports_member_list` is `False`

#### Scenario: Introspection does not raise
- **WHEN** `supports_random_access` or `supports_member_list` is read
- **THEN** the value is returned without performing archive I/O or raising
