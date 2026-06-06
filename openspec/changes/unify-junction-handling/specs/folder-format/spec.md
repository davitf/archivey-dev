## ADDED Requirements

### Requirement: On-disk junctions are recognized and not recursed into

The folder reader SHALL detect a directory junction (via `os.path.isjunction()`
or the mount-point reparse tag) before treating it as a directory, emit the
unified junction representation (`MemberType.SYMLINK`, `extra["is_junction"] = True`,
`link_target_type = MemberType.DIR`, `link_target` from `os.readlink`), and SHALL
NOT recurse into the junction's target during traversal.

#### Scenario: Folder junction is a flagged symlink
- **WHEN** the folder reader encounters an NTFS junction on disk
- **THEN** the member is typed `SYMLINK` with `extra["is_junction"] == True` and a
  populated `link_target`

#### Scenario: Folder junction is not traversed
- **WHEN** the folder reader encounters a junction during the walk
- **THEN** the contents of the junction's target are not listed as members of the folder
