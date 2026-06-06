## ADDED Requirements

### Requirement: RAR5 junctions are recognized

The RAR reader SHALL recognize RAR5 Windows junction redirections
(`file_redir[0] == RAR5_XREDIR_WIN_JUNCTION`) and emit the unified junction
representation — `MemberType.SYMLINK`, `extra["is_junction"] = True`,
`link_target_type = MemberType.DIR`, and `link_target` taken from
`file_redir[2]` — instead of classifying them as `MemberType.OTHER` with no target.

#### Scenario: RAR5 junction member
- **WHEN** a RAR5 archive contains a Windows junction
- **THEN** the member is typed `SYMLINK` with `extra["is_junction"] == True` and a
  populated `link_target`

#### Scenario: RAR5 junction target is preserved
- **WHEN** a RAR5 junction's redirection records a target path
- **THEN** the member's `link_target` reflects that target (rather than `None`)
