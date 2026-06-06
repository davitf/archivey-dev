## ADDED Requirements

### Requirement: 7z junctions are recognized

The 7z reader SHALL recognize junction entries reported by py7zr
(`file.is_junction`) and emit the unified junction representation —
`MemberType.SYMLINK`, `extra["is_junction"] = True`,
`link_target_type = MemberType.DIR`, and `link_target` read from the entry —
instead of classifying them as `MemberType.OTHER`.

#### Scenario: 7z junction member
- **WHEN** a 7z archive contains a junction entry
- **THEN** the member is typed `SYMLINK` with `extra["is_junction"] == True` and a
  populated `link_target`
