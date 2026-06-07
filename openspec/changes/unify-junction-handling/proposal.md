## Why

Windows NTFS junction points (directory mount points) are handled inconsistently
and lossily across formats today, violating the "lose nothing silently" and
"least astonishment" principles:

- **RAR** carries the full junction info (`file_redir[0] == WIN_JUNCTION`, target
  in `file_redir[2]`) but the reader classifies junctions as `MemberType.OTHER`
  and returns `link_target=None` — discarding both the type and the target.
- **7z** exposes `file.is_junction` via py7zr, but the reader maps junctions to
  `MemberType.OTHER` and never reads the target.
- **Folder** reading misclassifies junctions: on Windows `stat.S_ISLNK` is false
  for a junction, so it is reported as a plain `DIR` and **recursed into**.
- **TAR**/**ZIP**/**ISO** have no first-class junction concept (bsdtar demotes
  junctions to symlinks).

Meanwhile `ArchiveMember.is_junction`, `EXTRA_IS_JUNCTION`, and `FA_REPARSE_POINT`
already exist and document the intended representation (a `SYMLINK` flagged with
`extra["is_junction"]`), but **no reader populates them**, so `is_junction` always
returns `False`.

This change unifies how junctions are *read* and represented, and records the
type of a link's target so callers no longer have to resolve a link just to learn
that it points to a directory.

## What Changes

- **New** `ArchiveMember.link_target_type: Optional[MemberType]` — the type of the
  link's target (`DIR`/`FILE`/...) when the format records it, else `None`
  (unknown). Set to `DIR` for junctions, and for symlinks when the format states
  the target is a directory.
- **Modified** junction representation: junctions are reported as
  `MemberType.SYMLINK` with `extra["is_junction"] = True`, the target preserved
  verbatim in `link_target`, and `link_target_type = DIR`. This finally makes the
  documented `is_junction` contract true.
- **Modified** `rar_reader`: detect `RAR5_XREDIR_WIN_JUNCTION` and emit the unified
  junction representation, including the target from `file_redir[2]`.
- **Modified** `sevenzip_reader`: detect `file.is_junction` and emit the unified
  junction representation, reading the target.
- **Modified** `folder_reader`: detect junctions (`os.path.isjunction()` / reparse
  tag), read the target with `os.readlink`, emit the unified representation, and
  **stop recursing into junctions**.
- **Modified** `extraction-filters`: document that junctions with absolute,
  out-of-archive targets are rejected by the default `DATA`/`TAR` filters (the
  safe default), and preserved under `FULLY_TRUSTED`.

## Capabilities

### New Capabilities

- (none — this refines existing capabilities)

### Modified Capabilities

- `archive-metadata`: adds `link_target_type`; defines the cross-format junction
  representation
- `rar-format`: recognises RAR5 junctions
- `sevenzip-format`: recognises 7z junctions
- `folder-format`: recognises on-disk junctions and does not recurse into them
- `extraction-filters`: defines default-filter behaviour for junction targets

## Non-Goals

- **Faithful extraction of junctions** (recreating an NTFS junction on Windows via
  `_winapi`/`mklink /J`, or choosing a fallback on non-Windows). This needs
  Windows CI and is deferred to a separate change. For now extraction follows the
  existing symlink path, and absolute-target junctions are typically dropped by
  the default filter anyway.
- Changing `is_dir` to follow links. `is_dir` stays "is this entry itself a
  directory"; target dir-ness is exposed via the new `link_target_type` instead.
- TAR/ZIP junction support (the formats don't carry the information).

## Open Question (resolved by a spike — see design.md)

Whether RAR/7z store junction targets as **absolute** host paths (expected) or as
**relative** paths into the archive. This decides whether the default filter drops
them. The design includes the exact Windows procedure to confirm this before the
filter requirement is finalised.

## Dependencies / Sequencing

**Land last** (after both native-reader changes).

- `rar-native-metadata-reader` and `sevenzip-native-metadata-reader` must land
  first: RAR and 7z junction detection needs to be wired into the native parsers
  built by those changes rather than the old rarfile/py7zr facades.
- Run the Windows spike (see design.md) **during the native-reader phase** so the
  junction target format (absolute host path vs relative) is confirmed before this
  change is implemented.
- `test-suite-parametrization` should be in place so junction sample archives can
  be registered declaratively.

## Impact

- **Files changed**: `types.py` (new field + junction representation),
  `formats/rar_reader.py`, `formats/sevenzip_reader.py`, `formats/folder_reader.py`
- **Live specs touched**: `archive-metadata`, `rar-format`, `sevenzip-format`,
  `folder-format`, `extraction-filters`
- **Docs**: `design_principles.md` (north star), user guide note on junctions
- **Tests**: junction sample archives for RAR/7z and a folder fixture; the spike
  archives become regression fixtures
