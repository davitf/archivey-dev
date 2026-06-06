# Implementation Tasks: Unify junction handling

## 1. Spike — confirm junction storage (blocking; see design.md)

- [ ] 1.1 On Windows, build the fixture tree (design Step 1) and run the Python
      inspection (Step 2) to confirm how junctions are detected and what
      `os.readlink` returns
- [ ] 1.2 Archive with WinRAR and inspect `file_redir` via rarfile (Step 3)
- [ ] 1.3 Archive with 7-Zip and inspect `is_junction` + targets via py7zr (Step 4)
- [ ] 1.4 Fill in the findings table and record whether targets are absolute or
      relative, plus any normalization needed (Step 5)
- [ ] 1.5 Copy the junction archives into `tests/test_archives_external/` as
      regression fixtures (Step 6)
- [ ] 1.6 If targets can be relative/in-archive, update the `extraction-filters`
      delta to split absolute vs in-archive junction handling

## 2. Data model

- [ ] 2.1 Add `link_target_type: Optional[MemberType] = None` to `ArchiveMember`
      with a field description ("type of the link's target when known, else None")
- [ ] 2.2 Confirm `is_junction`, `EXTRA_IS_JUNCTION`, `FA_REPARSE_POINT` are
      adequate; keep `is_dir` type-based
- [ ] 2.3 Add/adjust unit coverage for the new field defaults

## 3. RAR reader

- [ ] 3.1 Detect `file_redir[0] == rarfile.RAR5_XREDIR_WIN_JUNCTION`; emit
      `SYMLINK` + `extra["is_junction"]=True` + `link_target_type=DIR`
- [ ] 3.2 Populate `link_target` from `file_redir[2]` for junctions (today
      `_get_link_target` returns None for non-symlink/non-hardlink) with any
      normalization the spike identified
- [ ] 3.3 Tests against the RAR junction fixture

## 4. 7z reader

- [ ] 4.1 Detect `file.is_junction`; emit the unified junction representation
      instead of `MemberType.OTHER`
- [ ] 4.2 Read and normalize the junction target into `link_target`
- [ ] 4.3 Tests against the 7z junction fixture

## 5. Folder reader

- [ ] 5.1 Detect junctions via `os.path.isjunction()` (3.12+) / `st_reparse_tag`
      before the `S_ISDIR` branch; emit the unified representation
- [ ] 5.2 Stop recursing into junctions during the walk (treat as a link, not a dir)
- [ ] 5.3 Tests with a folder fixture containing a junction (Windows-gated)

## 6. Filters & docs

- [ ] 6.1 Confirm default `DATA`/`TAR` filters drop absolute-target junctions;
      add the explicit requirement/scenario
- [ ] 6.2 Document junction handling in the user guide and reference
      `design_principles.md`

## 7. Validation

- [ ] 7.1 `openspec validate unify-junction-handling --type change --strict` passes
- [ ] 7.2 `hatch run lint` and `hatch run test` pass (RAR/7z junction tests gated on
      the optional packages / unrar as usual)
