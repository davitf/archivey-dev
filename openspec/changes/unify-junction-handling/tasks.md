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

> Lands after `rar-native-metadata-reader`: detection is wired into the native
> parser's RAR5 file-redirection record (the equivalent of rarfile's `file_redir`),
> not the rarfile facade. The spike (section 1) still uses rarfile in its own
> throwaway environment.

- [ ] 3.1 Expose the RAR5 redirect record (type + flags + target) on `RarMemberInfo`
      in the native parser; on the `WIN_JUNCTION` redirect type, emit
      `SYMLINK` + `extra["is_junction"]=True` + `link_target_type=DIR`
- [ ] 3.2 Populate `link_target` from the redirect record's target for junctions (today
      `_get_link_target` returns None for non-symlink/non-hardlink) with any
      normalization the spike identified
- [ ] 3.3 Tests against the RAR junction fixture

## 4. 7z reader

> Lands after `sevenzip-native-reader`: junction detection reads the native parser's
> Windows attribute bits (`FILE_ATTRIBUTE_REPARSE_POINT`, the basis of py7zr's
> `is_junction`), not the py7zr facade. Note that 7z stores a reparse point's target
> as the member's *content*, so reading the target costs a member read — in a solid
> folder, decompressing the folder prefix; reuse the mechanism the reader already
> uses for symlink targets (`_prepare_member_for_open`) instead of reading targets
> eagerly at registration.

- [ ] 4.1 Detect junctions from the native parser's attribute bits; emit the unified
      junction representation instead of `MemberType.OTHER`
- [ ] 4.2 Read and normalize the junction target into `link_target` lazily, via the
      existing link-target path (spike decides `\??\` prefix / slash normalization)
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
