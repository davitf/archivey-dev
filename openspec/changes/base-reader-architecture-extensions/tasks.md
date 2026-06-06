# Implementation Tasks: Base reader architecture extensions

## 1. Co-iteration hook (§8.A)

- [ ] 1.1 Add `_iter_members_and_streams()` to `BaseArchiveReader` with a default
      that opens each file member (`for_iteration=True`) and yields `None` for others
- [ ] 1.2 Route `iter_members_with_streams()` through the hook, keeping
      selection/filtering/auto-close in the base method
- [ ] 1.3 Reimplement the 7z and RAR (`use_rar_stream`) solid paths as overrides of
      the hook; delete their bespoke `iter_members_with_streams` overrides
- [ ] 1.4 Verify observable behavior is unchanged (existing `archive-reading` tests)

## 2. Capability/preference split (§8.B, §8.C)

- [ ] 2.1 Add `_format_supports_random_access: ClassVar[bool]`; set `False` only for
      non-seekable compressed TAR
- [ ] 2.2 Make `members_list_supported` a ClassVar on each reader instead of an
      `__init__` argument

## 3. Public introspection & types (§8.D, §8.E)

- [ ] 3.1 Add `CompressionMethod` `StrEnum` (STORED, DEFLATE, LZMA, LZMA2, ZSTD,
      BZIP2, PPMD, BCJ2, …, UNKNOWN) in `types.py`
- [ ] 3.2 Map readers' `compression_method` strings onto the enum (keep `None` when
      unknown/unreported)
- [ ] 3.3 Add `supports_random_access` and `supports_member_list` properties to
      `ArchiveReader`/`BaseArchiveReader`

## 4. Validation

- [ ] 4.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 4.2 `hatch run lint` and `hatch run test` (no behavioral regressions)
