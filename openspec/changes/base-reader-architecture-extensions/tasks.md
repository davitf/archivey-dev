# Implementation Tasks: Base reader architecture extensions

> Scope: §8.B–§8.E. §8.A (migrate the 7z/RAR solid readers onto the existing
> `_iter_members_and_streams_internal` hook) is folded into the native-reader
> changes (`rar-native-metadata-reader` / `sevenzip-native-metadata-reader`).

## 1. Capability/preference split (§8.B, §8.C)

- [ ] 1.1 Add `_format_supports_random_access: ClassVar[bool]`; set `False` only for
      non-seekable compressed TAR
- [ ] 1.2 Make `members_list_supported` a ClassVar on each reader instead of an
      `__init__` argument

## 2. Public introspection & types (§8.D, §8.E)

- [ ] 2.1 Add `CompressionMethod` `StrEnum` (STORED, DEFLATE, LZMA, LZMA2, ZSTD,
      BZIP2, PPMD, BCJ2, …, UNKNOWN) in `types.py`
- [ ] 2.2 Map readers' `compression_method` strings onto the enum (keep `None` when
      unknown/unreported)
- [ ] 2.3 Add `supports_random_access` and `supports_member_list` properties to
      `ArchiveReader`/`BaseArchiveReader`

## 3. Validation

- [ ] 3.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 3.2 `hatch run lint` and `hatch run test` (no behavioral regressions)
