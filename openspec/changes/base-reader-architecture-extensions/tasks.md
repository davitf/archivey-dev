# Implementation Tasks: Base reader architecture extensions

> Scope: §8.B–§8.E. §8.A (migrate the 7z/RAR solid readers onto the existing
> `_iter_members_and_streams_internal` hook) is folded into the native-reader
> changes (`rar-native-metadata-reader` / `sevenzip-native-metadata-reader`).

## 1. Capability/preference split (§8.B, §8.C)

- [ ] 1.1 Add a per-instance `_format_supports_random_access` flag (set in
      `__init__`, **not** a ClassVar); set `False` only when the format genuinely
      cannot random-access — i.e. a compressed TAR whose decompressor is
      non-seekable at construction time
- [ ] 1.2 Make `members_list_supported` a ClassVar on each reader instead of an
      `__init__` argument

## 2. Public introspection & types (§8.D, §8.E)

- [ ] 2.1 Add `CompressionMethod` `StrEnum` (STORED, DEFLATE, LZMA, LZMA2, ZSTD,
      BZIP2, PPMD, BCJ2, …, UNKNOWN) in `types.py`; add a free-form
      `compression_method_detail: Optional[str]` field to `ArchiveMember`
- [ ] 2.2 Map readers' primary `compression_method` onto the enum (`UNKNOWN` for
      reported-but-unmapped, `None` when unreported); preserve the verbatim/full
      codec description (e.g. 7z filter chains) in `compression_method_detail`
- [ ] 2.3 Add a `MemberListing` enum (`INDEXED` / `SCAN_REQUIRED` /
      `SEQUENTIAL_ONLY`) and a `member_listing` property; add a
      `supports_random_access` property to `ArchiveReader`/`BaseArchiveReader`
- [ ] 2.4 Remove `has_random_access()` (superseded by `supports_random_access`);
      update callers and docs
- [ ] 2.5 Tighten `get_members_if_available()` so it returns the list only for
      `INDEXED` (or already-registered) members and never triggers a `SCAN_REQUIRED`
      pass; have each reader report its `member_listing`

## 3. Validation

- [ ] 3.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 3.2 `hatch run lint` and `hatch run test` (no behavioral regressions)
