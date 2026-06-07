# Implementation Tasks: Base reader architecture extensions

> Scope: §8.B–§8.E. §8.A (migrate the 7z/RAR solid readers onto the existing
> `_iter_members_and_streams_internal` hook) is folded into the native-reader
> changes (`rar-native-metadata-reader` / `sevenzip-native-reader`).

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
      `SEQUENTIAL_ONLY`) and a shared `AccessCost` enum (`DIRECT` / `LIMITED` /
      `EXPENSIVE` / `UNAVAILABLE`) to `types.py`
- [ ] 2.4 Add `member_listing: MemberListing` and `member_access: AccessCost`
      properties to `ArchiveReader`/`BaseArchiveReader`; have each reader report both
      by mechanism (no I/O, no raise). For `TarReader`, derive `member_access` from the
      `seek_cost` of the decompressed stream it opens (refining the inner-stream
      `seekable()` check at `tar_reader.py:112`)
- [ ] 2.5 Remove `has_random_access()`; migrate callers/docs to
      `member_access != AccessCost.UNAVAILABLE`
- [ ] 2.6 Add a `seek_cost: AccessCost` property to the member-stream wrapper
      *alongside* the protocol-required `seekable()` (keep `seekable()` as-is — the IO
      contract); keep the two consistent (`seekable()` is `False` iff `seek_cost ==
      UNAVAILABLE`)
- [ ] 2.7 Tighten `get_members_if_available()` so it returns the list only for
      `INDEXED` (or already-registered) members and never triggers a `SCAN_REQUIRED`
      pass

## 3. Validation

- [ ] 3.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 3.2 `hatch run lint` and `hatch run test` (no behavioral regressions)
