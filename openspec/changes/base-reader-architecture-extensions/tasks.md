# Implementation Tasks: Base reader architecture extensions

> Scope: §8.B–§8.F. §8.A (migrate the 7z/RAR solid readers onto the existing
> `_iter_members_and_streams_internal` hook) is folded into the native-reader
> changes (`rar-native-metadata-reader` / `sevenzip-native-reader`).

## 1. Capability/preference split (§8.B, §8.C)

- [ ] 1.1 Add a per-instance `_format_supports_random_access` flag (set in
      `__init__`, **not** a ClassVar); set `False` only when the format genuinely
      cannot random-access — i.e. a compressed TAR whose decompressor is
      non-seekable at construction time
- [ ] 1.2 Replace the `members_list_supported` `__init__` argument with a
      `has_central_directory: ClassVar[bool]` on each reader class (`True` for
      ZIP/7z/RAR/ISO/folder/single-file, `False` for TAR); remove the standalone
      boolean (it is now `member_listing_cost == INDEXED`)

## 2. Compression method types (§8.D)

- [ ] 2.1 Add `CompressionMethod` `StrEnum` (STORED, DEFLATE, LZMA, LZMA2, ZSTD,
      BZIP2, PPMD, BCJ2, …, UNKNOWN) in `types.py`; add a free-form
      `compression_method_detail: Optional[str]` field to `ArchiveMember`
- [ ] 2.2 Map readers' primary `compression_method` onto the enum (`UNKNOWN` for
      reported-but-unmapped, `None` when unreported); preserve the verbatim/full
      codec description (e.g. 7z filter chains) in `compression_method_detail`

## 3. Cost introspection (§8.E)

- [ ] 3.1 Add a `MemberListingCost` enum (`INDEXED` / `SCAN_REQUIRED` /
      `SEQUENTIAL_ONLY`) and a shared `AccessCost` enum (`DIRECT` / `LIMITED` /
      `EXPENSIVE` / `UNAVAILABLE`) to `types.py`
- [ ] 3.2 Add a `member_listing_cost: MemberListingCost` property computed **per
      instance** from `has_central_directory` **and** source seekability (do **not**
      derive `INDEXED` from the ClassVar alone); `streaming=True` on a seekable catalog
      source stays `INDEXED`
- [ ] 3.3 Add a `seek_cost: AccessCost` property to each decompressor/seekable-stream
      class (stdlib rewind wrapper, rapidgzip, indexed_bzip2, `XzDecompressorStream`,
      lzip, plain file), consistent with its `seekable()`
- [ ] 3.4 Add a `member_access_cost: AccessCost` property to
      `ArchiveReader`/`BaseArchiveReader`; have each reader report it by mechanism
      (no I/O, no raise). For `TarReader`, **read** `member_access_cost` from the
      `seek_cost` of the decompressed stream it opens (do not re-derive from
      `config.use_*`); this refines the inner-stream `seekable()` check at
      `tar_reader.py:112`
- [ ] 3.5 Add a `seek_cost: AccessCost` property to the member-stream wrapper
      (`ArchiveStream`) *alongside* the protocol-required `seekable()` (keep
      `seekable()` as-is — the IO contract); keep the two consistent (`seekable()` is
      `False` iff `seek_cost == UNAVAILABLE`), and take it from the underlying
      decompressor stream's `seek_cost` where applicable
- [ ] 3.6 Remove `has_random_access()`; migrate callers/docs to
      `member_access_cost != AccessCost.UNAVAILABLE`
- [ ] 3.7 Tighten `get_members_if_available()` so it returns the list only for
      `INDEXED` (or already-registered) members and never triggers a `SCAN_REQUIRED`
      pass

## 4. Access intent (§8.F)

- [ ] 4.1 Add an `AccessIntent` `StrEnum` (`AUTO` / `SEQUENTIAL` / `RANDOM`) to
      `types.py`
- [ ] 4.2 Add an `access_intent: AccessIntent` parameter to `open_archive`
      (default `AUTO`, accepting the string literal too) and resolve it into the
      effective backend selection (the existing `use_*` flags), without adding a
      parallel selection path
- [ ] 4.3 Make `RANDOM` best-effort: prefer rapidgzip / indexed_bzip2 / multi-block xz
      **when installed**, otherwise fall back and let the cost properties report the
      realized (`EXPENSIVE`) outcome; keep explicit `use_*` flags mandatory (still
      raise `PackageNotInstalledError` when their package is absent)
- [ ] 4.4 Raise `ValueError` for `streaming=True` together with `access_intent=RANDOM`;
      allow `streaming=True` with `AUTO`/`SEQUENTIAL`

## 5. Inefficient-access warnings (§8.G)

- [ ] 5.1 Add a `warn_on_inefficient_access: bool = False` config flag and an
      `InefficientAccessWarning` warning category
- [ ] 5.2 When enabled, emit the warning at open when `access_intent=RANDOM` but the
      realized `member_access_cost` is `EXPENSIVE`/`UNAVAILABLE` (name the cause)
- [ ] 5.3 (secondary, may split) When enabled, track per-archive access patterns and
      warn on repeated out-of-order access / re-decompressing backward seeks on an
      `EXPENSIVE` target; keep this off the hot path and silent when the flag is off

## 6. Validation

- [ ] 6.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 6.2 `hatch run lint` and `hatch run test` (no behavioral regressions; `AUTO`
      default + `warn_on_inefficient_access=False` keep current behavior)
