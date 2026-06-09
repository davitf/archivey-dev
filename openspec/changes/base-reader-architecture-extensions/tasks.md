# Implementation Tasks: Base reader architecture extensions

> Scope: §8.B–§8.E. §8.A (migrate the 7z/RAR solid readers onto the existing
> `_iter_members_and_streams_internal` hook) is folded into the native-reader
> changes (`rar-native-metadata-reader` / `sevenzip-native-reader`). §8.F (access
> intent) is split out into the separate `access-intent` change, which builds on the
> cost introspection added here.

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

- [ ] 2.1 Add `CompressionMethod` `StrEnum` in `types.py`, **exhaustive over every
      `StreamFormat`**: STORED, DEFLATE (gzip/zlib), BZIP2, LZMA2 (xz), LZMA (lzip),
      ZSTD, LZ4, BROTLI, LZW (Unix compress), plus container-internal codecs (PPMD,
      BCJ2, DEFLATE64, DELTA, …) and UNKNOWN; add a free-form
      `compression_method_detail: Optional[str]` field to `ArchiveMember`
- [ ] 2.1a Add a regression test asserting every `StreamFormat` member resolves to a
      defined (non-`UNKNOWN`) `CompressionMethod`, so a new `StreamFormat` without its
      codec fails CI
- [ ] 2.2 Map readers' primary `compression_method` onto the enum (`UNKNOWN` for
      reported-but-unmapped, `None` when unreported); preserve the verbatim/full
      codec description (e.g. 7z filter chains) in `compression_method_detail`

## 3. Cost introspection (§8.E)

> The shared **public** `ArchiveyStream` base — guaranteeing `seek_cost` on *every*
> returned stream, including third-party library streams — is split into the separate
> `public-stream-interface` change. Here, `seek_cost` is added to archivey's own stream
> classes only.

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

## 4. Validation

- [ ] 4.1 `openspec validate base-reader-architecture-extensions --type change --strict`
- [ ] 4.2 `hatch run lint` and `hatch run test` (no behavioral regressions; cost
      properties are additive and `streaming` still drives access mode here)
