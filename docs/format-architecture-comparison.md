# Archive Format Architecture Comparison

This document compares all five formats archivey handles across the axes that
matter for the `BaseArchiveReader` contract: metadata availability, streaming
vs. random access, solid archives, encryption, and the impedance mismatches
between each format and the current abstract interface.  Its goal is to
identify where the current extension points are adequate and where the
architecture should evolve.

---

## 1. Format properties at a glance

| Property | ZIP | TAR | RAR | 7z | ISO |
|---|---|---|---|---|---|
| `members_list_supported` | True | False | True | True | True |
| `streaming_only` default | False | Configurable¹ | False | False | False |
| Central directory | Yes (EOCD) | No | Yes | Yes | Yes (VDs) |
| Compression granularity | Per-member | Whole-archive² | Per-member + groups (solid) | Per-folder (solid) | None (stored only) |
| Encryption granularity | Per-member (ZipCrypto) | None | Per-member + header | Per-folder | None |
| Solid support | No | Yes (compressed) | Optional | Almost always | No |
| Hardlinks | No | Yes (native) | No | No | No |
| Symlinks | Yes (Unix `external_attr`) | Yes (native) | Rarely³ | Yes | Yes (Rock Ridge only) |
| Duplicate filenames | No | Yes | No | Yes | No |
| CRC stored per member | CRC32 | None⁴ | CRC32 | CRC32 | None |
| Encryption detection | Per-member flag | N/A | Per-member + header flag | Per-folder flag | N/A |
| Library used | `zipfile` (stdlib) | `tarfile` (stdlib) | `rarfile` (third-party) | `py7zr` (third-party) | `pycdlib` (third-party, test-only) |

¹ Forced streaming when TAR is inside a non-seekable compressed stream; always
  streaming for piped stdin usage.

² "Whole-archive" because a compressed TAR is a single compression stream
  wrapping all members; individual members are not compressed independently.

³ Some RAR4 archives store symlinks; not part of the official spec and not
  widely supported.

⁴ TAR format has no checksum for file data; only the header has a simple
  checksum (sum of header bytes mod 256).

---

## 2. `BaseArchiveReader` flags

### 2.1 `members_list_supported`

If True, the archive has a "directory" structure that allows listing all
members before reading any file data.

| Format | Flag | Reason |
|---|---|---|
| ZIP | True | Central directory at EOF; all ZipInfo metadata parsed on `ZipFile()` open |
| TAR | False | No central directory; entries interleaved with data; must scan sequentially |
| RAR | True | End-of-archive block; rarfile reads all `RarInfo` objects at open time |
| 7z | True | End-header + Streams Info; py7zr reads all metadata at open |
| ISO | True | Volume Descriptor → root directory record; entire directory tree can be walked before reading data |

TAR is the outlier.  The base class handles this via the `_early_members_list_supported` path: when both `streaming_only=True` and `members_list_supported=False`, `get_members_if_available()` returns `None`.

### 2.2 `streaming_only`

This flag has two sources of truth that are currently conflated:

- **Format capability**: Can the format support random access at all?  ZIP, RAR, 7z, ISO always can. TAR can only if the underlying stream is seekable (plain TAR on disk, or compressed with an indexed backend like rapidgzip).
- **User preference**: The caller may pass `streaming_only=True` to `open_archive()` to force sequential mode even when the format supports random access, for efficiency.

Currently both are encoded in the single `_streaming_only` boolean on
`BaseArchiveReader`. The TarReader sets `streaming_only` based on the stream
type at construction time:
```python
streaming_only = streaming_only or (not is_seekable(archive_path) and ...)
```

The conflation means `get_members()` raises `ValueError` in streaming mode
even for TAR-on-disk where random access *would* work but was not requested.

---

## 3. The abstract method contract and how each format fits

### 3.1 `iter_members_for_registration()`

Yields `ArchiveMember` objects one by one.  All metadata except `link_target`
for encrypted-header archives must be populated here.

| Format | Naturalness | Notes |
|---|---|---|
| ZIP | Natural | `ZipFile.infolist()` → iterate ZipInfo objects |
| TAR | Natural | `tarfile.next()` in a loop; stream position advances |
| RAR | Natural | `rarfile.infolist()` → iterate RarInfo objects |
| 7z | Natural | `SevenZipFile.list()` → iterate py7zr FileInfo |
| ISO | Natural | `pycdlib.walk(rr_path="/")` + `get_record()` per file; or native DR walk |

All formats fit naturally here.  The only wrinkle is that for compressed TAR
the TarFile stream must remain open — `raw_info` stores the `TarInfo` and the
underlying stream position is preserved for later `extractfile()` calls.

### 3.2 `get_archive_info()`

Returns an `ArchiveInfo` with `format`, `version`, `is_solid`, `comment`,
`extra`.

| Format | `version` | `is_solid` | `comment` |
|---|---|---|---|
| ZIP | None (version_made_by not surfaced) | False | `ZipFile.comment` (bytes → str) |
| TAR | Format variant (ustar/GNU/pax) | True if compressed | None |
| RAR | "4" or "5" | From main archive block flag | `rarfile.comment` |
| 7z | None (could parse from signature header) | From folder structure | None |
| ISO | Interchange level (1/2/3) | False | Volume Identifier from PVD |

`is_solid` for 7z is determined by `_is_solid()` which checks whether any
folder has `num_unpackstreams > 1` — nearly every 7z archive is solid because
the default compressor packs all files into one folder.

### 3.3 `_open_member(member, pwd, for_iteration)`

Returns a `BinaryIO` for a single member's uncompressed data.

| Format | Random access | Notes |
|---|---|---|
| ZIP | O(1) seek | `ZipFile.open(name)` seeks to local file header |
| TAR | O(N) or O(1) | `tarfile.extractfile(info)` works if stream is seekable; otherwise must re-scan from start |
| RAR non-solid | O(1) | rarfile's "extract-hack": temp `.rar` file with one member, run unrar |
| RAR solid | O(N) | Without `use_rar_stream`: re-decompresses all preceding members each call |
| 7z | O(N) | py7zr must extract all folder members to reach target; thread+queue approach |
| ISO | O(1) | Seek to `extent_location * BLOCK_SIZE`; read `data_length` bytes |

The impedance mismatch for solid archives (RAR solid, 7z) is the core
architectural tension.  `_open_member()` is a pull-based, per-call interface
but solid archives need push-based, ordered sequential extraction.

### 3.4 `_translate_exception(e)`

Maps library exceptions to `ArchiveError` subclasses.

| Format | Exception types | Coverage |
|---|---|---|
| ZIP | `BadZipFile`, `NotImplementedError`, `RuntimeError` (wrong pwd) | Good |
| TAR | `TarError` and subclasses | Good |
| RAR | `rarfile.BadRarFile`, `rarfile.PasswordRequired`, `rarfile.NeedFirstVolume` | Good |
| 7z | `py7zr.*Error`, `PasswordRequired` | Good |
| ISO | N/A (reader not implemented) | — |

---

## 4. Solid archive strategies compared

A "solid" archive is one where extracting member N requires decompressing
members 0..N-1.  Each format handles this differently; the workarounds
expose the main limitation of the `_open_member()` per-call contract.

### 4.1 Compressed TAR (always solid)

The entire file is one decompressed stream.  `TarReader` stores the
decompressed stream and lets `tarfile` position it at each member's data
offset.  Random access means re-decompressing from the start (unless an
indexed backend like rapidgzip is used).

Strategy: sequential iteration natural; random access by stream rewind.

### 4.2 RAR solid archives

rarfile calls `unrar p` once per member: O(N²) total decompression cost.

Workaround (`use_rar_stream=True`): `RarStreamReader` spawns a single
`unrar p -inul archive.rar` subprocess and reads one `RarStreamMemberFile`
slice per member in order.  O(N) total cost.  Only works during
`iter_members_with_streams()` — calling `open()` on an individual member
still triggers the O(N) re-decompression.

### 4.3 7z (almost always solid)

py7zr extracts an entire folder into memory/disk when asked for any member
in that folder.  The `_extract_members_iterator()` method uses a background
thread and a `Queue` to bridge py7zr's push model (it calls
`StreamingFactory.create()` per member) to archivey's pull model.

Strategy:
1. First pass: yield dirs/empty files/symlinks with no stream needed.
2. Second pass: start background thread, extract all file members of a folder
   via `py7zr.SevenZipFile.extract(targets=...)`, dequeue results as
   `(member, StreamingFile)` tuples in archive order.

### 4.4 Comparison

| Strategy | Cost model | Random access | Works with `open()` |
|---|---|---|---|
| TAR rewind | O(N) per random open | Slow but correct | Yes |
| TAR indexed (rapidgzip) | O(log N) | Fast | Yes |
| RAR `use_rar_stream` | O(N) total streaming | No (streaming only) | No |
| RAR default | O(N²) total | Yes (expensive) | Yes |
| 7z thread+queue | O(folder_size) per folder | Only via `iter_members_with_streams` | Via queue drain |

The 7z approach (thread+queue) is the most sophisticated: it correctly handles
solid extraction while fitting into the archivey iterator protocol.  The RAR
stream approach is simpler but only works sequentially.

---

## 5. Metadata richness comparison

### 5.1 Timestamps

| Format | Precision | Timezone | Source |
|---|---|---|---|
| ZIP (DOS only) | 2 seconds | None (local time) | `ZipInfo.date_time` |
| ZIP (Extended, 0x5455) | 1 second | UTC | Archivey parses extra field manually |
| TAR (ustar/GNU) | 1 second | UTC | `TarInfo.mtime` (Unix timestamp) |
| TAR (PAX) | Nanosecond | UTC | `TarInfo.pax_headers["mtime"]` |
| RAR4 | 100 ns | UTC | Windows FILETIME via rarfile |
| RAR5 | 1 second or 100 ns | UTC | Windows FILETIME or UNIX time |
| 7z | 100 ns | UTC | Windows FILETIME |
| ISO (base) | 1 second | UTC offset (15-min intervals) | `DirectoryRecordDate.gmtoffset` |
| ISO (Rock Ridge TF) | 1 second | UTC offset | Rock Ridge TF extension |

ZIP is the weakest without the Extended Timestamp extra field, which archivey
explicitly parses.  All others provide UTC timestamps natively.

### 5.2 Unix permissions / ownership

| Format | Mode | UID/GID | Notes |
|---|---|---|---|
| ZIP (Unix) | Yes (`external_attr >> 16`) | No (some extra fields) | Only when `create_system == UNIX` |
| TAR | Yes (`TarInfo.mode`) | Yes (`TarInfo.uid/gid`) | Native; always present |
| RAR | Yes (Unix extra block) | Yes | Only when created on Unix |
| 7z | No | No | Not supported in format |
| ISO base | No | No | No POSIX metadata |
| ISO + Rock Ridge | Yes (`PX.posix_file_mode`) | Yes (`PX.posix_uid/gid`) | Only with Rock Ridge extension |

7z is the weakest; TAR and Rock Ridge ISO are the strongest.

### 5.3 Symlinks

| Format | Supported | How stored |
|---|---|---|
| ZIP | Yes (Unix only) | `external_attr` mode bits + file content = target |
| TAR | Yes (native) | `TarInfo.linkname` |
| RAR | Rarely | Not in official spec; limited support |
| 7z | Yes | Stored as regular file with special attribute |
| ISO base | No | Not in ISO9660 |
| ISO + Rock Ridge | Yes | `SL` System Use Entry |

### 5.4 Hardlinks

| Format | Supported | How stored |
|---|---|---|
| ZIP | No | — |
| TAR | Yes (native) | `LNKTYPE`, `linkname` points to another member |
| RAR | No | — |
| 7z | No | — |
| ISO | No (sort of) | Multiple DRs can point to same extent (not exposed) |

TAR is the only format with native hardlink support.

### 5.5 Archive-level metadata

| Format | Comment | Password-protected header | Volume label |
|---|---|---|---|
| ZIP | Yes (archive + per-member) | No | No |
| TAR | No | No | No |
| RAR | Yes | Yes (RAR5 encrypted headers) | No |
| 7z | No | No (per-folder encryption) | No |
| ISO | No comment per se | No | Volume Identifier (32 bytes) in PVD |

---

## 6. Library dependency comparison

| Format | Library | Role | Quality | Replaceability |
|---|---|---|---|---|
| ZIP | `zipfile` (stdlib) | Full in-process decompressor + parser | Excellent | Low benefit: stdlib works well |
| TAR | `tarfile` (stdlib) | Full in-process decompressor + parser | Good | Low benefit: stdlib works well |
| RAR | `rarfile` (third-party) | Metadata parsing; shells out to `unrar` for data | Medium | **High** — native reader target; eliminates dependency |
| 7z | `py7zr` (third-party) | Metadata + in-process decompressor | Medium | **High** — native reader target for metadata; keep py7zr for decompression |
| ISO | `pycdlib` (third-party) | Test-only (creation); no reader exists | N/A | Wrap pycdlib or write native (native is ~400 lines) |

The RAR and 7z native reader designs are documented in
`rar-native-reader-design.md` and `sevenzip-native-reader-design.md`.

---

## 7. Impedance mismatches with `BaseArchiveReader`

### 7.1 The registration/open split doesn't fit solid archives

`iter_members_for_registration()` yields metadata only; `_open_member()` is
called later, independently, for data.  For solid archives, these two phases
cannot be fully separated: you can list members without decompressing (metadata
lives in the header), but opening member N requires having streamed members
0..N-1 in order.

Current solutions are all workarounds:
- **7z**: overrides `iter_members_with_streams()` wholesale — the base class
  dispatch is bypassed entirely for file members.
- **RAR**: exposes `use_rar_stream` config that changes the behaviour of
  `_open_member()` at call time.
- **Compressed TAR**: the stream is seekable so backward seek-by-rewind is
  possible (expensive).

A cleaner design would introduce an optional `iter_members_with_open()`
contract that solid-archive readers implement directly, with the base class
calling it instead of the `iter_members_for_registration` + `_open_member`
two-step.

### 7.2 `streaming_only` conflates format capability and user preference

As noted in §2.2, `streaming_only` combines two orthogonal facts:
1. "This format/stream doesn't support random access" — a format invariant.
2. "The user wants sequential processing" — a runtime preference.

Currently TAR sets `streaming_only=True` when the underlying stream is
non-seekable; the user cannot override this.  A user might want to force
streaming mode on a ZIP file even though ZIP supports random access.

Proposal: split into `format_supports_random_access: bool` (class attribute)
and `streaming_only: bool` (runtime flag that can be True only if the format
supports both modes, or always True for non-seekable streams).

### 7.3 Duplicate filenames (7z, TAR)

Both 7z and TAR can contain multiple members with the same filename.  The base
class tracks members by `member_id` (sequential integer) but `get_member(str)`
returns the first match.

The 7z reader renames duplicates (`filename_2`, `filename_3`, …) at
registration time.  TAR does not rename — if the same filename appears twice,
the second `open("name")` call will return the first entry.

A principled solution: a `dedup_policy` parameter (`KEEP_FIRST`, `KEEP_LAST`,
`KEEP_ALL_RENAME`, `KEEP_ALL_BY_ID`).

### 7.4 Per-member vs archive-level password

The `open(member, pwd=...)` API allows per-call password override.  In 7z
this requires the `_temporary_password` context-manager hack because py7zr
binds the password to each `Folder` at archive-open time.

For a native 7z reader this problem goes away entirely — the key derivation
can be done per-call.  For other formats it is already natural (ZIP and RAR
accept per-call passwords).

### 7.5 ISO: no streaming concerns, but multi-namespace path system

ISO is the simplest format for data access (sector-aligned reads, no
compression) but pycdlib's API requires choosing a namespace before every
path operation.  An archivey `IsoReader` must:
1. At open time: detect which extensions are present (Rock Ridge, Joliet, UDF).
2. Pick a priority chain: Rock Ridge → Joliet → ISO9660.
3. Walk using `walk(rr_path="/")` / `walk(joliet_path="/")` / `walk(iso_path="/")`.
4. On `_open_member()`: call `open_file_from_iso()` with the correct keyword.

This namespace selection must be stored on the reader instance; `raw_info`
on each `ArchiveMember` must store enough to reconstruct the correct keyword
argument for `open_file_from_iso()`.

Alternatively, a native ISO reader avoids pycdlib entirely and uses direct
struct parsing.

---

## 8. Are the current extension points adequate?

### What works well

- **`iter_members_for_registration()` + `_open_member()`** is the right split
  for non-solid formats (ZIP, RAR non-solid, ISO).  It maps cleanly to "parse
  directory" + "seek and read".

- **`raw_info`** on `ArchiveMember` is a useful escape hatch.  Each reader
  stores format-specific data there (ZipInfo, TarInfo, RarInfo, py7zr
  FileInfo) without polluting the public `ArchiveMember` API.

- **`_prepare_member_for_open()`** hook elegantly handles the case where some
  metadata (e.g., `link_target` for encrypted RAR members) is only known after
  opening the stream.  The hook is called before `_open_member()` so the member
  can be adjusted.

- **`_translate_exception()`** cleanly isolates format-specific error handling
  from the generic logic in `ArchiveStream`.

- **`members_list_supported` flag** correctly captures the TAR/non-TAR
  distinction and drives `get_members_if_available()` behaviour.

### What needs extension

#### A. Solid-archive co-iteration contract

**Problem**: `iter_members_with_streams()` is fully overridden by 7z and
partly by RAR, duplicating the registration and iteration logic.

**Proposed addition**: a protected `_iter_members_and_streams()` hook that
the base class calls from inside `iter_members_with_streams()`:

```python
def _iter_members_and_streams(
    self,
) -> Iterator[tuple[ArchiveMember, BinaryIO | None]]:
    """
    Optional override for solid-archive readers.

    Yield (member, stream) pairs in archive order.
    stream=None means the member has no data (dir, symlink, empty file).
    The base class default calls _open_member() for each file member.
    """
    for member in self.iter_members():
        if member.is_file:
            yield member, self._open_member(member, pwd=self._pwd, for_iteration=True)
        else:
            yield member, None
```

Solid readers override this to drive a single extraction pass.  The base
class `iter_members_with_streams()` calls `_iter_members_and_streams()`
instead of its current piecemeal approach.

#### B. Separate format capability from user preference

**Problem**: `streaming_only` is both a format fact and a user preference.

**Proposed addition**: a class attribute:
```python
class BaseArchiveReader:
    _format_supports_random_access: ClassVar[bool] = True
```

Set to `False` for non-seekable compressed TAR streams (not for all TAR — only
when the underlying decompressor is non-seekable). The runtime `streaming_only`
flag then means "user requested streaming OR format cannot random-access".

#### C. `members_list_supported` should be a class attribute

Currently passed to `__init__()` as a constructor argument.  But it's
determined solely by the format type, not by any runtime condition.

Exception: TAR sets it based on whether the stream has a seekable structure,
but the flag actually doesn't matter for TAR since `streaming_only` already
captures the relevant behaviour.

Making it a `ClassVar` would clarify that it is a format-level property:
```python
class ZipReader(BaseArchiveReader):
    members_list_supported = True
```

#### D. Typed compression method enum

`ArchiveMember.compression_method` is an untyped string.  Adding a
`CompressionMethod` enum (or `StrEnum`) with known values like `STORED`,
`DEFLATE`, `LZMA`, `ZSTD`, `BZIP2`, `LZMA2`, `BCJ2`, `PPMD`, etc. and a
fallback `UNKNOWN` would allow callers to make programmatic decisions without
parsing strings.

#### E. Capability introspection

**Problem**: callers must try operations and catch `ValueError` to discover
whether random access is available.

**Proposed addition**:
```python
@property
def supports_random_access(self) -> bool:
    return not self._streaming_only

@property
def supports_member_list(self) -> bool:
    return self._early_members_list_supported or not self._streaming_only
```

Both are derivable from existing state; making them properties prevents
callers from relying on internal flags.

---

## 9. Recommended implementation order

Given the analysis above, the natural sequencing for work is:

1. **ISO reader** — simplest to add (no solid concerns, no external process,
   library or ~400-line native parser).  Completes the format matrix.

2. **RAR native metadata reader** — eliminates the `rarfile` dependency for
   metadata.  `unrar` remains for decompression.  Documented in
   `rar-native-reader-design.md`.

3. **7z native metadata reader** — eliminates py7zr for metadata parsing.
   py7zr remains for decompression.  Documented in
   `sevenzip-native-reader-design.md`.

4. **Solid archive co-iteration refactor** (§8.A) — once both native readers
   are done, the thread+queue pattern in 7z and the stream-reader pattern in
   RAR can be unified under a cleaner `_iter_members_and_streams()` hook.

5. **`streaming_only` / capability refactor** (§8.B, §8.C, §8.E) — cosmetic
   but improves the public API and clarifies the mental model for contributors.

---

## 10. References

| Document | Covers |
|---|---|
| `docs/rar-native-reader-design.md` | RAR3/RAR5 native parser design, extract-hack, solid streaming |
| `docs/sevenzip-native-reader-design.md` | 7z folder model, thread+queue streaming, AES key derivation |
| `docs/zip-stdlib-limitations.md` | zipfile gaps, Extended Timestamp parsing, symlink detection |
| `docs/tar-stdlib-limitations.md` | tarfile gaps, integrity check, compressed-stream backends |
| `docs/iso-pycdlib-analysis.md` | ISO9660/pycdlib multi-namespace confusion, native reader option |
| `src/archivey/internal/base_reader.py` | BaseArchiveReader implementation |
| `src/archivey/archive_reader.py` | ArchiveReader public interface |
