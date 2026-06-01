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
| `members_list_supported` | True (seekable) / False (streaming)⁵ | False | True | True | True |
| `streaming_only` default | False (current impl)⁶ | Configurable¹ | False | False | False |
| Central directory | Yes (EOCD at EOF) | No | Yes (at EOF) | Yes (at EOF) | Yes (VDs at sector 16) |
| Compression granularity | Per-member | Whole-archive² | Per-member + groups (solid) | Per-folder (solid) | None (stored only) |
| Encryption granularity | Per-member (ZipCrypto) | None | Per-member + header | Per-folder | None |
| Solid support | No | Yes (compressed, but see §4) | Optional | Almost always | No |
| Hardlinks | No | Yes (native) | No | No | No |
| Symlinks | Yes (Unix `external_attr`) | Yes (native) | Rarely³ | Yes | Yes (Rock Ridge only) |
| Duplicate filenames | No | Yes | No | Yes | No |
| CRC stored per member | CRC32 | None⁴ | CRC32 | CRC32 | None |
| Encryption detection | Per-member flag | N/A | Per-member + header flag | Per-folder flag | N/A |
| Library used | `zipfile` (stdlib) | `tarfile` (stdlib) | `rarfile` (third-party) | `py7zr` (third-party) | `pycdlib` (third-party, test-only) |

¹ Forced streaming when TAR is inside a non-seekable compressed stream; always
  streaming for piped stdin usage.  With indexed backends the compressed stream
  becomes seekable and streaming mode is unnecessary.

² "Whole-archive" because a compressed TAR is a single compression stream
  wrapping all members; individual members are not compressed independently.

³ Some RAR4 archives store symlinks; not part of the official spec and not
  widely supported.

⁴ TAR format has no checksum for file data; only the header has a simple
  checksum (sum of header bytes mod 256).

⁵ With seekable input, `zipfile` seeks to the EOCD and reads the full central
  directory, giving `members_list_supported=True`.  With a streaming (non-seekable)
  reader that uses local file headers directly, members are discovered
  sequentially just like TAR and `members_list_supported=False`.  Some metadata
  is only in the central directory and not available in streaming mode (see §2.1).

⁶ The current archivey `ZipReader` (backed by stdlib `zipfile`) rejects
  non-seekable input at construction.  This is a library limitation, not a
  format limitation — see §2.1 for what a streaming ZIP reader would look like.

---

## 2. `BaseArchiveReader` flags

### 2.1 `members_list_supported`

If True, the archive has a "directory" structure that allows listing all
members before reading any file data.

| Format | Flag (seekable) | Flag (streaming) | Reason |
|---|---|---|---|
| ZIP | True | False (format supports; stdlib doesn't) | Central dir at EOF for seekable; local headers before data enable streaming |
| TAR | False | False | No central directory; entries discovered sequentially |
| RAR | True | N/A | End-of-archive block at EOF; rarfile reads all `RarInfo` objects at open time |
| 7z | True | N/A | End-header + Streams Info at EOF; py7zr reads all metadata at open time |
| ISO | True | N/A | VD at sector 16; full directory tree walkable before any file data |

**ZIP can be streamed at the format level**: each ZIP entry has a local file
header immediately before its compressed data, identical in concept to a TAR
entry header.  A streaming ZIP reader processes these local headers in order —
filename, compression method, and flags are all present — without ever needing
the central directory.

The **data descriptor** (flag bit 3) complicates sizing for streaming writes:
when a ZIP is written to a pipe the writer doesn't know the CRC or sizes at the
time the local header is written, so it puts them in a trailing data descriptor
record after the compressed data.  For compressed methods (deflate, bzip2,
lzma) this is handled naturally — each decompressor has an end-of-stream
marker so the reader knows when the data ends without needing the size.  For
stored (uncompressed) entries with bit 3 set, the end must be found by scanning
for the data-descriptor signature `PK\x07\x08`, which is ambiguous if that
byte sequence appears in the payload.

What streaming mode **cannot** provide (central-directory-only fields):

| Field | Notes |
|---|---|
| `external_attr` | Unix mode bits (permissions) and symlink detection — not in local header |
| `create_system` | Needed to interpret `external_attr` — not in local header |
| Per-file comment | Only in central directory |

Everything else — filename (with UTF-8 detection via bit 11), compression
method, DOS timestamp, and the Extended Timestamp extra field (0x5455) — is
present in the local header and available in streaming mode.

The current archivey `ZipReader` (stdlib `zipfile`) cannot stream: it seeks
to EOCD at open time and rejects non-seekable input.  This is a **stdlib
limitation**, not a format one.  Implementing a streaming ZIP reader would
require parsing local headers directly.

For RAR and 7z, streaming is not possible at all: the end-of-file index is the
only place member metadata lives.

The base class handles the TAR (and streaming ZIP) case via the
`_early_members_list_supported` path: when both `streaming_only=True` and
`members_list_supported=False`, `get_members_if_available()` returns `None`.

### 2.2 `streaming_only`

This flag has two sources of truth that are currently conflated:

- **Format capability**: Can the format support random access at all?  ZIP,
  RAR, 7z, ISO always can (given a seekable source).  Plain TAR can if the
  source is seekable.  Compressed TAR depends on the decompressor backend: the
  stdlib backends (gzip, bz2, lzma) expose forward-seekable-only streams;
  rapidgzip and indexed_bzip2 expose fully seekable streams with index-based
  random access (see §4.1).
- **User preference**: The caller may pass `streaming_only=True` to
  `open_archive()` to force sequential mode even when the format supports
  random access, for efficiency.

Currently both are encoded in the single `_streaming_only` boolean on
`BaseArchiveReader`.  `TarReader` sets it based on whether the decompressed
stream is seekable:

```python
if not streaming_only and not is_seekable(self._fileobj):
    raise ArchiveStreamNotSeekableError(...)
open_mode = "r|" if streaming_only else "r:"
```

When an indexed backend (rapidgzip, indexed_bzip2, python-xz) is configured,
the decompressed stream reports `seekable() == True`, so `TarReader` opens in
`"r:"` (random-access) mode automatically.  The compressed TAR behaves like a
non-solid archive for practical purposes — individual member opens are
efficient — even though the underlying data is still in one compression stream.

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
| TAR (stdlib backend) | O(N) per backward seek | `tarfile.extractfile(info)` re-decompresses from position 0 on backward seek |
| TAR (rapidgzip) | O(checkpoint) ≈ O(1–4 MB) | rapidgzip checkpoint index limits rewind distance |
| TAR (indexed_bzip2/python-xz) | O(1) per block | True index-based; no rewind needed after initial scan |
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

### 4.1 Compressed TAR — default backends (always solid)

The entire file is one decompressed stream.  `TarReader` passes this stream
to `tarfile.open(mode="r:")` (random-access) or `"r|"` (streaming only).
Random access requires backward seeks; with stdlib backends these restarts
are done by rewinding to position 0 and re-decompressing forward.

The `DecompressorStream` base class in `compressed_streams.py` implements
seek-by-rewind for Lzip, Zlib, Brotli, and uncompresspy backends:
- **Forward seek**: reads and discards bytes.
- **Backward seek**: calls `_rewind()`, then reads forward.
- **Cost**: O(target_offset) per backward seek.

Strategy: sequential iteration is O(N); random member access is O(N·M) for
M random opens on an N-byte decompressed stream.

### 4.1a Compressed TAR — indexed backends (quasi-random access)

Three optional backends rewrite the cost model:

| Backend | Config flag | Compression | Mechanism | Seek cost after index |
|---|---|---|---|---|
| `rapidgzip` | `use_rapidgzip` | gzip | Builds gzip-block checkpoint index; each block is a restart point | O(block_size) ≈ O(1–4 MB) per backward seek |
| `indexed_bzip2` | `use_indexed_bzip2` | bzip2 | Finds all bzip2 block boundaries (each block ~900 KB, independently decompressible) | O(1) once index is built |
| `python-xz` | `use_python_xz` | xz | Uses XZ's native block index at end of file | O(1) per block seek |
| `zstandard` | `use_zstandard` | zstd | `ZstandardReopenOnBackwardsSeekIO`: reopens from position 0 on backward seek | O(target_offset) — same as stdlib |

With rapidgzip and indexed_bzip2, the decompressed stream is fully seekable;
`is_seekable()` returns True; `TarReader` opens in `"r:"` (random-access)
mode and `streaming_only` is False.  The archive is no longer effectively
solid:

- **members_list_supported** remains False (no central directory).
- **streaming_only** becomes False — individual members can be opened directly.
- **is_solid** in `ArchiveInfo` is still True (format has no per-member
  compression), but practical random-access cost is sub-linear.

The index is built lazily during the first sequential scan.  After the first
`get_members()` call, any subsequent `open(member)` call costs only a seek to
the nearest checkpoint, not a full re-decompression.

Zstandard with the reopen wrapper does not benefit from this: every backward
seek still restarts from byte 0.  A streaming-only `.tar.zst` is effectively
as solid as one with any stdlib backend.

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

| Strategy | Cost model | streaming_only | `open()` works |
|---|---|---|---|
| TAR + stdlib gzip/bz2/lzma (rewind) | O(N) per backward seek | True (non-seekable) or False | Yes (expensive) |
| TAR + rapidgzip | O(checkpoint_distance) ≈ O(1–4 MB) per seek | False | Yes |
| TAR + indexed_bzip2 / python-xz | O(1) per block after initial scan | False | Yes |
| TAR + zstandard (reopen wrapper) | O(target_offset) per backward seek | False (if file seekable) | Yes (expensive) |
| RAR non-solid | O(1) per member | False | Yes |
| RAR solid (default) | O(N²) total over all members | False | Yes (expensive) |
| RAR solid (`use_rar_stream`) | O(N) total, sequential only | True (effectively) | No |
| 7z thread+queue | O(folder_size) per folder | False | Via queue drain |

Key observations:

- **indexed_bzip2 and python-xz** effectively break compressed TAR out of the
  "solid" cost class after the first scan.  A `.tar.bz2` with indexed_bzip2
  behaves comparably to a non-solid format for random member access.
- **rapidgzip** is nearly as good — checkpoint granularity is a few MB, not
  the full file.  Parallel decompression also makes the initial scan faster.
- **zstandard** with the reopen wrapper provides forward-only efficiency; it
  is not better than the stdlib backends for random access.
- The 7z thread+queue approach is the most sophisticated for truly solid
  archives where sequential extraction is unavoidable.  The RAR stream approach
  is simpler but only works when iterating all members in order.

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

Currently TarReader sets `streaming_only=True` when the decompressed stream is
non-seekable.  The outcome depends on which backend is configured:

- Stdlib gzip/bz2/lzma on a piped input → non-seekable → `streaming_only=True`
- Stdlib gzip/bz2/lzma on a file → seekable (via rewind) → `streaming_only=False`
- rapidgzip or indexed_bzip2 → always seekable → `streaming_only=False`

The current archivey `ZipReader` (stdlib `zipfile`) cannot use streaming mode;
passing `streaming_only=True` to `open_archive()` for a ZIP raises an error at
construction.  The ZIP format itself does support streaming via local file
headers (see §2.1); a future streaming `ZipReader` would expose
`streaming_only=True` just like `TarReader`, at the cost of losing
`external_attr` (permissions, symlinks) and per-file comments.

Proposal: split into `format_supports_random_access: bool` (class attribute)
and `streaming_only: bool` (runtime flag that can be True only when the format
and backend both support sequential-only access).

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
