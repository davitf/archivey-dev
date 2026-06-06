# 7-Zip Native Reader — Architecture Design Notes

This document captures the research needed to replace `py7zr`-based 7-zip handling
with a built-in implementation that parses archive metadata itself but still
delegates decompression to py7zr (or a future alternative like the `lzma` stdlib
module for LZMA2-only archives, or an external `7z` tool for other codecs).

The structure mirrors [`rar-native-reader-design.md`](rar-native-reader-design.md).

---

## 1. What the BaseArchiveReader contract requires

See the RAR design doc §1 for the full description.  The relevant flags for 7z:

- `members_list_supported=True` — 7z has a central directory at the end of the
  file (the "end header"), so a complete member list is always available before
  any decompression.
- `streaming_only=False` — random access is supported.
- Non-seekable input streams are rejected at construction time
  (`ArchiveStreamNotSeekableError`), because `SevenZipFile._real_get_contents()`
  seeks back to the start of the payload after reading the header.

---

## 2. What the current py7zr-based reader does

`src/archivey/formats/sevenzip_reader.py` wraps `py7zr` (≥ 1.0.0).  Unlike
RAR, py7zr is a **pure-Python decompressor** — it never calls an external tool.

### 2.1 Metadata reading

`SevenZipFile.__init__` calls `_real_get_contents(password)` which:

1. Reads the 32-byte **signature header** at the start of the file to get the
   offset and size of the **end header** block.
2. Seeks to the end header, reads it into a `BytesIO` buffer, and verifies its
   CRC32.
3. Passes the buffer to `Header.retrieve(fp, buffer, afterheader, password)`,
   which parses `ENCODED_HEADER` (LZMA2-compressed header) or plain `HEADER`
   blocks, potentially decrypting if header encryption is in use.
4. Iterates over the parsed `files_info.files` list, associating each file
   record with a `Folder` object and computing per-file sizes and CRCs from
   `SubStreamsInfo`.
5. Builds `self.files` as an `ArchiveFileList` of `ArchiveFile` objects.

`sevenzip_reader.py:iter_members_for_registration()` (line 414) iterates
`self._archive.files` and converts each `ArchiveFile` to an `ArchiveMember`.

Fields extracted from `ArchiveFile`:

| `ArchiveFile` field | Usage | Notes |
|---|---|---|
| `filename` | member path | UTF-16LE decoded by py7zr |
| `is_directory` | `MemberType.DIR` | trailing `/` added if missing |
| `is_symlink` | `MemberType.SYMLINK` | derived from POSIX mode bits |
| `is_junction`, `is_socket` | `MemberType.OTHER` | Windows junction / socket |
| `uncompressed` | `file_size` | typed as `list[int]` in py7zr docs but actually `int` |
| `compressed` | `compress_size` | **folder's** total packed size, not per-file |
| `crc32` | `crc32` | per-file CRC32; empty files get 0 explicitly |
| `lastwritetime` | `mtime_with_tz` | Windows FILETIME; `py7zr.helpers.filetime_to_dt()` converts it |
| `posix_mode` | `mode` | POSIX permissions from Unix extra field |
| `folder` | used for `encrypted` detection | internal `Folder` object reference |

Fields **not** exposed by py7zr (set to `None` in `ArchiveMember`):

- `compression_method` — py7zr doesn't expose the codec per-file.
- `comment` — 7z format supports comments but py7zr doesn't expose them.
- `create_system` — not available.
- Per-file `compressed` size in solid archives (all files in the same folder
  share the folder's total `compressed` value).

### 2.2 Decompression: how py7zr works

py7zr implements all decompression in Python:

| Codec | py7zr implementation | Method ID |
|---|---|---|
| LZMA / LZMA1 | `lzma` stdlib | `0x030101` |
| LZMA2 | `lzma` stdlib | `0x21` |
| Delta filter | Python | `0x03` |
| BCJ (x86/ARM/…) | Python | `0x04`–`0x09`, `0x03030103` etc. |
| Deflate | `zlib` stdlib | `0x040108` |
| Deflate64 | `inflate64` optional | `0x040109` |
| BZip2 | `bz2` stdlib | `0x040202` |
| Zstd | `zstandard` optional | `0x04f71101` |
| Brotli | `brotli` optional | `0x04f71102` |
| AES-256/SHA-256 | `Cryptodome.Cipher.AES` | `0x06f10701` |
| Copy (store) | identity | `0x00` |
| PPMd | (limited) | `0x030401` |

The `Folder.get_decompressor(packsize)` method builds a `SevenZipDecompressor`
chain for the folder's coders list — filters are applied in pipeline order
(e.g. LZMA2 → AES means: decrypt first, then decompress; in the coder list
order that is AES → LZMA2 which is reversed for decompression).

`extract(targets=[...], factory=factory)` iterates `self.files`, skips
non-targeted files (registering a `NullIO` on the worker), then extracts
targeted ones in a single forward pass through the archive, decompressing
each folder once regardless of how many files in it are targeted.

Key implication: **there is no O(N²) problem for solid archives** as long as
all members from a given folder are extracted in a single `extract()` call.
This is what `_extract_members_iterator()` does — it collects all pending files
and passes them all to one `extract()` invocation.

### 2.3 The solid archive concept in 7z

Unlike RAR (where "solid" is an archive-wide flag), 7z uses a **folder-based**
model:

- A **Folder** is a compression unit: one packed bitstream that decompresses to
  the concatenation of one or more files.
- If a folder holds exactly 1 file, it is non-solid for that file.
- If a folder holds N > 1 files, those N files form a solid group.
- `SubStreamsInfo.num_unpackstreams_folders[i]` = number of files in folder i.
- `SevenZipFile._is_solid()` (line 835) returns `True` if any
  `num_unpackstreams_folders` value is > 1.

`archiveinfo().solid` reports this same flag.

The guard in `_is_solid()` (lines 795–800):

```python
if self._archive.header.main_streams is None:
    # py7zr bug: archiveinfo() raises if archive is empty / no main streams
    return False
```

### 2.4 The thread + queue streaming design

py7zr's extraction model is push-based: you pass a `WriterFactory` and py7zr
calls `factory.create(filename)` → writes chunks → calls `writer.close()`.
There is no way to pull one file at a time from the outside.

To expose the archivey `iter_members_with_streams()` pull-based iterator,
the reader spawns a background thread:

```
Main thread                      Background thread
──────────────────               ─────────────────────────────
iter_members_with_streams()      extractor()
  _extract_members_iterator()
    Thread(target=extractor).start()
                                   archive.reset()
                                   archive.extract(targets, factory=factory)
                                     factory.create("a.txt") → StreamingFile
                                       StreamingFile.write(chunk)
    q.get() ← ────────────────────────── q.put(("a.txt", reader))
    yield member, reader
    caller reads reader.read(n)
      ← reader._data_queue.get() ← StreamingFile._data_queue.put(chunk)
                                     StreamingFile.close()
                                       _data_queue.put(None)  # EOF
    stream.close()
                                   factory.finish()
                                     q.put(None)  # no more files
    q.get() → None → break
```

Each `StreamingFile` has its own internal `_data_queue` (max 64 chunks) for
backpressure.  The shared `q` carries `(filename, reader)` pairs as each new
file starts (when py7zr first calls `writer.write()`).

**Edge cases in the streaming path**:

- **Empty files** (line 683): py7zr never calls `write()` for zero-size files,
  so they never appear in the queue.  The first pass yields them with an empty
  `BytesIO` immediately.
- **Directories** (line 678): yielded from the first pass with `stream=None`.
- **Unresolved links without target** (lines 665–668): collected into
  `pending_links_by_id`, extracted alongside files in the second pass, their
  content decoded as UTF-8 to set `link_target`.
- **Exceptions in background thread** (lines 585–590): caught and put into the
  queue as `Exception` objects; the main thread detects these and re-raises
  (after joining the thread).

### 2.5 Password handling — the `_temporary_password` hack

py7zr stores the AES password on each `Folder` object (`folder.password`).
The password is set when the archive is opened:

```python
# in _real_get_contents():
for folder in folders:
    folder.password = password   # string or None
```

There is **no parameter on `extract()`** to pass a different password.  To
support per-call passwords, the reader temporarily mutates the folder objects:

```python
@contextmanager
def _temporary_password(self, pwd):
    SevenZipReader._password_lock.acquire()   # class-level lock
    folders = archive.header.main_streams.unpackinfo.folders
    previous = [f.password for f in folders]
    for f in folders:
        f.password = bytes_to_str(pwd)
    yield
    for f, p in zip(folders, previous):
        f.password = p
    SevenZipReader._password_lock.release()
```

The **class-level `_password_lock`** (line 286) prevents two threads from
concurrently mutating the same archive's folder passwords.  A single `SevenZipFile`
object is shared across all calls to the reader; concurrent `open()` or
`iter_members_with_streams()` calls with different passwords would race without
this lock.

**Known limitation** (tests are skipped for it): `iter_members_with_streams(pwd=...)`
does not work correctly when different members need different passwords
(e.g. `encryption_several_passwords__7zcmd.7z`).  The lock sets the password
globally for all folders, so files in folders that should have a different
password (or no password) will fail or produce garbage.  The password must be
set at `open_archive()` construction time for reliable behaviour.

### 2.6 Duplicate filename handling

py7zr renames duplicate filenames during extraction by appending `_<n-1>` to
later occurrences.  The `extract_filename` mapping in `raw_info.extra` mirrors
this logic (lines 427–433) so that `StreamingFactory.create(filename)` returns
the correct member when py7zr passes the sanitised name.

### 2.7 `reset()` requirement

`archive.reset()` (called before every `extract()`) recreates the internal
`Worker` object.  Without it, a second extraction call on the same
`SevenZipFile` fails because the worker's state has been consumed.

### 2.8 Archive info and `password_protected`

`SevenZipFile.password_protected` is set to:
- `True` if a password was passed to the constructor **or** if any folder's
  coders include the AES method (detected after parsing, lines 555–558).
- This is what `get_archive_info()` exposes as `extra["is_encrypted"]`.

There is no explicit "header encryption" flag exposed in py7zr's public API;
it is implicit — if `header_encryption=True` was set during writing, the
`ENCODED_HEADER` block is wrapped in AES, and `_real_get_contents` decrypts it
using the provided password before parsing.

---

## 3. 7z format essentials

### 3.1 Signature

```
37 7a bc af 27 1c     (6 bytes)  —  MAGIC_7Z = b"7z\xbc\xaf\x27\x1c"
```

Unlike ZIP or RAR, there is **no SFX support** in the signature — 7z files
always start at byte 0.

### 3.2 Signature header (first 32 bytes)

```
Signature[6]          = 37 7a bc af 27 1c
MajorVersion[1]       = 00
MinorVersion[1]       = 04
StartHeaderCRC[4]     — CRC32 of the next 20 bytes
NextHeaderOffset[8]   — offset of end header from byte 32 (after sig header)
NextHeaderSize[8]     — size of end header
NextHeaderCRC[4]      — CRC32 of end header
```

The end header immediately follows the payload data:
`end_header_pos = 32 + NextHeaderOffset`.

After parsing the signature header, py7zr seeks to `afterheader +
sig_header.nextheaderofs` to read the header block.

### 3.3 End header / archive header

The end header is either:

- A plain `HEADER` block (property tag `0x01`), or
- An `ENCODED_HEADER` block (property tag `0x17`) — the actual header is
  stored compressed (LZMA2 by default) as another packed stream; the
  `ENCODED_HEADER` contains a `StreamsInfo` describing how to decompress it.
- If **header encryption** is active, the `ENCODED_HEADER`'s packed stream is
  encrypted with AES-256.

A plain `HEADER` block contains:

```
HEADER (0x01)
  [ARCHIVE_PROPERTIES (0x02)]          — optional
  [ADDITIONAL_STREAMS_INFO (0x03)]     — optional
  [MAIN_STREAMS_INFO (0x04)]
    PACK_INFO (0x06)
      PackPos (uint64)
      NumPackStreams (uint64)
      SIZE (0x09): N × uint64 pack sizes
      [CRC (0x0a): optional folder CRCs]
    UNPACK_INFO (0x07)
      FOLDER (0x0b)
        NumFolders (uint64)
        [External flag]
        Folder[NumFolders]:
          NumCoders (uint64)
          Coder[NumCoders]:
            CodecIdSize (1 byte)
            CodecId[CodecIdSize]
            [NumInStreams/NumOutStreams if complex coder]
            [PropertiesSize + Properties if has properties]
          [BindPairsInfo]
          [PackedStreamsForCoder: index of input pack stream]
          UnpackSizes[NumCoders]
          [CRC]
      CODERS_UNPACK_SIZE (0x0c): per-coder unpack sizes
      [CRC (0x0a)]
    SUBSTREAMS_INFO (0x08)
      [NUM_UNPACK_STREAM (0x0d): files-per-folder counts]
      [SIZE (0x09): per-stream unpack sizes, omitted if 1 per folder]
      [CRC (0x0a): per-file CRCs]
  FILES_INFO (0x05)
    NumFiles (uint64)
    Property records until END (0x00):
      0x0e  EMPTY_STREAM bitmask   — which files have no data stream
      0x0f  EMPTY_FILE bitmask     — which empty-stream files are real files vs dirs
      0x10  ANTI bitmask           — anti-items (delete on extraction)
      0x11  NAME: UTF-16LE names with NUL separator
      0x12  CREATION_TIME: optional Windows FILETIME values
      0x13  LAST_ACCESS_TIME: optional
      0x14  LAST_WRITE_TIME: optional
      0x15  ATTRIBUTES: optional Windows file attributes
      0x18  START_POS: optional per-file pack start positions
END (0x00)
```

All integers are little-endian.  Sizes use a variable-length encoding
(`read_uint64`): first byte's high bits indicate length; if `< 0x80` it's 1
byte, otherwise up to 8 bytes.

### 3.4 Folder and solid detection

A `Folder` represents one compressed stream.  Its `coders` list describes the
pipeline: each coder has a method ID (byte sequence) and optional properties.

The **solid flag** for a folder is determined during `_real_get_contents`:

```python
folder.solid = subinfo.num_unpackstreams_folders[folder_idx] > 1
```

A folder is solid if it decompresses to more than one file.  The archive is
solid overall if any folder is solid.

**Implication for a native reader**: detecting `is_solid` only requires
reading `SubStreamsInfo.num_unpackstreams_folders`, not decompressing anything.

### 3.5 AES-256 encryption

Method ID: `0x06f10701` (`CRYPT_AES256_SHA256`).

Properties (stored as coder properties):

```
FirstByte[1]:
  bits 5..0 = NumCyclesPower (key derivation iteration count = 1 << NumCyclesPower,
              or 0x3f for max)
  bit  6    = IV is present
  bit  7    = Salt is present
SaltSize[1] (if Salt present): upper nibble = salt byte count
IVSize[1] (if IV present)
Salt[SaltSize bytes]
IV[IVSize bytes], zero-padded to 16 bytes
```

Key derivation (`calculate_key` in py7zr):

```python
password_bytes = password.encode("utf-16LE")
iterations = 1 << NumCyclesPower   # (special case: if NumCyclesPower == 0x3f, use 0x7fffffff)
sha = hashlib.sha256()
for round in range(iterations):
    sha.update(password_bytes)
    sha.update(salt)
    sha.update(struct.pack("<Q", round))   # round counter as little-endian uint64
key = sha.digest()  # 32 bytes
```

Then AES-256-CBC decrypt with the derived key and the IV from properties.

Note: **header encryption** uses the same AES-256 scheme but applied to the
`ENCODED_HEADER`'s packed stream.  There is no separate "check value" for
pre-flight password verification (unlike RAR5).  A wrong password simply
produces garbage that fails CRC validation.

---

## 4. What a native reader needs to implement

### 4.1 Scope

The goal is to replace py7zr for **metadata parsing** while still using py7zr
(or another backend) for decompression.  A native parser would:

1. Detect the 7z signature and read the 32-byte signature header.
2. Seek to the end header, read and verify its CRC32.
3. If `ENCODED_HEADER`: read the inner `StreamsInfo` to locate the compressed
   header payload, then decompress it (using `lzma.decompress` for LZMA2, or
   delegating to py7zr/external tool), verify CRC.
4. If header-encrypted: derive the AES key and decrypt before step 3.
5. Parse `HEADER` → `FILES_INFO` to build the member list.
6. Parse `MAIN_STREAMS_INFO` → `UNPACK_INFO` → folders + coders, and
   `SUBSTREAMS_INFO` → per-file sizes and CRCs.
7. Associate each file with its folder and compute per-file metadata.
8. Detect `is_solid`, `is_encrypted`, folder-level encryption.

### 4.2 Native `SevenZipMemberInfo` dataclass

The `raw_info` payload currently stored is py7zr's `ArchiveFile` object.
The only field used after registration is `ArchiveFile.filename` for building
the `extract_targets` list (line 569).  An equivalent native dataclass:

```python
@dataclass
class SevenZipMemberInfo:
    filename: str              # original name as stored in archive (no trailing /)
    is_directory: bool
    is_symlink: bool
    is_junction: bool
    is_socket: bool
    is_emptystream: bool       # has no data stream (dirs, some special files)
    uncompressed: int          # file_size
    compressed: int | None     # total folder packed size (not per-file in solid archives)
    crc32: int | None          # per-file CRC32
    lastwritetime: int | None  # Windows FILETIME (100ns since 1601)
    posix_mode: int | None     # Unix mode bits from extra field
    folder_index: int | None   # which folder holds this file; None for empty-stream files
    file_in_folder: int        # 0-based index within the folder
    # for extract_filename de-dup tracking (mirrors py7zr naming):
    extract_filename: str      # may differ from filename if duplicate
```

### 4.3 Decompression strategy

Nothing changes in the decompression path — we still use py7zr's `extract()`
with the `StreamingFactory`/`WriterFactory` pattern.  The only change is that
we no longer need py7zr for metadata (the `iter_members_for_registration` phase),
so we build our own `SevenZipMemberInfo` objects and construct a minimal
py7zr-compatible structure for the extraction call.

**Alternative (future)**: if py7zr is removed entirely, decompression would need
to fall back to an external `7z` tool (like `unrar` for RAR).  The streaming
design would then mirror `RarStreamReader`: spawn `7z e -so`, share stdout.

### 4.4 Solid-archive extraction: current strategy is already O(N)

Unlike RAR (where each `rarfile.open()` re-decompresses from the start of the
solid stream), py7zr's `extract(targets=[f1, f2, ..., fN], factory=factory)`
decompresses each folder **once** regardless of how many of its files are
targeted.  The archivey reader collects all pending members before calling
`extract()`, which means:

- Best case (all members extracted at once): O(N) total decompression work.
- Worst case (single file from a large solid folder): O(folder_files)
  decompression work per call (same as any decompressor).

The `use_rar_stream` optimization has no 7z equivalent — the same result is
achieved by the existing batch collection in `iter_members_with_streams()`.

For `_open_member()` (random access), the current implementation calls
`iter_members_with_streams()` with a single-member list (lines 528–539),
which still goes through the full thread+queue machinery.  This means a single
`open()` call is relatively expensive for members in large solid folders.

### 4.5 Password handling: what would improve

The `_temporary_password` pattern is a **workaround** for py7zr's lack of a
per-call password parameter.  If the decompression backend changes:

- A native Python decompressor would accept a password at decompression time
  (pass to `AESDecompressor(aes_properties, password)`).
- An external `7z` tool would accept `-p<password>` on the command line.

Either approach eliminates the need for the global class-level lock.

### 4.6 Header decryption in a native parser

For header-encrypted archives, the parser needs to:

1. Read the `ENCODED_HEADER` block.
2. Find the `PACK_INFO` → pack stream position and size.
3. Seek to the pack stream, read the encrypted bytes.
4. Derive the AES-256 key from the password (same algorithm as §3.5).
5. Decrypt (AES-256-CBC), strip padding.
6. Decompress (usually LZMA2).
7. Parse the resulting plain `HEADER` bytes as normal.

This requires either:
- `Cryptodome.Cipher.AES` (already listed as optional dependency for RAR), or
- `cryptography` (already used for RAR encrypted headers).

Without the password, header-encrypted archives report no members (same
behaviour as today).

### 4.7 Metadata fields gap analysis

| Field | Source | Gap |
|---|---|---|
| `filename` | `FILES_INFO.NAME` property | None |
| `file_size` | `SUBSTREAMS_INFO.SIZE` or folder unpack size | None |
| `compress_size` | `PACK_INFO.packsizes` (folder total) | Not per-file in solid archives; same as now |
| `crc32` | `SUBSTREAMS_INFO.CRC` | None |
| `mtime` | `FILES_INFO.LAST_WRITE_TIME` | None |
| `atime`, `ctime` | `FILES_INFO.LAST_ACCESS_TIME / CREATION_TIME` | Not currently exposed by archivey |
| `mode` (POSIX) | Extra field in attributes (POSIX extension) | Currently from `posix_mode` |
| `is_directory` | `EMPTY_STREAM` + `EMPTY_FILE` bitmasks | None |
| `is_symlink` | `st_fmt` from POSIX mode bits | Same detection logic as py7zr |
| `encrypted` | folder's coders include `0x06f10701` | Same as `SupportedMethods.needs_password()` |
| `compression_method` | folder coder IDs | **New**: native parser can expose this |
| `is_solid` | `num_unpackstreams_folders` | None |
| `comment` | `FILES_INFO.COMMENT` (0x16) property | py7zr ignores this; native reader could expose it |

The most significant improvement a native parser offers is exposing
**`compression_method`** per member (currently `None`) and archive
**comments** (currently discarded by py7zr).

---

## 5. Things that can be dropped / simplified

| Current complexity | Why it exists | Native reader stance |
|---|---|---|
| `_temporary_password` context manager + class lock | py7zr has no per-call password | Keep until decompression backend also changes |
| `reset()` before every `extract()` | py7zr's Worker is stateful | Keep if still using py7zr extraction |
| `_build_extract_filename_to_member_map` with `get_sanitized_output_path` | py7zr renames duplicates | Keep if still using py7zr extraction |
| `_is_member_encrypted` via `SupportedMethods.needs_password` | py7zr private API | Replace with direct coder-list check in native parser |
| `archiveinfo()` crash guard (empty archive) | py7zr bug | Can implement `is_solid` directly from `num_unpackstreams_folders` |
| `py7zr.helpers.filetime_to_dt` | py7zr utility | Trivial to inline: `datetime(1601,1,1) + timedelta(microseconds=ft//10)` |
| Exception catch-all in extractor thread | py7zr exceptions not wrapped | Keep regardless of backend |

---

## 6. Files affected by the change

| File | Change |
|---|---|
| `src/archivey/formats/sevenzip_reader.py` | Replace `py7zr.files` / `ArchiveFile` usage in `iter_members_for_registration` with native parser; keep streaming machinery |
| New: `src/archivey/formats/sevenzip_parser.py` | `SevenZipParser`, `SevenZipMemberInfo`, raw header reading |
| `src/archivey/internal/dependency_checker.py` | Mark `py7zr` as optional for metadata, still required for decompression |
| Tests | Verify all existing 7z test archives still pass; add parser unit tests |

If the decompression backend is also replaced (phase 2):

| File | Change |
|---|---|
| `src/archivey/formats/sevenzip_reader.py` | Replace `_extract_members_iterator` thread+queue with direct decompressor calls or external-tool subprocess |
| `pyproject.toml` | Remove `py7zr>=1.0.0` from `optional` dependencies |

---

## 7. Open questions / risks

1. **LZMA1 vs LZMA2 detection** — most modern 7z archives use LZMA2, but older
   ones use LZMA1.  Both are supported by Python's `lzma` module.  The native
   parser needs to dispatch correctly based on the coder ID.

2. **BCJ/Delta filter chain** — many 7z archives prepend a BCJ (Branch
   Conversion Jump) filter or Delta filter before LZMA2.  These must be
   inverted during decompression.  py7zr implements all of these in Python; a
   native reader must either port them or continue to use py7zr for
   decompression.

3. **Multi-volume archives** — py7zr uses `multivolumefile.MultiVolume` for
   split archives.  This is not currently supported in archivey (same as RAR);
   a native parser should raise `ArchiveError` cleanly at the volume-detection
   step rather than producing garbage output.

4. **Anti-files** — 7z supports "anti-items" (files that should be deleted
   during extraction from a delta-update archive).  py7zr handles them silently.
   A native reader should either support them or warn.

5. **Empty archive CRC edge case** — `_real_get_contents` skips `FILES_INFO` if
   absent (line 478: `if getattr(self.header, "files_info", None) is None`).
   The native parser must handle the same case.

6. **Thread safety of the extractor thread** — py7zr (1.0+) itself is not
   thread-safe, which is why the `_temporary_password` lock exists at the
   reader level.  Any new decompression backend must document its own
   thread-safety model.

7. **`compressed` size inaccuracy for solid archives** — the current reader
   stores the **folder's** total packed size as `compress_size` for every file
   in a solid folder.  A native parser has the same structural limitation (per-
   file compressed sizes are undefined in solid streams) but could at least
   expose the folder-level size clearly via `extra`.

8. **Compression method name mapping** — the coder ID `0x030101` = LZMA,
   `0x21` = LZMA2, etc.  A native reader can produce human-readable method
   names (e.g. `"LZMA2 + BCJ"`) by walking the folder's coder list.  Currently
   archivey returns `None` for 7z.

---

## 8. Useful references

- 7z format specification: included in the 7-Zip source tree as
  `DOC/7zFormat.txt` (also at https://py7zr.readthedocs.io/en/latest/archive_format.html).
- py7zr source (1.1.0): `/tmp/py7zr_dl/py7zr_src/py7zr/` (extracted from
  wheel during this research session).
  - `archiveinfo.py` — header parsing: `Header`, `SignatureHeader`, `Folder`,
    `UnpackInfo`, `PackInfo`, `SubStreamsInfo`, `FilesInfo`.
  - `compressor.py` — `AESDecompressor`, `SevenZipDecompressor`, `Folder.get_decompressor`.
  - `py7zr.py` — `SevenZipFile._real_get_contents`, `_extract`, `_is_solid`,
    `archiveinfo()`, `ArchiveFile`.
  - `properties.py` — `MAGIC_7Z`, `PROPERTY.*` constants, `CompressionMethod.*`.
- Current implementation: `src/archivey/formats/sevenzip_reader.py`.
- Base class: `src/archivey/internal/base_reader.py`.
- RAR reader (for comparison, especially `RarStreamReader` solid-stream
  pattern): `src/archivey/formats/rar_reader.py`.
