# 7-Zip Native Reader — Architecture Design Notes

This document captures the research needed to replace `py7zr`-based 7-zip handling
with a built-in implementation that parses archive metadata **and** drives
decompression itself, removing the py7zr dependency entirely. Decompression reuses
codecs archivey already has (stdlib `lzma`/`bz2`/`zlib`, plus the existing
`zstandard`/`brotli`/crypto optionals) and adds two small optional packages
(`pyppmd`, `inflate64`) for the codecs stdlib lacks. The key enabler: liblzma (via
the stdlib `lzma` module) already implements the whole BCJ branch-filter family and
the Delta filter, so the 7z "preprocessors" need no bespoke code for the common
case. See §4.3 for the codec landscape.

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

> **This entire mechanism is removed by native decompression (§4.3).** The thread,
> the two queues, the backpressure logic, and the `StreamingFile`/`WriterFactory`
> machinery exist *only* because py7zr is push-based. Driving decompression
> ourselves makes the iterator naturally pull-based (the tar/zip model), so none of
> this survives. It is documented here as the thing being deleted.

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

> **Also removed by native decompression (§4.3, §4.5).** The `_temporary_password`
> context manager and the class-level `_password_lock` are workarounds for py7zr
> having no per-call password parameter. A native `AESDecompressor` takes the
> password (or derived key) as a constructor argument, so the per-folder mutation
> and the global lock both disappear — and per-member passwords (today a skipped
> test) become expressible because each folder's decryptor is built independently.

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

The goal is to replace py7zr **entirely** — for both metadata parsing and
decompression — so 7z support depends only on the stdlib plus codec packages
archivey already manages. The native reader:

**Header parsing** (same as the metadata-only plan):

1. Detect the 7z signature and read the 32-byte signature header.
2. Seek to the end header, read and verify its CRC32.
3. If `ENCODED_HEADER`: read the inner `StreamsInfo` to locate the compressed
   header payload, then decompress it (via the same native folder pipeline used for
   member data — typically `lzma` FORMAT_RAW LZMA2), verify CRC.
4. If header-encrypted: derive the AES key and decrypt before step 3.
5. Parse `HEADER` → `FILES_INFO` to build the member list.
6. Parse `MAIN_STREAMS_INFO` → `PACK_INFO` (pack-stream offsets/sizes),
   `UNPACK_INFO` (folders + coders + bind pairs), and `SUBSTREAMS_INFO` (per-file
   sizes and CRCs).
7. Associate each file with its folder and compute per-file metadata.
8. Detect `is_solid`, `is_encrypted`, folder-level encryption.

**Decompression** (new — replaces py7zr's `extract()` + thread/queue):

9. For a target member, locate its folder's packed byte range from `PACK_INFO`,
   wrap it as a bounded reader over the archive file.
10. Build a decompressor pipeline from the folder's coder chain (§4.3) and expose it
    as a pull-based `BinaryIO` via the existing `DecompressorStream` wrapper.
11. For solid folders, decompress the folder once and slice out each substream by
    its unpack size, yielding members in order — naturally O(N), no threads.

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

Decompression is **native and pull-based**, replacing py7zr's push model. A 7z
folder is one packed bitstream produced by a short *coder chain* (a filter pipeline
ending in an entropy coder). We read the folder's packed bytes from the archive,
run them back through the inverse chain, and slice the result into members.

**Codec landscape — what we already have vs. what we add.** The central finding is
that almost every coder 7z emits is already available to archivey, and the BCJ/Delta
"preprocessors" are not missing at all — liblzma implements them:

| 7z coder | Method ID | Our route | New dep? |
|---|---|---|---|
| Copy (store) | `0x00` | identity passthrough | — |
| LZMA2 | `0x21` | `lzma` FORMAT_RAW | stdlib |
| LZMA1 | `0x030101` | `lzma` FORMAT_RAW | stdlib |
| Delta | `0x03` | `lzma.FILTER_DELTA` in the raw chain | stdlib |
| BCJ x86 | `0x04` / `0x03030103` | `lzma.FILTER_X86` | stdlib |
| BCJ ARM / ARMT / PPC / SPARC / IA64 | `0x05`–`0x09` … | `lzma.FILTER_ARM`/`ARMTHUMB`/`POWERPC`/`SPARC`/`IA64` | stdlib |
| Deflate | `0x040108` | `zlib.decompressobj(-15)` (raw) | stdlib |
| BZip2 | `0x040202` | `bz2.BZ2Decompressor` | stdlib |
| Zstd | `0x04f71101` | `zstandard` | existing optional |
| Brotli | `0x04f71102` | `brotli` | existing optional |
| AES-256 / SHA-256 | `0x06f10701` | `cryptography` / `pycryptodome` | existing optional |
| PPMd (var.H) | `0x030401` | `pyppmd.Ppmd7Decoder` | **new optional** |
| Deflate64 | `0x040109` | `inflate64.Inflater` | **new optional** |
| BCJ2 | `0x0303011B` | — (detect and raise) | — |

Verified facts behind this table:

- Python's stdlib `lzma` exposes `FILTER_X86`, `FILTER_ARM`, `FILTER_ARMTHUMB`,
  `FILTER_POWERPC`, `FILTER_SPARC`, `FILTER_IA64`, and `FILTER_DELTA`, and a raw
  `[FILTER_X86, FILTER_LZMA2]` chain round-trips. So the common executable-archive
  case (LZMA2 + a BCJ filter, optionally + Delta) is **pure stdlib** — no pybcj, no
  rolled-own branch filters, no external tool.
- `pyppmd`, `inflate64`, and `pybcj` are already present in the environment
  *transitively via py7zr*. Dropping py7zr and adding `pyppmd` + `inflate64` as
  direct optionals is therefore a re-labeling, not new dependency weight. We do
  **not** need `pybcj` (liblzma covers BCJ for the LZMA2 chains that matter).
- **PPMd and Deflate64 become first-class stream decompressors** (alongside the
  existing gzip/bz2/lzma/zstd/brotli openers in `compressed_streams.py`), so they are
  shared with the planned native ZIP reader rather than 7z-local. This is deliberate:
  stdlib `zipfile` does **not** support Deflate64 — `_get_decompressor` raises
  `NotImplementedError: compression type 9 (deflate64)` — so a native ZIP reader will
  need the same `inflate64` backend (ZIP method 9).

**Coder chains and bind pairs.** A folder's coders form a small DAG joined by *bind
pairs* (`out_index → in_index`) with a list of packed-stream indices feeding the
unbound inputs. Two cases matter:

```
Linear chain (≈ all real archives)        BCJ2 (the one exception)
┌─────┐    ┌───────┐                      ┌──────┐  4 inputs, 3 packed streams
│ BCJ │───▶│ LZMA2 │                      │ BCJ2 │◀── main  (LZMA2)
└─────┘    └───────┘                      │      │◀── call  (LZMA2)
   ▲                                       │      │◀── jump  (LZMA2)
   └─ 1 packed stream                      └──────┘◀── rc    (raw range coder)
```

For a **linear chain** the pipeline is the coder list applied in reverse (decode
order), which is exactly how stdlib builds a raw filter list — a single
`lzma.LZMADecompressor(format=FORMAT_RAW, filters=[...])` can hold the whole
`[BCJ, LZMA2]` sequence, and non-lzma stages (zstd, bz2, …) chain as separate
`decompress()` steps. **BCJ2** is the only multi-input structure; it is *not*
supported by py7zr either (py7zr raises `UnsupportedCompressionMethodError`), so the
native reader reaches parity by detecting it and raising the same kind of clean
error. Folders with more than 4 coders are already rejected upstream.

**Stream wrapping.** archivey already has `DecompressorStream`
(`formats/decompressor_stream.py`), which turns a `decompress(data)`-style object
into a seekable, pull-based `BinaryIO` (with optional seek points). The folder
pipeline plugs into it the same way the gzip/brotli/zlib streams do today, so member
streams are ordinary readable files — no background thread, no queue.

**Solid folders.** Because we own the decompression loop, a solid folder is
decompressed **once** and its substreams are sliced out by their unpack sizes as we
read forward. This matches py7zr's single-pass `extract(targets=[...])` cost (design
§4.4) without the batch-collection dance: opening the whole archive is O(N); opening
a single member from a large solid folder still pays for the prefix up to that member
(the inherent solid cost, reported via `AccessCost.EXPENSIVE` from
`base-reader-architecture-extensions` §8.E).

### 4.4 Solid-archive extraction: native single-pass is O(N)

The native reader keeps the O(N) property py7zr's batch `extract()` had, but more
directly. Iterating the archive decompresses each folder once and slices its
substreams forward (§4.3), so a full pass is O(N) with no batch-collection logic.

- Full iteration: O(N) total decompression work, one pass per folder.
- Single-member `open()` from a solid folder: pays for the prefix up to that member
  within its folder (the inherent solid cost) — but only that folder, not the whole
  archive. The previous design routed `_open_member()` through the entire
  thread+queue path for a single-member list; the native path just opens the folder
  stream and seeks to the substream offset.

This is where the §8.E `AccessCost` enum earns its place: a solid 7z folder reports
`EXPENSIVE` (random access in a loop is O(N²)), a non-solid folder `DIRECT`, so
callers can choose in-order iteration when it matters without measuring.

### 4.5 Password handling: native per-folder decryptors

The native decompressor accepts the password (or the key derived from it per §3.5)
when it builds a folder's AES stage: `AESDecompressor(aes_properties, password)`.
This eliminates the `_temporary_password` context manager and the class-level
`_password_lock` entirely (§2.5), because nothing mutates shared state — each
folder's pipeline is constructed independently with whatever password applies to it.

This also **fixes the skipped multi-password test**
(`encryption_several_passwords__7zcmd.7z`) for free, using the password mechanism
archivey already has — no new public API. The base reader already accepts an
archive-wide password at open time (stored as `_archive_password`) and a per-call
`pwd` on `open(member, pwd=...)` / `_open_member(..., pwd=...)`, exactly as RAR and
the other readers use it. The native reader simply builds each folder's decryptor
from the `pwd` passed to that open call, falling back to the archive-wide default.
Because the decryptor is per-call rather than a global mutation of the shared
archive object, opening two members that need different passwords just works — which
is precisely what py7zr's global `folder.password` assignment made impossible.

Wrong passwords behave as in §3.5: there is no pre-flight check value, so a bad
password decrypts to garbage that fails the substream CRC — which we surface as a
decryption/corruption error.

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

Because py7zr is removed for decompression too, the entire push-model scaffolding
goes away — not "kept until phase 2":

| Current complexity | Why it exists | Native reader stance |
|---|---|---|
| Background extractor thread + two queues + 64-chunk backpressure | py7zr is push-based (`WriterFactory`) | **Removed** — native decompression is pull-based (§4.3) |
| `_temporary_password` context manager + class `_password_lock` | py7zr has no per-call password | **Removed** — per-folder `AESDecompressor` takes the password (§4.5) |
| `reset()` before every `extract()` | py7zr's `Worker` is stateful | **Removed** — no py7zr `Worker` |
| `_build_extract_filename_to_member_map` + `get_sanitized_output_path` | py7zr renames duplicates during extraction | **Removed** — we map members to folders/substreams directly; no name round-trip |
| `StreamingFile` / `WriterFactory` / `NullIO` | adapt push API to a pull iterator | **Removed** — members are plain `DecompressorStream` files |
| `_is_member_encrypted` via `SupportedMethods.needs_password` | py7zr private API | Replaced with a direct coder-list check in the native parser |
| `archiveinfo()` crash guard (empty archive) | py7zr bug | `is_solid` computed directly from `num_unpackstreams_folders` |
| `py7zr.helpers.filetime_to_dt` | py7zr utility | Inlined: `datetime(1601,1,1) + timedelta(microseconds=ft//10)` |
| Exception catch-all in extractor thread | py7zr exceptions not wrapped on a thread | **Removed** — no thread; errors propagate directly and go through `_translate_exception` |

What remains genuinely new (the cost of dropping py7zr): the coder-chain →
decompressor-pipeline builder (§4.3), the pack-stream locator, the AES key
derivation + CBC stage (§3.5), and the PPMd/Deflate64 backends. All are bounded and
mostly thin wrappers over existing libraries.

---

## 6. Files affected by the change

| File | Change |
|---|---|
| New: `src/archivey/formats/sevenzip_parser.py` | `SevenZipParser`, `SevenZipMemberInfo`, raw header reading, pack-stream offsets, coder chains/bind pairs |
| New: `src/archivey/formats/sevenzip_codecs.py` (or fold into the parser) | Coder-chain → `DecompressorStream` pipeline builder; AES-256 stage (KDF §3.5 + CBC); BCJ2 detect-and-raise |
| `src/archivey/formats/compressed_streams.py` | Add `pyppmd` (PPMd var.H) and `inflate64` (Deflate64) stream openers — **shared with the future native ZIP reader** (ZIP method 9) |
| `src/archivey/formats/sevenzip_reader.py` | Drop py7zr entirely; build members from the native parser; replace the thread+queue extractor with native pull-based folder decompression; override `_iter_members_and_streams_internal` (§8.A) |
| `src/archivey/internal/dependency_checker.py` | Remove `py7zr`; gate PPMd/Deflate64/AES/zstd/brotli on their own packages with clear `PackageNotInstalledError`s |
| `pyproject.toml` | Remove `py7zr>=1.0.0`; add `pyppmd` and `inflate64` to the `optional` (and `optional-freethreaded`) extras |
| Tests | All existing 7z archives still pass (metadata + extraction + CRC); per-codec decompression tests; BCJ2 raises cleanly |

---

## 7. Open questions / risks

1. **LZMA1 + BCJ in one chain** — liblzma's raw decoder chains BCJ with **LZMA2**
   fine (verified), but the rare **LZMA1 + BCJ** combination is what py7zr handles
   with a special case: it routes the BCJ stage through its own `pybcj` decoder
   while LZMA1 stays on liblzma (compressor.py:634, the "native + alternative" hack).
   The native reader must either (a) special-case LZMA1+BCJ the same way (pull in
   `pybcj` only for this path) or (b) decode the BCJ stage as a separate liblzma
   single-filter step. Decide during implementation; LZMA2 is the 7z default so this
   is a tail case, but it must not silently corrupt output. **Action:** add a test
   archive that uses LZMA1+BCJ.

2. **BCJ2 (`0x0303011B`)** — the 4-stream branch coder. **Not supported by py7zr
   either** (raises `UnsupportedCompressionMethodError`), so detect-and-raise reaches
   parity. The risk is purely that we must detect it *before* attempting to build a
   linear pipeline (it has `numinstreams == 4`), and raise the same kind of clean
   error archivey already raises for unsupported methods. Acceptable per the agreed
   "strictly equal-or-better than py7zr" bar.

3. **Newer BCJ filters (ARM64, RISC-V)** — added to xz/7z more recently and **not**
   exposed by Python's `lzma` on older liblzma (this env's `lzma` lists x86/ARM/ARMT/
   PPC/SPARC/IA64 + Delta, no ARM64). Archives using ARM64/RISC-V BCJ would need a
   newer liblzma or a fallback. Treat as detect-and-raise for now (still a clean
   error, not corruption); revisit if it shows up in real archives.

4. **PPMd / Deflate64 backends** — `pyppmd.Ppmd7Decoder` (7z uses PPMd var.H = Ppmd7)
   and `inflate64.Inflater` cover these; both are already in the tree via py7zr today.
   Risk is API-surface fit into `DecompressorStream` (chunked `decompress`/`inflate`
   semantics, end-of-stream handling) — bounded wrapper work, validated by the
   existing PPMd/Deflate64 test archives.

5. **Multi-volume archives** — not supported (same as RAR). The native parser raises
   `ArchiveError` cleanly at volume detection rather than producing garbage.

6. **Anti-files** — 7z "anti-items" (deletion markers in delta-update archives).
   Warn and skip, matching the metadata-reader plan.

7. **Empty / `FILES_INFO`-absent archives** — handle the absent-`FILES_INFO` case
   directly (no py7zr crash guard needed); `is_solid` from `num_unpackstreams_folders`.

8. **Thread-safety** — with the thread+queue gone, the reader's concurrency model is
   just "one decompression pipeline per open stream." Document that two concurrent
   `open()`s on overlapping folders each build independent pipelines and seek the
   archive file independently (the shared file handle's seeks must be serialized or
   each pipeline given its own handle/`pread`).

9. **`compressed` size for solid folders** — unchanged structural limit: per-file
   packed size is undefined inside a solid stream; expose the folder total clearly.

10. **Compression method mapping** — coder IDs → typed `CompressionMethod` (§8.D
    enum) for the primary codec, with the full chain (e.g. `"LZMA2 + BCJ"`) in
    `compression_method_detail`. Currently `None` for 7z.

---

## 8. Useful references

- 7z format specification: included in the 7-Zip source tree as
  `DOC/7zFormat.txt` (also at https://py7zr.readthedocs.io/en/latest/archive_format.html).
- py7zr source (1.1.0): `/tmp/py7zr_dl/py7zr_src/py7zr/` (extracted from
  wheel during this research session).
  - `archiveinfo.py` — header parsing: `Header`, `SignatureHeader`, `Folder`,
    `UnpackInfo`, `PackInfo`, `SubStreamsInfo`, `FilesInfo`.
  - `compressor.py` — `AESDecompressor`, `SevenZipDecompressor`, `Folder.get_decompressor`.
    Note its `methods_map` / `_get_lzma_decompressor` logic: BCJ+LZMA2 goes through
    liblzma (`FORMAT_RAW`), and only the LZMA1+BCJ "native + alternative" case
    (compressor.py:634) falls back to the `pybcj` package — the model for §4.3.
- Codec packages (decompression backends):
  - `lzma` (stdlib) — LZMA1/LZMA2 + BCJ (x86/ARM/ARMT/PPC/SPARC/IA64) + Delta, all via
    `FORMAT_RAW` filter chains; `zlib`/`bz2` (stdlib) — Deflate/BZip2.
  - `pyppmd` — PPMd var.H (`Ppmd7Decoder`); `inflate64` — Deflate64 (`Inflater`).
  - `zstandard`, `brotli`, `cryptography`/`pycryptodome` — existing archivey optionals.
  - `py7zr.py` — `SevenZipFile._real_get_contents`, `_extract`, `_is_solid`,
    `archiveinfo()`, `ArchiveFile`.
  - `properties.py` — `MAGIC_7Z`, `PROPERTY.*` constants, `CompressionMethod.*`.
- Current implementation: `src/archivey/formats/sevenzip_reader.py`.
- Base class: `src/archivey/internal/base_reader.py`.
- RAR reader (for comparison, especially `RarStreamReader` solid-stream
  pattern): `src/archivey/formats/rar_reader.py`.
