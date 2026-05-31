# RAR Native Reader — Architecture Design Notes

This document captures the research needed to replace `rarfile`-based RAR handling
with a built-in implementation that parses archive metadata itself but still calls
an external decompressor (`unrar` or a compatible library) for actual decompression.

---

## 1. What the BaseArchiveReader contract requires

Every format reader inherits from `BaseArchiveReader`
(`src/archivey/internal/base_reader.py`).  The contract is defined by three
abstract methods and one hook:

| Method | Obligation |
|---|---|
| `iter_members_for_registration()` | Yield `ArchiveMember` objects one by one, metadata-only, with `raw_info` pointing at whatever format-specific object the reader keeps. Called once; the base class drives member ID assignment and the lookup indices. |
| `get_archive_info()` → `ArchiveInfo` | Return archive-level metadata: `is_solid`, `version`, `comment`, `extra` (format-specific dict). Called lazily; may cache. |
| `_open_member(member, pwd, for_iteration)` → `BinaryIO` | Open and return a readable byte stream for a single file member.  Guaranteed to be called only for `is_file` members after link resolution.  `for_iteration=True` is a hint that the call comes from sequential iteration; `for_iteration=False` means random access. |
| `_translate_exception(e)` → `ArchiveError \| None` | Map format library exceptions to archivey exception types; return `None` for unrecognised exceptions. |

Optional hooks:

- `_prepare_member_for_open(member, pwd, for_iteration)` — called before link
  resolution in `_open_internal`.  The current RAR reader uses this to fetch
  encrypted symlink/hardlink targets lazily.
- `_close_archive()` — format-specific teardown.

Constructor parameters that control base-class behaviour:

- `members_list_supported=True` — tells the base class it can call
  `get_members_if_available()` to build the full member list up-front (RAR has a
  central-directory-like structure, so this is safe).
- `streaming_only=False` — RAR supports random access so this is always False.

---

## 2. What the current rarfile-based reader does

`src/archivey/formats/rar_reader.py` wraps the `rarfile` third-party library
(≥ 4.2).  Here is what each phase uses from `rarfile`:

### 2.1 Metadata reading

`rarfile` parses the entire block sequence on construction (`RarFile.__init__` →
`_parse()`).  The result is a `CommonParser` subclass (`Rar3Parser` or
`Rar5Parser`) that holds:

- `_info_list` — ordered list of `Rar3Info` / `Rar5Info` objects
- `_main` — the archive main header (solid flag, password flag, comment …)
- `_file_parser.has_header_encryption()` — whether headers are encrypted
- `is_solid()` / `needs_password()` — delegated to the parser

`rar_reader.py:iter_members_for_registration()` (line 610) calls
`self._archive.infolist()` and converts each `RarInfo` to an `ArchiveMember`.
Fields extracted:

| `RarInfo` field | Usage |
|---|---|
| `filename` / `orig_filename` | UTF-8/UTF-16 dual decode, corruption detection |
| `file_size`, `compress_size` | sizes |
| `mtime` / `date_time` | modification time (RAR5 UTC-aware, RAR3 naive) |
| `is_dir()`, `is_file()`, `is_symlink()`, `file_redir` | member type |
| `mode` | unix permissions |
| `CRC` / `blake2sp_hash` | integrity |
| `compress_type` (0x30–0x35) | compression method name |
| `host_os` | create system |
| `needs_password()`, `file_encryption` | encryption metadata |
| `comment` | per-file comment (RAR3 only) |

### 2.2 Decompression path

`rarfile` never decompresses in Python.  When `RarFile.open(inf, pwd)` is
called it picks a strategy (lines 1280–1303 of `rarfile.py`):

1. **`_open_clear`** — file is stored uncompressed (M0) and not encrypted: read
   the raw bytes directly from the archive file using `DirectReader` (a seekable
   Python reader that skips over RAR block headers).

2. **`_open_hack`** — enabled when `USE_EXTRACT_HACK=1` (default) and the file
   is small (< `HACK_SIZE_LIMIT` = 20 MB) and the archive is *not* solid and
   not encrypted:
   - Constructs a minimal single-file RAR archive in a temp file containing
     only the target entry's compressed data (with a synthetic main header and
     end-of-archive marker).
   - Runs `unrar p -inul` against that tiny temp file.
   - This lets unrar decompress just the one member without scanning the whole
     original archive — a significant speed win for large archives.
   - For RAR3: prefix is `RAR_ID + BLK_HDR + 13 zero bytes`.
   - For RAR5: prefix is `RAR5_ID + minimal main block + endarc block`.
   - Disabled for: solid flag on archive or file, encrypted files/headers,
     split volumes, file copies (`RAR5_XREDIR_FILE_COPY`), large files.

3. **`_open_unrar_membuf`** — if the archive is a file-like object (in-memory):
   write it to a temp file first, then call unrar normally.

4. **`_open_unrar`** — full archive path, ask unrar to extract just the named
   member:  `unrar p -inul [-pPASSWORD] archive.rar member_path`.
   Result is `PipeReader`, which wraps the subprocess stdout and handles
   CRC/Blake2sp verification on close.

`rarfile` supports multiple backend tools (picked in priority order by
`tool_setup()`):

| Tool | Command | Known limitations |
|---|---|---|
| `unrar` | `unrar p -inul` | Full RAR3/RAR5 support, passwords, solid |
| `unar` | `unar -q -o -` | No RAR2 locked files, no RAR5 Blake2sp |
| `bsdtar` | `bsdtar -x --to-stdout -f` | No solid, no passwords, no RARVM filters |
| `7z` / `7zz` | `7z e -so -bb0` | Mostly complete |

### 2.3 Solid archive problem

A **solid archive** is one where multiple files are compressed as a single
stream, so each file's decompression depends on the full decompressor state
from all preceding files.  The solid flag lives at two levels:

- **Archive-level** (`RAR_MAIN_SOLID` flag in RAR3 main header;
  `RAR5_MAIN_FLAG_SOLID` in RAR5 main block) — the whole archive is solid.
- **File-level** (`RAR5_COMPR_SOLID` in file compress flags) — this specific
  file continues the preceding solid stream.

When an archive is solid, calling `rarfile.RarFile.open(member)` for the *k*-th
member requires unrar to re-decompress members 1 through k−1 before it can
produce member k.  If all N members are extracted one by one, the total work
is O(N²).

**Current workaround — `RarStreamReader`** (`rar_reader.py` lines 388–477,
enabled with config `use_rar_stream=True`):

1. Spawn one `unrar p -inul` subprocess that prints all file data
   sequentially to stdout.
2. For each member (in member-ID order), wrap a fixed-size slice of that
   shared stdout into a `RarStreamMemberFile` that tracks remaining bytes and
   validates the CRC on full read.
3. Yield `(member, stream)` pairs from `rar_stream_iterator()`.

This reduces total decompression work to O(N) at the cost of requiring
sequential consumption of the stream.  Caveats:

- Requires a file path (not in-memory stream).
- Only safe when all members share the same password (solid archives
  guarantee this).  Mixed encryption/no-encryption archives may mismatch if
  unrar silently skips some files.
- CRC validation handles the "tweaked checksum" case for RAR5 encrypted
  files where the stored CRC is modified with the password via
  HMAC-SHA-256 over PBKDF2 key material.

### 2.4 Encrypted headers

RAR archives can encrypt file headers (not just file data):

- **RAR3**: `RAR_MAIN_PASSWORD` flag on the main block signals encrypted
  headers.  The block content is AES-CBC encrypted; `rarfile` calls
  `_decrypt_header()` with AES-256 derived from password + salt via SHA-1 (20
  rounds of mixing).
- **RAR5**: A separate `RAR5_BLOCK_ENCRYPTION` block holds the AES-256
  parameters (KDF count, salt, IV).  If present, all subsequent blocks are
  encrypted.

`rar_reader.py:get_archive_info()` (line 688) reads this via the private
`self._archive._file_parser.has_header_encryption()`.

Without the password, header-encrypted archives report no members.

### 2.5 Encryption per-member (RAR5)

RAR5 stores per-file encryption in the `RAR5_XFILE_ENCRYPTION` extra record:

```
(algo, flags, kdf_count, salt[16], iv[16], check_value[12])
```

- `algo` = 0 → AES-256-CBC.
- `flags & RAR5_ENC_FLAG_HAS_CHECKVAL` (0x01) → `check_value` present,
  allows pre-flight password verification (avoids decompressing garbage).
- `flags & RAR5_XENC_TWEAKED` (0x02) → checksums are "tweaked": the stored
  `CRC` field is not the actual data CRC but `HMAC-SHA256(CRC, key)` where
  `key = PBKDF2-SHA256(password, salt, 1<<kdf_count + 16)`.

`rar_reader.py` implements this independently:

- `verify_rar5_password()` (line 215) — pre-flight check using the 8+4 byte
  check value; cached via `lru_cache`.
- `convert_crc_to_encrypted()` (line 241) — re-derives the tweaked CRC from
  a computed data CRC and the password.
- `check_rarinfo_crc()` (line 270) — dispatches to either plain CRC compare
  or tweaked compare.

---

## 3. RAR format essentials

### 3.1 RAR3 (v2.0 – v4.x on-disk)

**Signatures**:

```
RAR_ID  = b"Rar!\x1a\x07\x00"     (7 bytes)
```

SFX archives may have an arbitrary-length prefix; `is_rarfile_sfx` scans up
to 2 MB.

**Block layout** — every block starts with:

```
CRC16  (2 bytes)  — CRC of the block header
TYPE   (1 byte)   — block type
FLAGS  (2 bytes)
SIZE   (2 bytes)  — header size (includes all fixed fields, excludes data area)
[ADD_SIZE (4 bytes) if FLAGS & RAR_LONG_BLOCK]  — data area size
```

Block types and their solid/encryption relevance:

| Type | Const | Notes |
|---|---|---|
| 0x72 | `RAR_BLOCK_MARK` | marker block (fixed) |
| 0x73 | `RAR_BLOCK_MAIN` | archive header; `flags & RAR_MAIN_SOLID` = solid; `flags & RAR_MAIN_PASSWORD` = encrypted headers; `flags & RAR_MAIN_VOLUME` = multi-volume |
| 0x74 | `RAR_BLOCK_FILE` | file entry; `flags & RAR_FILE_PASSWORD` = encrypted; `flags & RAR_FILE_SOLID` = continues solid stream |
| 0x7a | `RAR_BLOCK_SUB` | service/sub-block (comments, NTFS streams, …) |
| 0x7b | `RAR_BLOCK_ENDARC` | end of archive |

File header additional fields (after the fixed 7-byte common part):

```
PACK_SIZE  (4 bytes)  — compressed size
UNP_SIZE   (4 bytes)  — uncompressed size
HOST_OS    (1 byte)
FILE_CRC   (4 bytes)  — CRC32 of uncompressed data
FTIME      (4 bytes)  — MS-DOS timestamp
UNP_VER    (1 byte)   — minimum unrar version (e.g., 0x1d = 29)
METHOD     (1 byte)   — 0x30..0x35
NAME_SIZE  (2 bytes)
ATTR       (4 bytes)
[HIGH_PACK, HIGH_UNP  (4+4 bytes) if FLAGS & RAR_FILE_LARGE]
[FILENAME  (NAME_SIZE bytes)]
[SALT      (8 bytes) if FLAGS & RAR_FILE_SALT]
[EXTTIME   if FLAGS & RAR_FILE_EXTTIME]  — sub-second timestamps
```

Unicode filenames: if `FLAGS & RAR_FILE_UNICODE`, the filename field starts
with the 8-bit (OEM/windows-1252) name, followed by a compressed UTF-16
encoding (the `UnicodeFilename` class in `rarfile.py` decodes it).  The
non-BMP truncation bug (emoji stored as surrogates) occurs in some RAR 2.9–4
archivers and is detected by `get_non_corrupted_filename()`.

### 3.2 RAR5 (v5.x on-disk)

**Signatures**:

```
RAR5_ID = b"Rar!\x1a\x07\x01\x00"  (8 bytes)
```

**Block layout** — variable-length integer encoding (vint: 7 bits per byte,
MSB continuation flag):

```
HEADER_CRC32  (4 bytes)  — CRC of everything that follows in the header
HEADER_SIZE   (vint)
HEADER_TYPE   (vint)     — 1=MAIN, 2=FILE, 3=SERVICE, 4=ENCRYPTION, 5=ENDARC
HEADER_FLAGS  (vint)
[EXTRA_DATA_SIZE (vint) if flags & RAR5_BLOCK_FLAG_EXTRA_DATA]
[DATA_AREA_SIZE  (vint) if flags & RAR5_BLOCK_FLAG_DATA_AREA]
... type-specific fields ...
[EXTRA_DATA  — array of typed extra records]
```

Important MAIN block flags:

- `RAR5_MAIN_FLAG_SOLID` (0x04) — solid archive.
- `RAR5_MAIN_FLAG_ISVOL` (0x01) — multi-volume.

File block fields:

```
FILE_FLAGS  (vint)     — RAR5_FILE_FLAG_ISDIR, HAS_MTIME, HAS_CRC32, UNKNOWN_SIZE
UNP_SIZE    (vint)
ATTR        (vint)
MTIME       (4 bytes)  — Unix timestamp if FILE_FLAG_HAS_MTIME
DATA_CRC32  (4 bytes)  — if FILE_FLAG_HAS_CRC32
COMPRESS_INFO  (vint)  — method(3b), solid(1b), algo version(6b), dict(4b)
HOST_OS     (vint)     — 0=Windows, 1=Unix
NAME_LEN    (vint)
NAME        (bytes, UTF-8)
[EXTRA_DATA records]
```

`compress_info & RAR5_COMPR_SOLID` (0x40) means this file depends on the
previous compressed stream (per-file solid flag).

Extra record types relevant to a reader:

| Type | Content |
|---|---|
| `RAR5_XFILE_ENCRYPTION` (1) | AES-256 params: algo, flags, kdf_count, salt[16], iv[16], check_value[12] |
| `RAR5_XFILE_HASH` (2) | Blake2SP hash (32 bytes) |
| `RAR5_XFILE_TIME` (3) | Extended time: mtime/ctime/atime; optional nanosecond precision |
| `RAR5_XFILE_REDIR` (5) | Symlink / hardlink / file copy target |
| `RAR5_XFILE_OWNER` (6) | Unix uid/gid and name strings |

The `RAR5_BLOCK_ENCRYPTION` block (type 4) precedes all file blocks when
headers are encrypted.  It holds: `algo`, `flags`, `kdf_count`, `salt[16]`.
The consumer must decrypt each subsequent header block (AES-256-CBC) before
it can parse it.

---

## 4. What a native reader needs to implement

### 4.1 Scope: metadata only

The goal is to replace `rarfile` for **metadata parsing** while still
delegating decompression to an external tool (`unrar`, or in future a native
Python library such as `unrar-cffi`).

This means the new reader must:

1. Detect and skip the SFX prefix (scan up to 2 MB for `RAR_ID` / `RAR5_ID`).
2. Parse the archive's block sequence to build the member list.
3. Decode all member metadata fields listed in §2.1.
4. Detect `is_solid`, `has_header_encryption`, `needs_password`.
5. For header-encrypted archives, derive the AES-256 key from password and
   decrypt headers before parsing.
6. For `RAR5_XFILE_ENCRYPTION`, expose the per-file encryption record so
   `verify_rar5_password()` and `convert_crc_to_encrypted()` can continue to
   work as-is (they only need the `(algo, flags, kdf_count, salt, iv,
   check_value)` tuple, which we already extract as `RarEncryptionInfo`).
7. For RAR3 symlink/hardlink targets: the target is stored as the file's data
   content, so we need to call the external tool once to read it (same as the
   current `_get_link_target()` fallback path).
8. Provide the `raw_info` object (our own dataclass replacing `RarInfo`) with
   at minimum `filename`, `file_size`, `CRC`, `needs_password()`, and the
   encryption record, since these are referenced by `RarStreamMemberFile` and
   the CRC helpers.

### 4.2 Decompression strategy

Nothing changes conceptually from today:

| Path | When | Mechanism |
|---|---|---|
| **Stored** (M0, no encryption) | `compress_type == 0x30` | Read raw bytes directly from archive at `data_offset`, no subprocess. |
| **Random access** (non-solid, or solid but single member) | Default `_open_member` path | Spawn `unrar p -inul [-pPWD] archive.rar member_name`. |
| **Extract-hack** (non-solid, file ≤ 20 MB) | When enabled | Build minimal temp archive containing only this member's compressed block, run unrar on it. Avoids unrar re-scanning whole archive. |
| **Solid streaming** | `use_rar_stream=True` | One `unrar p -inul` subprocess, share stdout across all members via `RarStreamMemberFile`. Already implemented in `RarStreamReader`; no change needed. |

The extract-hack currently lives inside `rarfile`.  A native reader should
re-implement it:

- For **RAR3**: prefix = `RAR_ID + BLK_HDR(crc=0x90CF, type=0x73, flags=0, size=13) + 6 zero bytes`, then verbatim copy of the file header + compressed data block, no suffix.
- For **RAR5**: prefix = `RAR5_ID + crc32(main_hdr) + main_hdr + crc32(endarc_hdr) + endarc_hdr`, where `main_hdr = b"\x03\x01\x00\x00"` and `endarc_hdr = b"\x03\x05\x00\x00"`, then the file block verbatim.
- This requires storing the on-disk byte offsets and sizes of each block (`header_offset`, `header_size`, `add_size`) in the native `RarMemberInfo` so they can be re-read when building the temp archive.

Conditions to **disable** the extract-hack (same as rarfile):

- Archive is solid (`RAR_MAIN_SOLID` or `RAR5_MAIN_FLAG_SOLID`).
- File has `RAR_FILE_SOLID` / `RAR5_COMPR_SOLID` flag (depends on previous stream).
- File is password-protected.
- Headers are encrypted.
- File is split across volumes.
- File is a redirect / file copy (`file_redir` present).
- `file_size > HACK_SIZE_LIMIT` (20 MB).

### 4.3 Header decryption (encrypted headers)

For RAR3:

1. Each block's AES-256-CBC key is derived via 70,144 rounds of SHA-1 mixing
   (`rarfile` does this in `_gen_rar3_key`, not shown here but documented in
   WinRAR SDK).  The salt is 8 bytes stored right after the main header.
2. The `cryptography` or `pycryptodome` package is required (same as today).

For RAR5:

1. Read the `RAR5_BLOCK_ENCRYPTION` block (type 4): `algo=0`, `kdf_count`,
   `salt[16]`.
2. Derive AES-256 key: `PBKDF2-HMAC-SHA256(password, salt, 1<<kdf_count + 32)` —
   first 32 bytes are the key, next 16 bytes are the IV tweak XOR'd with the
   block's stored IV field.
3. Decrypt each subsequent block header with AES-256-CBC.

### 4.4 Native `RarMemberInfo` dataclass

Replace `Rar3Info` / `Rar5Info` as the `raw_info` payload.  Minimum fields
needed by existing consumers:

```python
@dataclass
class RarMemberInfo:
    filename: str
    orig_filename: bytes | None        # RAR3 8-bit name, for corruption detection
    file_size: int | None
    compress_size: int | None
    compress_type: int | None          # 0x30..0x35
    CRC: int | None
    blake2sp_hash: bytes | None
    flags: int                         # for RAR_FILE_PASSWORD, RAR_FILE_UNICODE, etc.
    rar_version: int                   # 3 or 5
    file_encryption: tuple | None      # RarEncryptionInfo namedtuple fields
    file_redir: tuple | None           # (type, flags, target) for RAR5
    host_os: int | None
    mode: int | None
    mtime: datetime | None
    ctime: datetime | None
    atime: datetime | None
    comment: str | None
    # for extract-hack:
    header_offset: int
    header_size: int
    data_offset: int
    volume_file: str
    # for is_solid detection:
    file_compress_flags: int           # RAR5 compress_info field

    def needs_password(self) -> bool: ...
    def is_dir(self) -> bool: ...
    def is_file(self) -> bool: ...
    def is_symlink(self) -> bool: ...
```

### 4.5 Parser classes

Following rarfile's pattern, separate parsers for RAR3 and RAR5:

```
NativeRar3Parser
    - read_blocks() → list[RarMemberInfo]
    - _parse_block_header(fd) → header dict
    - _decrypt_header(fd) → decrypted fd  (RAR3 header encryption)
    - is_solid() → bool
    - needs_password() → bool
    - has_header_encryption() → bool
    - comment: str | None

NativeRar5Parser
    - read_blocks() → list[RarMemberInfo]
    - _parse_vint(fd) → int
    - _parse_block(fd) → header dict
    - _decrypt_header(fd, enc_block) → decrypted fd  (RAR5 header encryption)
    - _parse_extra(data, h) → None  (populates extra fields on h)
    - is_solid() → bool
    - needs_password() → bool
    - has_header_encryption() → bool
```

### 4.6 Unicode filename handling

The `get_non_corrupted_filename()` function can remain unchanged — it only
needs `rarinfo.filename` (decoded UTF-16) and `rarinfo.orig_filename` (8-bit
bytes).  Both fields will be present on `RarMemberInfo`.

The RAR3 `UnicodeFilename` decompressor logic (currently in `rarfile`) will
need to be ported, or we can use the same UTF-16 compressed format description
from the RAR Technical Note.

---

## 5. Things that can be dropped / simplified

| Current complexity | Why it exists | Native reader stance |
|---|---|---|
| `rarfile.RarFile` constructor + `_parse()` | `rarfile` re-parses on `setpassword()` if headers were encrypted | Native reader just re-parses once when password is provided |
| `_open_unrar_membuf` path (in-memory archive → temp file) | `rarfile` supports file-like objects | We can keep or drop; most use is file-path based |
| Multiple backend tools (unar, bsdtar, 7z) | `rarfile` abstracts tool selection | Initially target `unrar` only; add others if needed |
| `rarfile.FORCE_TOOL` and `CURRENT_SETUP` global | Tool selection caching | Not needed if we own the subprocess |
| `rarfile.RarExtFile.seek()` | rarfile's open() claims to be seekable | Our `_open_member` is not expected to be seekable (base class sets `seekable=not streaming_only=True`) |

---

## 6. Files affected by the change

| File | Change |
|---|---|
| `src/archivey/formats/rar_reader.py` | Replace `rarfile.RarFile` / `Rar3Info` / `Rar5Info` usage with native parser; keep `RarStreamReader`, `RarStreamMemberFile`, all CRC/encryption helpers |
| New: `src/archivey/formats/rar_parser.py` | `NativeRar3Parser`, `NativeRar5Parser`, `RarMemberInfo` |
| `src/archivey/internal/dependency_checker.py` | `rarfile` becomes optional; list `unrar` as required tool instead |
| `pyproject.toml` | Remove `rarfile>=4.2` from `optional` dependencies |
| Tests | Verify all existing RAR test archives still pass; add parser unit tests |

---

## 7. Open questions / risks

1. **Multi-volume archives** — `rarfile` handles volume chaining
   (`_next_volname`, `NeedFirstVolume`).  The native reader needs the same
   logic.  Currently archivey does not advertise multi-volume support, but
   it should at minimum raise `ArchiveError` cleanly.

2. **RARVM filters** (RAR3) — Some RAR3 archives use bytecode filter programs
   for preprocessing (e.g., the "delta" or "exe" filters).  `unrar` handles
   these transparently.  The native parser does not need to implement RARVM;
   the extract-hack will still work because unrar in the temp archive path runs
   over the raw compressed block including any filter metadata.

3. **RAR2 / very old formats** — RAR 2.0 (extract_version ≤ 20) archives have
   a slightly different block layout.  `rarfile` handles them.  Decide whether
   to support them or raise `ArchiveError("RAR2 not supported")`.

4. **Blake2sp verification** — RAR5 files may have Blake2sp instead of CRC32.
   `rarfile`'s `PipeReader` verifies this; our subprocess-based path does not
   currently verify anything (verification is left to the subprocess exit code).
   The `RarStreamMemberFile` only checks CRC32.  For a native reader, if
   `blake2sp_hash` is present and `CRC` is None, streaming CRC checks become
   no-ops.  Consider adding Blake2sp streaming verification.

5. **Encrypted headers + in-memory stream** — current reader raises
   `ArchiveStreamNotSeekableError` for non-seekable streams; seekable
   in-memory streams work.  Header decryption requires reading a fixed number
   of bytes at known offsets, which is compatible with seekable streams.
   The native reader can preserve this behaviour.

6. **`file_encryption` is a private rarfile attribute** — currently accessed
   via `rarinfo.file_encryption`.  With a native reader this becomes a
   first-class field on `RarMemberInfo` and the hack is gone.

---

## 8. Useful references

- WinRAR Technical Note (included with WinRAR): `technote.txt`; defines the
  complete RAR5 format.
- RAR3 format: historically undocumented; `rarfile.py` source is the best
  available description.
- `rarfile.py` source (4.2): `/tmp/rarfile_src/rarfile.py` (extracted from
  wheel during this research session).
- Current implementation: `src/archivey/formats/rar_reader.py`.
- Base class: `src/archivey/internal/base_reader.py`.
- 7-zip reader (parallel for solid-archive streaming pattern):
  `src/archivey/formats/sevenzip_reader.py`.
