# ZIP Reader — stdlib zipfile Limitations and Workarounds

This document analyses what the Python stdlib `zipfile` module provides, where
it falls short, and what archivey does to work around each gap.  Unlike the RAR
and 7z design docs, the goal here is not to replace the library — `zipfile` is
a full in-process decompressor and there is no external tool in the loop.  The
focus is on understanding the boundary between what the library does and what
archivey must handle itself.

---

## 1. BaseArchiveReader flags for ZIP

| Flag | Value | Reason |
|---|---|---|
| `members_list_supported` | `True` | ZIP has a central directory at end-of-file; all member metadata is available before any decompression. |
| `streaming_only` | `False` | Random access is fully supported (and required). |

Non-seekable input is rejected at construction (line 157–160 in `zip_reader.py`)
because `zipfile` must seek to the End of Central Directory record at the tail
of the file before it can read anything.  There is no workaround; the entire
file must be buffered in memory or on disk first.

---

## 2. What zipfile provides

### 2.1 Central directory parsing

`ZipFile._RealGetContents()` seeks to the end of the file, finds the **End of
Central Directory** record (magic `0x06054b50`, up to 64 KB from EOF to allow
for a comment), then reads the entire central directory in one shot into a
`BytesIO` buffer.

For archives > 4 GB, `_EndRecData64()` additionally reads the **ZIP64 End of
Central Directory Locator** (`0x07064b50`) and the **ZIP64 EOCD** record
(`0x06064b50`) which hold 64-bit values for offsets and counts.

Each central directory entry provides a `ZipInfo` object with:

| `ZipInfo` field | Format | Notes |
|---|---|---|
| `filename` | UTF-8 or CP437 | UTF-8 flag in `flag_bits & 0x800`; else CP437 |
| `date_time` | 6-tuple, DOS format | 2-second granularity, years 1980–2107, local time |
| `compress_type` | integer | 0=store, 8=deflate, 12=bzip2, 14=lzma |
| `CRC` | uint32 | CRC32 of uncompressed data |
| `compress_size` | uint32/uint64 | ZIP64 if central dir shows 0xFFFFFFFF |
| `file_size` | uint32/uint64 | ZIP64 if central dir shows 0xFFFFFFFF |
| `flag_bits` | uint16 | encryption (bit 0), data descriptor (bit 3), UTF-8 (bit 11), strong encryption (bit 6) |
| `create_system` | uint8 | 0=DOS, 3=UNIX, 19=macOS, etc. |
| `create_version` | uint8 | version made by (major*10 + minor) |
| `extract_version` | uint8 | minimum version needed to extract |
| `external_attr` | uint32 | upper 16 bits = Unix mode; lower 16 bits = DOS attributes |
| `internal_attr` | uint16 | bit 0 = text file |
| `comment` | bytes | per-file comment (raw bytes, no encoding) |
| `extra` | bytes | raw extra fields (not parsed beyond ZIP64 by stdlib) |
| `header_offset` | int | offset of local file header |
| `volume` | int | disk number (multi-disk not supported) |

`ZipInfo._decodeExtra()` is called automatically and parses one extra field:
`0x0001` (ZIP64) to replace 0xFFFFFFFF placeholders with 64-bit values.  All
other extra field tags are silently ignored.

### 2.2 Compression support

`zipfile` supports four compression methods natively:

| Method ID | Name | Dependency |
|---|---|---|
| 0 | stored | none |
| 8 | deflated | `zlib` |
| 12 | bzip2 | `bz2` |
| 14 | lzma | `lzma` |

Any other method raises `NotImplementedError("That compression method is not
supported")`.  Historical methods (shrink, reduce×4, implode, tokenize) and
modern ones (deflate64, brotli, zstd, wavpack, ppmd) are not implemented.

### 2.3 Encryption

Only **ZipCrypto** (Traditional PKWARE Encryption, sometimes called "ZipCrypto"
or "classic encryption") is supported.  The implementation (`_ZipDecrypter`,
lines 611–645) uses a 3-word key schedule updated per byte.

Strong encryption (flag bit 6, `_MASK_STRONG_ENCRYPTION`) raises
`NotImplementedError("strong encryption (flag bit 6)")` — this covers WinZip
AES (method 99) and PKWARE Strong Encryption.  There is no path to support
WinZip AES-256 without a third-party library.

Password check: the first 12 encrypted bytes are a random header; byte 12 is
compared against either the high byte of the CRC32 (normal) or the high byte of
`_raw_time` (if the data descriptor flag is set).

### 2.4 The `open()` return value (`ZipExtFile`)

`ZipFile.open(name, mode="r", pwd=None)` returns a `ZipExtFile`
(`io.BufferedIOBase` subclass) that:

- Reads and decompresses on-the-fly in 4 KB blocks (`MIN_READ_SIZE = 4096`).
- Supports `read(n)`, `read1(n)`, `readline()`, `peek(n)`.
- Supports **seeking** if the underlying file is seekable — backward seeking
  restarts decompression from the local file header.
- Verifies CRC32 on close.
- Decrypts transparently if password is provided.

### 2.5 Data descriptor records

When a file is written to a non-seekable output (flag bit 3,
`_MASK_USE_DATA_DESCRIPTOR`), the CRC and sizes are unknown at the time the
local header is written and appear in a trailing data descriptor block instead.
`zipfile` handles this transparently during reading: it uses `_end_offset` (the
offset of the next entry's local header, computed from the central directory) to
know when the compressed data ends, then reads and discards the data descriptor
for CRC verification.

---

## 3. Limitations zipfile does NOT handle — archivey workarounds

### 3.1 Timestamp precision and timezone

**The problem**: `ZipInfo.date_time` is a 6-tuple derived from the DOS timestamp
format, which has 2-second granularity and stores local time with no timezone
info.  The year range is 1980–2107.

**Extra field 0x5455 — Extended Timestamp**: InfoZIP and many modern tools write
this extra field in both the local file header and the central directory.  It
stores one or more Unix timestamps (4-byte signed integers, seconds since epoch)
which are UTC and have 1-second precision.  The central-directory version
typically stores only `mtime` (bit 0 of flags); the local-header version may
also have `atime` (bit 1) and `ctime` (bit 2).

`zipfile` never parses `0x5455`.  It stores the raw extra bytes in `ZipInfo.extra`
but does nothing with them.

**Archivey's workaround** (`get_zipinfo_timestamp()`, lines 40–92 of
`zip_reader.py`): manually parses the `extra` field, scans for tag `0x5455`,
reads the Unix modtime, and returns a UTC-aware `datetime` object.  Falls back
to the DOS `date_time` tuple (naive, no timezone) if the field is absent or the
flag bit is not set.

**Implication**: archivey timestamps from ZIP are UTC-aware when the extended
field is present (most modern archives), naive local time otherwise.

### 3.2 Encoding of filenames and comments

**The problem**: The ZIP specification originally required CP437 encoding for
filenames.  Flag bit 11 (`_MASK_UTF_FILENAME`) signals UTF-8, but many legacy
tools wrote non-UTF-8, non-CP437 filenames without setting the flag — for
example, Windows tools using the system codepage (CP1252, Shift-JIS, etc.).

`zipfile` decodes filenames as UTF-8 if the flag is set, else CP437.  It does
not try other encodings on failure.  `ZipInfo.comment` is returned as raw bytes
without any decoding.

**Archivey's workaround**: `_ZIP_ENCODINGS = ["utf-8", "cp437", "cp1252",
"latin-1"]` (line 35).  The `decode_bytes_with_fallback()` utility tries each
encoding in order until one succeeds without errors.  Applied to member comments
(line 204).

For `ZipInfo.filename`, archivey relies on stdlib's decoding but the fallback
chain is not applied — the stdlib-decoded string is used as-is.  This could
silently produce wrong filenames for archives with CP1252 filenames and no UTF-8
flag.

### 3.3 Symlinks

**The problem**: ZIP has no native symlink entry type.  Unix tools (InfoZIP,
7-Zip on Unix) store symlinks as regular files with the Unix mode bits in
`external_attr >> 16` set to `S_IFLNK (0o120000)` and the symlink target as
the file content.

`zipfile` does not recognise or expose this convention.  `ZipInfo.is_dir()` and
the internal flags only look at the filename (trailing slash) and the MS-DOS
attribute bits.

**Archivey's workaround** (`_zipinfo_to_archive_member()`, lines 181–231):

```python
mode = info.external_attr >> 16
is_link = stat.S_ISLNK(mode)   # checks S_IFLNK bit
```

If `is_link` is True, archivey immediately opens the member and reads its
content as the symlink target (`_read_link_target()`, line 224–231), then stores
it in `ArchiveMember.link_target`.  The member type is set to
`MemberType.SYMLINK`.

**Limitation**: This only works for Unix-created archives.  Windows-created
archives store `external_attr` with DOS attributes in the low 16 bits and zero
in the high 16 bits (or Windows attribute flags), so symlinks created by Windows
tools may not be detected.

### 3.4 Hardlinks

**The problem**: ZIP has no hardlink concept.  There is no standard way to
encode hardlinks in a ZIP file.

**Archivey's stance**: Hardlinks are not detected or supported in ZIP.  A file
that is a hardlink appears as a regular file.

### 3.5 Compression method name

`ZipInfo.compress_type` is an integer.  `zipfile` provides a `compressor_names`
dict but it is not stored on `ZipInfo`.

**Archivey's workaround**: `ZIP_COMPRESSION_METHODS` dict (lines 96–114) maps
method IDs to human-readable names.  Archivey stores the name as
`ArchiveMember.compression_method`.  Unknown IDs produce `"unknown"`.

### 3.6 AES-256 / WinZip strong encryption

`zipfile` raises `NotImplementedError` for flag bit 6 (strong encryption) and
does not parse the AES extra field (method 99, tag `0x9901`).

**Current archivey stance**: Not supported.  The `_translate_exception` handler
maps `RuntimeError("password required")` and `RuntimeError("Bad password")` to
`ArchiveEncryptedError`, but WinZip AES archives will raise `NotImplementedError`
which propagates as an unhandled exception (or could be mapped to
`ArchiveUnsupportedFeatureError`).

**What it would take to fix**: Parse extra field `0x9901` to get the AES key
strength (128/192/256) and the authentication code; use `cryptography` or
`pycryptodome` to derive the key via PBKDF2-SHA1 (not SHA-256), decrypt, and
verify the HMAC-SHA1 authentication code.  This cannot reuse zipfile internals
at all.

### 3.7 `external_attr` mode when archive is from a non-Unix system

When `create_system != 3` (UNIX), `external_attr >> 16` is not a Unix mode and
should not be interpreted as permissions.

**Archivey's workaround** (line 201):
```python
mode=stat.S_IMODE(mode) if info.external_attr != 0 else None,
```
Only extracts permissions if `external_attr` is non-zero.  However, it does not
check `create_system`, so DOS-created archives with non-zero `external_attr`
low bits could produce a spurious mode value (the DOS attribute byte in the low
16 bits shifts into the mode field).

### 3.8 `is_encrypted` archive-level flag

`zipfile` does not expose an archive-level "is encrypted" flag.

**Archivey's workaround** (lines 266–268):
```python
"is_encrypted": any(info.flag_bits & 0x1 for info in self._archive.infolist())
```
Scans all members for the encryption flag.

### 3.9 Non-seekable streams (streaming mode)

`zipfile` requires a seekable stream and rejects non-seekable input at
construction.  This is a **library limitation, not a format limitation**.

The ZIP format was designed from the beginning to support streaming writes: each
entry has a 30-byte local file header immediately before its compressed data,
containing the filename, compression method, and flags.  A streaming reader can
process these local headers in order, identical in concept to a TAR reader.

When an archive is written to a pipe or network socket (the "streaming" writing
case), the writer does not know the CRC32 or sizes at the time the local header
is written.  It sets flag bit 3 (`_MASK_USE_DATA_DESCRIPTOR`) and writes the
CRC32, compressed size, and uncompressed size in a trailing data descriptor
record after the compressed data.  Compressed methods (deflate, bzip2, lzma)
have end-of-stream markers so the decompressor knows when the data ends without
needing the size field.  Stored (uncompressed) entries with bit 3 set require
scanning for the `PK\x07\x08` data-descriptor signature, which is ambiguous if
that byte sequence appears in the payload.

**What a streaming ZIP reader would lose** (central-directory-only fields):

| Field | Impact |
|---|---|
| `external_attr` | Unix mode bits lost; symlink detection unavailable |
| `create_system` | Cannot determine if archive was created on Unix |
| Per-file comment | Not available in streaming mode |

Everything else — filename (UTF-8 via flag bit 11), compression method, DOS
timestamp, and the Extended Timestamp extra field (`0x5455`) — is present in
the local file header.

Implementing a streaming `ZipReader` would require parsing local file headers
directly rather than relying on `zipfile`.  The resulting reader would set
`members_list_supported=False` and `streaming_only=True`, analogous to
`TarReader` with a non-seekable compressed stream.

---

## 4. What zipfile handles well (no workaround needed)

| Feature | Status |
|---|---|
| ZIP64 large files (>4 GB) | Automatic via `_decodeExtra` |
| Data descriptor records | Transparent |
| Multi-entry decompression | Each `open()` is independent |
| CRC32 verification | Automatic on `close()` |
| Archive and member comments | Exposed as bytes |
| Zip bomb detection | `_end_offset` overlap check |
| Deflated, BZip2, LZMA | Built-in decompressors |
| Stored (uncompressed) | Direct reads |
| UTF-8 filenames | Automatic via flag bit 11 |
| Thread safety (per-member) | Each `open()` uses a `_SharedFile` slice |

---

## 5. Config options (ZIP-specific)

None.  All ZIP behaviour is hardcoded in the reader.  The global
`extraction_filter` and `overwrite_mode` apply as for all formats.

---

## 6. TarInfo / ZipInfo fields exposed vs hidden

Fields that are in the format but not exposed by `ZipInfo` / archivey:

| Field | In format | Exposed | Notes |
|---|---|---|---|
| Access time | Extra 0x5455 bits 1 | No | Only mtime from central dir extra |
| Creation time | Extra 0x5455 bits 2 | No | Only in local header extra |
| NTFS timestamps | Extra 0x000A | No | stdlib ignores entirely |
| Unix UID/GID | Extra 0x7875 (Info-ZIP Unix3) | No | stdlib ignores |
| Extended attributes | Extra 0x756E (ASi Unix) | No | stdlib ignores |
| AES info | Extra 0x9901 | No | WinZip AES not supported |
| Strong encryption type | flag bit 6 | Via exception | Raises NotImplementedError |
| Internal attributes | `ZipInfo.internal_attr` | Via `extra` dict | Not surfaced as a field |

---

## 7. Known zipfile bugs / quirks

- **DOS timestamp epoch**: year 0 in DOS format maps to 1980, not the Unix
  epoch.  `ZipInfo.date_time = (1980, 0, 0, 0, 0, 0)` is used as a sentinel
  for "no timestamp"; archivey's `get_zipinfo_timestamp()` returns `None` in
  this case (line 47).
- **Invalid dates**: Some archivers write invalid DOS dates (e.g. month 0, day
  0).  Archivey catches `ValueError` from `datetime(*zip_info.date_time)` and
  falls back to `None` (lines 52–56).
- **Trailing slash ambiguity**: `ZipInfo.is_dir()` checks for trailing slash
  only.  A file whose name ends in `/` is treated as a directory even if it has
  content.
- **Multi-disk archives**: Not supported; `BadZipFile` is raised if
  `disk_number_start != 0`.
- **`ZipExtFile` seeking**: Backward seeking in `ZipExtFile` restarts the
  decompressor from scratch (re-reads from the local file header).  This is
  slow but correct.
- **LZMA extra field**: `zipfile`'s LZMA decompressor reads filter properties
  from the first 4 bytes of the compressed data's extra field (2 bytes ID + 2
  bytes properties size).  LZMA-compressed ZIP members from some tools may have
  a slightly different format and fail.

---

## 8. Useful references

- PKWARE APPNOTE.TXT (ZIP format specification): https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
- InfoZIP extra field specification (0x5455, 0x7875, 0x756E, etc.)
- Python stdlib zipfile source: `/usr/lib/python3.11/zipfile.py`
- Current implementation: `src/archivey/formats/zip_reader.py`
