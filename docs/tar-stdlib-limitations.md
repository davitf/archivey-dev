# TAR Reader — stdlib tarfile Limitations and Workarounds

This document analyses what the Python stdlib `tarfile` module provides, where
it falls short, and what archivey does to work around each gap.  Like ZIP, the
goal is not to replace the library — `tarfile` handles the in-process reading.
The analysis focuses on the boundary between library and archivey.

---

## 1. BaseArchiveReader flags for TAR

| Flag | Value | Reason |
|---|---|---|
| `members_list_supported` | `False` | TAR has no central directory; members are found by sequential scan only. |
| `streaming_only` | configurable | True for non-seekable compressed streams (e.g. piped gzip); False when the underlying stream supports `seek()`. |

Unlike ZIP, TAR is an inherently sequential format: each member's header
immediately precedes its data, and there is no end-of-file index.  This means:

- `get_members_if_available()` overrides the base-class implementation to return
  `None` when `streaming_only=True` (line 148–151), because the member list
  cannot be built without scanning the entire archive.
- For seekable archives, `get_members()` still works by iterating to completion.
- `iter_members_for_registration()` is a true generator: each `yield` advances
  the tarfile read position by one header + data block.

---

## 2. TAR format variants

`tarfile` handles three on-disk format families, selected at write time and
auto-detected at read time:

### 2.1 POSIX ustar (`USTAR_FORMAT = 0`, magic `"ustar\x0000"`)

The original POSIX.1-1988 format.  Each header is exactly 512 bytes:

```
name[100]         filename (null-terminated, ASCII)
mode[8]           octal permissions
uid[8]            octal UID
gid[8]            octal GID
size[12]          octal file size (max 8 GB via GNU extension)
mtime[12]         octal Unix timestamp (seconds)
checksum[8]       header checksum
typeflag[1]       0=regular, 1=hardlink, 2=symlink, 3=char, 4=block, 5=dir, 6=fifo
linkname[100]     link target (null-terminated)
magic[6]          "ustar\0"
version[2]        "00"
uname[32]         owner name (ASCII)
gname[32]         group name (ASCII)
devmajor[8]       device major number
devminor[8]       device minor number
prefix[155]       prefix for long paths (joined with "/" to name)
padding[12]
```

**Filename limit**: 100 bytes for `name` + 155 bytes for `prefix`, joined by
`/`.  Filenames exceeding this cannot be stored in ustar without loss.

**Size limit**: The `size` field is 12 octal ASCII digits → max `077777777777`
octal = 8 GB.  Enforced in archiver; `tarfile` reads it without validation.

**No sub-second timestamp**: `mtime` is integer seconds only.

### 2.2 GNU tar (`GNU_FORMAT = 1`, magic `"ustar  \0"`)

Extends ustar with:

- **`GNUTYPE_LONGNAME` (`L`)**: A synthetic header entry whose content is the
  full filename of the next entry (>100 bytes).
- **`GNUTYPE_LONGLINK` (`K`)**: Same for symlink targets (>100 bytes).
- **`GNUTYPE_SPARSE` (`S`)**: Sparse file descriptor with hole/data map.
- **Numeric encoding for large UID/GID/size**: values > octal max are stored as
  big-endian binary with a leading `0x80` byte.

`tarfile` reads GNU long names/links transparently (`_proc_gnulong`): the
content of the `L`/`K` entry replaces the truncated `name`/`linkname` field.

### 2.3 PAX (POSIX.1-2001, `PAX_FORMAT = 2`)

The modern standard.  Extends ustar with **extended header** entries:

- **`XHDTYPE` (`x`)**: Per-file extended header.  Content is UTF-8 key=value
  pairs (`%d %s=%s\n` format).
- **`XGLTYPE` (`g`)**: Global extended header applying to all subsequent entries.

PAX fields that override the base header (`PAX_FIELDS`):
`path`, `linkpath`, `size`, `mtime`, `atime`, `ctime`, `uid`, `gid`, `uname`,
`gname`, `hdrcharset`, `comment`, `charset`.

Key improvements over ustar:
- **Arbitrary length filenames and symlink targets** (via `path`/`linkpath`).
- **Sub-second timestamps**: `mtime` stored as a decimal fraction (e.g.
  `1234567890.123456789`).
- **Large UIDs/GIDs** (> 2^21).
- **Unicode filenames** (always UTF-8 in PAX headers).
- **Arbitrary metadata** via non-standard `SCHILY.*` or `LIBARCHIVE.*` prefixes.

`tarfile` reads PAX headers transparently via `_proc_paxheaders` and stores
parsed key-value pairs in `TarInfo.pax_headers`.

---

## 3. What tarfile provides

### 3.1 TarFile open modes

`tarfile.open(name, fileobj, mode, errorlevel)`:

| Mode | Meaning |
|---|---|
| `"r:"` | Read, no compression, seekable required |
| `"r:\*"` | Read, auto-detect compression, seekable preferred |
| `"r|"` | Read streaming (pipe), no compression |
| `"r|gz"` | Read streaming, gzip compressed |
| `"r|bz2"` | Read streaming, bzip2 compressed |
| `"r|xz"` | Read streaming, xz compressed |

**Key distinction**: `r:` mode calls `getmembers()` lazily (advances on each
`next()` call); `r|` mode uses `_Stream` (an internal wrapper) which cannot
seek backward.

Archivey always uses:
- `"r:"` for seekable streams (random access supported)
- `"r|"` for streaming (one-pass only)

`errorlevel=2` makes tarfile raise exceptions on any error instead of silently
continuing.

### 3.2 TarInfo metadata fields

| Field | Type | Notes |
|---|---|---|
| `name` | str | filename; PAX overrides with Unicode version |
| `size` | int | uncompressed size in bytes |
| `mtime` | float | Unix timestamp (seconds); float for PAX sub-second |
| `mode` | int | Unix mode bits (type + permissions) |
| `type` | bytes | entry type: REGTYPE, LNKTYPE, SYMTYPE, DIRTYPE, etc. |
| `linkname` | str | symlink target or hardlink target |
| `uid`, `gid` | int | owner numeric IDs |
| `uname`, `gname` | str | owner name strings |
| `devmajor`, `devminor` | int | device numbers for CHRTYPE/BLKTYPE |
| `pax_headers` | dict | PAX extended header key-value pairs |
| `sparse` | list | sparse file segment list (GNU sparse) |
| `offset` | int | byte offset of this header in the archive file |
| `offset_data` | int | byte offset of the file data |

Archivey uses all of these except `sparse`, `devmajor`/`devminor` (stored in
`extra` dict only), and `pax_headers` (not exposed).

### 3.3 `extractfile()` — opening a member

`TarFile.extractfile(tarinfo)` returns:

- A `tarfile.ExFileObject` (a `_FileInFile` wrapper) for regular files and
  CONTTYPE entries.
- `None` for directories, symlinks, hardlinks, device files, FIFOs.
- `None` for any entry where `is_file()` is False.

`ExFileObject` supports `read(size)`, `readline()`, `readlines()`, `tell()`,
and `seek()`.  It reads the data section of the entry directly from the
underlying archive file at `offset_data`.

**Seeking in streaming mode**: In `r|` mode, the underlying stream is non-
seekable.  `ExFileObject.seek()` will raise or silently produce wrong results.
Archivey never calls `open()` (random access) in streaming mode; the base
class's `_prepare_member_for_open` raises `ValueError` if `for_iteration=False`
and `streaming_only=True` (lines 243–248 of `tar_reader.py`).

### 3.4 Iteration

`for tarinfo in TarFile` calls `TarFile.next()` on each iteration, which:

1. Reads the next 512-byte block.
2. Detects `GNUTYPE_LONGNAME`/`GNUTYPE_LONGLINK`/`XHDTYPE`/`XGLTYPE` and
   processes them before returning the actual entry.
3. Returns `None` at end-of-archive (two consecutive zero blocks).
4. Skips unknown types with `errorlevel < 2`.

With `errorlevel=2` (archivey's setting), unknown block types raise `ReadError`.

---

## 4. Limitations and workarounds

### 4.1 No central directory → `members_list_supported=False`

TAR has no index; to get a complete member list, the archive must be scanned
from start to finish.

`iter_members_for_registration()` is a plain generator over `for tarinfo in self._archive`.
Members are registered one at a time as they are yielded.

**Implication**: The first call to `get_members()` on an unseekable TAR reads
and discards all data.  Subsequent calls reuse the cached `_members` list.  For
large compressed TARs over a network pipe this is the only option.

### 4.2 Silent corruption at end of archive — `tar_check_integrity` workaround

**The problem**: When `tarfile` encounters a block it cannot parse as a valid
header — e.g. a zero block in the middle of a corrupted archive — it raises a
`HeaderError` subclass.  With `errorlevel=2` this becomes a `ReadError`, but
**only if the bad block follows a valid header**.  If the archive is truncated
partway through the final member's data, `tarfile` can silently stop iterating
and return only the members read so far, treating the truncation as end-of-archive.

The TAR end-of-archive marker is **two consecutive 512-byte zero blocks**.
tarfile stops when it sees the first zero block (it may or may not check for the
second one in different Python versions).

**Archivey's workaround** (`_check_tar_integrity()`, lines 198–238): after
`iter_members_for_registration()` finishes (all `TarInfo` objects yielded),
archivey manually seeks to just after the last member's data, reads 1024 bytes,
and verifies they are all zeros.  If not (short read, non-zero data), an
`ArchiveCorruptedError` is raised.

Controlled by `config.tar_check_integrity` (default `True`).  For non-seekable
streams, the check uses `self._archive.fileobj.tell()` (the internal position
tracked by tarfile's `_Stream` wrapper) to compute how far to skip forward
before reading the terminal blocks.

### 4.3 `tarfile.read()` short-read failure

**The problem**: Inside `tarfile._FileInFile.read()`, the code calls
`self.fileobj.read(size)` and assumes it gets exactly `size` bytes.  If the
underlying file object's `read()` returns fewer bytes (a legal Python I/O
behaviour), tarfile raises a confusing error or reads corrupt data.

This can happen with `GzipFile`, `BZ2File`, and similar decompressor wrappers
that are not `BufferedReader`-wrapped — they sometimes return partial chunks.

**Archivey's workaround** (lines 90–94): `ensure_bufferedio()` wraps the
decompressed stream in `io.BufferedReader` before passing it to `tarfile.open()`.
`BufferedReader.read(n)` always returns exactly `n` bytes (or raises on EOF).

### 4.4 `GzipFile.seekable()` lies

**The problem**: `gzip.GzipFile.seekable()` returns `True` even when the
underlying file object is a pipe (non-seekable), because it implements `seek()`
by reading and discarding data.  tarfile's `"r:"` mode checks `seekable()` to
decide whether random access is available.

**Archivey's workaround** (`open_gzip_stream()`, lines 123–132 of
`compressed_streams.py`): if the underlying stream is not seekable, the
`seekable` attribute of the `GzipFile` is monkey-patched to a lambda returning
`False`, and `seek` is replaced with a function that raises
`io.UnsupportedOperation`.

### 4.5 Compressed TAR is architecturally solid

A compressed TAR (`.tar.gz`, `.tar.bz2`, `.tar.xz`, etc.) is a single-stream
archive — all members are compressed together as one stream.

In archivey, `is_solid` is set to `True` for compressed TARs (line 282–283):
```python
is_solid=format.stream is not None and format.stream != StreamFormat.UNCOMPRESSED,
```

**Implication**: Random access to individual members in a compressed TAR
requires decompressing from the beginning up to the member's offset.  This is
O(member_offset) per access.  For a 1 GB `.tar.gz`, accessing the last member
requires reading the entire compressed stream.

**Alternative backends for random access**: The `ArchiveyConfig` offers
drop-in replacements for the stdlib decompressors that support random access:

| Format | Default | Alternative (`config.use_X=True`) | Random access |
|---|---|---|---|
| `.tar.gz` | `gzip.GzipFile` | `rapidgzip` | Yes (index-based) |
| `.tar.bz2` | `bz2.BZ2File` | `indexed_bzip2` | Yes (index-based) |
| `.tar.xz` | `lzma.open` | `python-xz` | Yes |
| `.tar.zst` | `pyzstd.open` | `zstandard` (via `ZstandardReopenOnBackwardsSeekIO`) | Reopen from start on backward seek |

### 4.6 Zstandard backward seeking — `ZstandardReopenOnBackwardsSeekIO`

**The problem**: `zstandard.open()` raises `OSError("cannot seek zstd
decompression stream backwards")` on backward seeks.  For a seekable `.tar.zst`,
tarfile needs to seek backward when re-opening a member (random access mode).

**Archivey's workaround** (`ZstandardReopenOnBackwardsSeekIO`, lines 267–329 of
`compressed_streams.py`): a wrapper that catches backward-seek `OSError`,
re-opens the underlying `zstandard.open()` from position 0, and re-seeks forward
to the target position.  For large archives this can be slow; it is essentially
O(target_offset) per backward seek.

### 4.7 Encoding issues

**The problem**: TAR header names are fixed-width byte arrays.

- **ustar**: ASCII-only; non-ASCII names are encoded with the `encoding` and
  `errors` parameters.  Default encoding is `sys.getfilesystemencoding()` (often
  UTF-8 on Linux, but not always).
- **GNU format**: Same as ustar for the base header; long names via `L`/`K`
  entries are raw bytes decoded with the configured encoding.
- **PAX format**: `path` and `linkpath` in extended headers are always UTF-8;
  non-name fields use the configured encoding.  `hdrcharset` field can override
  encoding for the name fields within a PAX header.

Archivey opens tarfile with the default encoding (UTF-8 on modern systems) and
`errors="surrogateescape"` (tarfile's default).  Names that cannot be decoded in
UTF-8 will contain surrogate escape characters, which Python will round-trip
through most operations but cannot be written to some filesystems.

**Exposed in `get_archive_info()`** (line 288):
```python
"encoding": self._archive.encoding
```

### 4.8 Hardlinks vs symlinks

Both are natively represented in TAR:
- `LNKTYPE = b"1"`: hardlink, `linkname` = target filename within the archive.
- `SYMTYPE = b"2"`: symbolic link, `linkname` = symlink target path.

`tarfile` exposes both via `TarInfo.islnk()` and `TarInfo.issym()`.

Archivey maps:
- `islnk()` → `MemberType.HARDLINK`, `link_target = info.linkname`
- `issym()` → `MemberType.SYMLINK`, `link_target = info.linkname`

The base class handles hardlink resolution via `resolve_link()` — it looks up
the target filename in the registered member table by `member_id`.

**Limitation in streaming mode**: When `streaming_only=True`, hardlink targets
may not have been yielded yet (if the target appears later in the archive), so
link resolution may fail.  Links that cannot be resolved return `None` from
`resolve_link()`.

### 4.9 Special file types

TAR stores device files, FIFOs, and contiguous files:

| Type byte | Name | `tarfile` exposes | Archivey maps to |
|---|---|---|---|
| `3` | Character device | `ischr()`, `devmajor`, `devminor` | `MemberType.OTHER` |
| `4` | Block device | `isblk()`, `devmajor`, `devminor` | `MemberType.OTHER` |
| `6` | FIFO | `isfifo()` | `MemberType.OTHER` |
| `7` | Contiguous file | — (treated as regular) | `MemberType.FILE` |

`devmajor` and `devminor` are stored in `ArchiveMember.extra` but not in a
dedicated field.

### 4.10 Sparse files

GNU sparse files (`GNUTYPE_SPARSE`) store a list of `(offset, size)` pairs
describing non-zero regions.  `tarfile` reads the sparse map into
`TarInfo.sparse`, but `extractfile()` does not reconstruct the sparse layout —
it returns the raw (packed) data without the holes.  The caller would need to
use `TarInfo.sparse` to reconstruct the sparse file correctly.

Archivey does not handle sparse files specially; they are treated as regular
files and their `file_size` reflects the logical (sparse) size while the data
stream contains only the non-hole data.  This may produce incorrect output for
sparse files.

### 4.11 The Python 3.12+ `filter` parameter — security implications

Python 3.12 introduced `TarFile.extractall(filter=...)` and
`TarFile.extract(filter=...)` with three built-in security filters:

| Filter name | What it blocks |
|---|---|
| `"fully_trusted"` | Nothing; extracts everything |
| `"tar"` | Absolute paths, `..` components |
| `"data"` | All of above + special files, setuid bits, hardlinks outside dest |

Python 3.12 emits a `DeprecationWarning` if no `filter` is specified (defaults
to `"fully_trusted"`).  Python 3.14 will change the default to `"data"`.

**Archivey's stance**: archivey uses `tarfile` only for reading member metadata
and opening individual file streams via `extractfile()`, not for direct
extraction to the filesystem via `extract()` or `extractall()`.  The `filter`
parameter does not apply to `extractfile()`.

However, the `ArchiveyConfig.extraction_filter` setting serves a similar
purpose for archivey's own extraction path: `ExtractionFilter.DATA` (the default)
applies tarfile-like security rules within archivey's extraction logic.

### 4.12 No CRC or hash verification

TAR files do not store a checksum per-member (only a header checksum for error
detection, not data integrity).  `tarfile` verifies the header checksum on read
but there is no equivalent for the member data.

Archivey sets `crc32=None` for all TAR members.

### 4.13 Timestamp timezone

`TarInfo.mtime` is a Unix timestamp (float for PAX sub-second precision, int for
ustar/GNU).  Unix timestamps are UTC by definition.

Archivey converts correctly (line 166):
```python
mtime_with_tz=datetime.fromtimestamp(info.mtime, tz=timezone.utc)
```

This is simpler and more correct than ZIP (which needs the extended timestamp
extra field).

---

## 5. Compressed stream handling

For compressed TARs, the decompression is handled **outside** tarfile by
`open_stream()` in `compressed_streams.py`.  The returned stream is passed to
`tarfile.open(fileobj=stream, mode="r:")` (or `"r|"` for non-seekable).

This separation means archivey can swap the decompressor (e.g. stdlib gzip vs
rapidgzip) without touching any tarfile logic.

### 5.1 Stream format table

| Extension | Format | Default backend | Alternative |
|---|---|---|---|
| `.tar.gz` / `.tgz` | gzip | `gzip.GzipFile` | `rapidgzip.open` |
| `.tar.bz2` / `.tbz2` | bzip2 | `bz2.open` | `indexed_bzip2.open` |
| `.tar.xz` / `.txz` | xz/LZMA | `lzma.open` | `xz.open` (python-xz) |
| `.tar.zst` | zstandard | `pyzstd.open` | `zstandard.open` + reopen-on-backward-seek |
| `.tar.lz4` | LZ4 | `lz4.frame.open` | — |
| `.tar.lz` | lzip | `LzipDecompressorStream` | — |
| `.tar.zz` | zlib/deflate | `ZlibDecompressorStream` | — |
| `.tar.br` | brotli | `BrotliDecompressorStream` | — |
| `.tar.Z` | LZW (Unix compress) | `uncompresspy.LZWFile` | — |

### 5.2 The `DecompressorStream` base class

Many of the non-stdlib decompressors that don't support seeking are wrapped in
`DecompressorStream` (`compressed_streams.py`, lines 420–565), which provides:

- **Forward seeking**: reads and discards data up to the target position.
- **Backward seeking**: calls `_rewind()` which seeks the underlying raw stream
  to 0 and recreates the decompressor, then reads forward.
- **SEEK_END**: reads to the end to find the size, then seeks backward.
- **EOF detection**: raises `ArchiveEOFError` if the decompressor ends before
  expected.

This makes these streams usable as seekable `fileobj` for `tarfile.open(mode="r:")`.

---

## 6. Config options (TAR-specific)

| Option | Default | Effect |
|---|---|---|
| `tar_check_integrity` | `True` | After iterating all members, verify the two-zero-block EOF marker. Raises `ArchiveCorruptedError` on truncated archives that tarfile would accept silently. |
| `use_rapidgzip` | `False` | Use `rapidgzip` instead of `gzip.GzipFile` for `.tar.gz`. Enables parallel decompression and index-based random access. |
| `use_indexed_bzip2` | `False` | Use `indexed_bzip2` instead of `bz2` for `.tar.bz2`. Enables parallel decompression and index-based random access. |
| `use_python_xz` | `False` | Use `python-xz` instead of `lzma.open` for `.tar.xz`. Enables random access. |
| `use_zstandard` | `False` | Use `zstandard` instead of `pyzstd` for `.tar.zst`. Uses `ZstandardReopenOnBackwardsSeekIO` wrapper; slightly worse error reporting. |

---

## 7. TarInfo fields: exposed vs not exposed in archivey

| `TarInfo` field | Archivey field | Notes |
|---|---|---|
| `name` | `filename` | trailing `/` added for dirs |
| `size` | `file_size` | logical size (may differ from data length for sparse) |
| `mtime` | `mtime_with_tz` | UTC-aware datetime |
| `mode` | `mode` | Unix permission bits via `stat.S_IMODE(info.mode)` |
| `uid` | `uid` | 0 is stored as `None` (considered "unset") |
| `gid` | `gid` | 0 is stored as `None` |
| `uname` | `uname` | empty string stored as `None` |
| `gname` | `gname` | empty string stored as `None` |
| `linkname` | `link_target` | for SYMTYPE and LNKTYPE only |
| `type` | `extra["type"]` | raw type byte |
| `devmajor`, `devminor` | `extra["devmajor/devminor"]` | not a dedicated field |
| `pax_headers` | not exposed | PAX key-value pairs discarded |
| `sparse` | not exposed | sparse map discarded |
| `offset`, `offset_data` | not exposed | internal tarfile fields |
| `compress_size` | `None` | TAR doesn't store compressed size per-member |
| `crc32` | `None` | TAR doesn't store data checksums |

---

## 8. Useful references

- GNU tar format documentation: `info tar` → "Internals" chapter
- POSIX.1-2001 pax interchange format: IEEE Std 1003.1-2001
- Python stdlib tarfile source: `/usr/lib/python3.11/tarfile.py`
- Current implementation: `src/archivey/formats/tar_reader.py`
- Compressed stream wrappers: `src/archivey/formats/compressed_streams.py`
- PEP 706 (tarfile extraction filters): https://peps.python.org/pep-0706/
