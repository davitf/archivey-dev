# ISO Reader — pycdlib Analysis and Implementation Guide

This document analyses the ISO9660 format, what pycdlib provides, where it falls
short, and what an `IsoReader` implementation must handle itself.  Unlike the ZIP
and TAR docs — which address limitations of a library already in use — this
document also covers the implementation decision (pycdlib vs native) because no
reader exists yet.  The goal is to give a developer enough context to write an
`IsoReader` from scratch.

---

## 1. Current state of ISO support in archivey

| Item | Status |
|---|---|
| `ContainerFormat.ISO` / `ArchiveFormat.ISO` | Defined in `src/archivey/types.py` |
| Magic-byte detection (offset `0x8001`) | Implemented in `src/archivey/formats/format_detection.py` |
| Extension mapping | Only `.iso`; see section 9 for others |
| `_FORMAT_TO_READER` entry in `src/archivey/core.py` | **Missing** — opening an ISO raises `ArchiveNotSupportedError` |
| `IsoReader` class | **Does not exist** |
| pycdlib in dependency checker | Declared in `src/archivey/internal/dependency_checker.py` (`pycdlib_version`), not used at runtime |
| Test archive creation | `create_iso_archive_with_pycdlib` and `create_iso_archive_with_genisoimage` in `tests/archivey/create_archives.py` — both working, ISO filenames commented out of `SKIP_TEST_FILENAMES` |

---

## 2. BaseArchiveReader flags for ISO

| Flag | Value | Reason |
|---|---|---|
| `members_list_supported` | `True` | ISO9660 has a root directory record; a full recursive walk produces the complete member list before any file data is read.  Unlike TAR there is no need to scan data blocks to discover members. |
| `streaming_only` | `False` | ISO is sector-addressed; every file has a fixed `extent_location` (LBA).  Random access to any file requires only a `seek()` to `LBA * 2048`.  There is no sequential dependency between members. |

---

## 3. ISO9660 format essentials

### 3.1 On-disk layout

An ISO image is a sequence of 2048-byte sectors (logical blocks).  The first 16
sectors (bytes 0–32767) are the **System Area** — reserved for bootloaders and
left undefined by ISO9660.

The Volume Descriptor chain begins at **sector 16** (offset `0x8000`):

| Type | Name | Notes |
|---|---|---|
| 1 | Primary Volume Descriptor (PVD) | Always present; defines root directory record, volume name, dates |
| 2 | Supplementary Volume Descriptor (SVD) | Joliet uses this; same structure as PVD but with UTF-16 BE names |
| 3 | Volume Partition Descriptor | Rarely used |
| 255 | Volume Descriptor Set Terminator | End of VD chain |

UDF has a separate recognition area at sector 16+, parallel to the ISO9660 chain.

Archivey's magic-byte check reads 5 bytes at offset `0x8001` (one byte past the
start of sector 16) and checks for `CD001`, `CDW02`, `BEA01`, `NSR02`, `NSR03`,
`TEA01`, or `BOOT2` — the same list pycdlib uses internally.

### 3.2 Directory Record (DR) structure

Each file or directory in an ISO9660 directory extent is described by a Directory
Record.  pycdlib exposes these as `dr.DirectoryRecord` objects.

| Field | Type | Notes |
|---|---|---|
| `dr_len` | uint8 | Length of this DR including System Use area |
| `xattr_len` | uint8 | Extended attribute record length (usually 0) |
| `orig_extent_loc` | uint32 LE + BE | LBA of file data start |
| `data_length` | uint32 LE + BE | File size in bytes |
| `date` | `DirectoryRecordDate` | 7-byte compact date; see section 3.3 |
| `file_flags` | uint8 | Bit 1 = directory; bit 7 = multi-extent |
| `file_unit_size` | uint8 | Interleaved files only (usually 0) |
| `interleave_gap_size` | uint8 | Interleaved files only (usually 0) |
| `seqnum` | uint16 | Volume sequence number |
| `len_fi` | uint8 | Length of the file identifier |
| `file_ident` | bytes | Filename; even-padded |

Key methods: `is_dir()`, `is_file()`, `is_symlink()` (requires Rock Ridge),
`is_dot()`, `is_dotdot()`, `file_identifier()`.

### 3.3 DirectoryRecordDate (7 bytes)

Defined in `pycdlib/dates.py`:

```python
class DirectoryRecordDate:
    __slots__ = ('_initialized', 'years_since_1900', 'month', 'day_of_month',
                 'hour', 'minute', 'second', 'gmtoffset')
    # gmtoffset: signed int8, units of 15 minutes
```

This is **not** a Python `datetime`.  To convert to a UTC-aware `datetime`:

```python
from datetime import datetime, timezone, timedelta

def dr_date_to_datetime(d):
    year = 1900 + d.years_since_1900
    tz = timezone(timedelta(minutes=d.gmtoffset * 15))
    return datetime(year, d.month, d.day_of_month,
                    d.hour, d.minute, d.second, tzinfo=tz)
```

The `gmtoffset` field is present in every Directory Record date, so timestamps
are always timezone-aware.  This is better than DOS ZIP timestamps (which are
naive local time) and comparable to PAX TAR timestamps.

The Volume Descriptor uses a different, longer date format (`VolumeDescriptorDate`
in pycdlib) with hundredths-of-a-second precision and a separate `gmtoffset`
byte.

---

## 4. Extension namespaces — the core pycdlib complexity

pycdlib has **four separate namespace systems**.  Every key API call (`walk()`,
`list_children()`, `get_record()`, `open_file_from_iso()`) accepts `**kwargs`
and requires **exactly one** of:

| Kwarg | Namespace | Filename rules |
|---|---|---|
| `iso_path=` | Plain ISO9660 | Uppercase A–Z 0–9 `_`; files have `;1` version suffix (`README.TXT;1`); interchange level 3 relaxes charset but `;1` still applies |
| `rr_path=` | Rock Ridge (POSIX extension) | Case-sensitive, arbitrary filenames, full UTF-8 |
| `joliet_path=` | Joliet (Microsoft Unicode) | UTF-16 BE; max 64 characters per component; no `;1` suffix |
| `udf_path=` | UDF (DVD/Blu-ray) | Separate filesystem entirely |

An ISO can have **none, one, two, or all three** of these extensions
simultaneously.  The test-creation code in archivey demonstrates the authoring
side:

```python
iso.new(interchange_level=3, rock_ridge="1.09", joliet=3)
# ...
iso.add_file(src_path,
             iso_path=iso_path.upper(),   # plain ISO9660
             rr_name=f_name,              # Rock Ridge
             joliet_path=iso_path)        # Joliet
```

For reading, you pick **one preferred namespace** and walk with it.

### 4.1 Detecting which namespaces are present

```python
iso = pycdlib.pycdlib.PyCdlib()
iso.open(path)

has_rr     = bool(iso.rock_ridge)     # non-empty string if RR present, e.g. "1.09"
has_joliet = iso.joliet_vd is not None
has_udf    = iso.udf_root is not None
```

Note: `iso.rock_ridge` is a string (the RR version), not a boolean.  An empty
string means no Rock Ridge.

### 4.2 Recommended namespace priority for reading

```
Rock Ridge  →  Joliet  →  plain ISO9660
```

- **Rock Ridge** is preferred because it preserves case, full POSIX metadata
  (permissions, UID/GID, symlinks), and arbitrary filenames.
- **Joliet** is preferred over plain ISO9660 because it preserves mixed-case
  Unicode names.
- **Plain ISO9660** is the fallback; filenames will be uppercase and `;1`-suffixed.
- **UDF** is a separate, more complex filesystem (different record types, different
  iteration pattern).  It can be deferred to a later implementation milestone.

### 4.3 Stripping the `;1` version suffix

When using `iso_path=` the filenames returned by `walk()` include the version
suffix (`;1`).  These must be stripped before storing in `ArchiveMember.filename`:

```python
def strip_version(name: str) -> str:
    if name.endswith(';1'):
        return name[:-2]
    return name
```

---

## 5. pycdlib API walk patterns

### 5.1 `walk()` — yields `(path, dirs, files)`

```python
# Rock Ridge walk (preferred)
for dirpath, dirnames, filenames in iso.walk(rr_path="/"):
    for fname in filenames:
        full_path = dirpath.rstrip("/") + "/" + fname
        record = iso.get_record(rr_path=full_path)
        # record is a dr.DirectoryRecord
```

`walk()` yields only **names** (strings), not records.  To get metadata, call
`iso.get_record()` with the full path for each entry.  This is a two-step
process.

For Joliet the path separator and namespace kwarg change but the pattern is
identical.  For plain ISO9660, strip `;1` from filenames after the walk.

### 5.2 `open_file_from_iso()` — returns a `PyCdlibIO`

```python
with iso.open_file_from_iso(rr_path="/path/to/file") as f:
    data = f.read()
```

`PyCdlibIO` is defined in `pycdlib/pycdlibio.py` and inherits from
`io.RawIOBase`.  It implements `read()`, `readinto()`, `seek()`, `tell()`,
`readable()`, and `seekable()`.

**Important**: `PyCdlibIO` is a context manager and a raw IO object, but it is
**not** a `BufferedIOBase`.  `read(n)` may not return exactly `n` bytes in one
call.  Wrap it in `io.BufferedReader` before passing to archivey callers:

```python
raw = iso.open_file_from_iso(rr_path=full_path)
raw.__enter__()
return io.BufferedReader(raw)
```

The `__enter__` call is required because `PyCdlibIO` uses a context manager to
bind the internal `_fp` file pointer.  Without it, `read()` will fail with an
`AttributeError`.

### 5.3 `get_record()` — returns a `dr.DirectoryRecord` (or `udf.UDFFileEntry` for UDF)

```python
record = iso.get_record(rr_path="/path/to/file")
# record.data_length      — file size
# record.date             — DirectoryRecordDate
# record.rock_ridge       — RockRidge object (or None if no RR)
# record.is_dir()
# record.is_file()
# record.is_symlink()     — True only if RR is present and SL entry exists
```

---

## 6. Rock Ridge extension details

Rock Ridge stores POSIX metadata in System Use fields appended after the filename
in each Directory Record.  pycdlib parses these automatically; the parsed object
is accessible via `dr_record.rock_ridge`.

Relevant System Use Entry types:

| SU type | pycdlib accessor | Content |
|---|---|---|
| `PX` | `rock_ridge.posix_file_mode`, `.posix_file_links`, `.posix_uid`, `.posix_gid` | POSIX permissions, link count, UID, GID |
| `TF` | `rock_ridge.access_time`, `.modify_time`, `.attribute_time`, `.creation_time` | Timestamps (each is a `DirectoryRecordDate`) |
| `SL` | `rock_ridge.symlink_path()` | Symlink target as bytes |
| `NM` | `rock_ridge.name()` | Alternate (case-preserving) filename as bytes |
| `CL` / `PL` / `RE` | — | Deep directory relocation; handled internally by pycdlib, safe to ignore for reading |
| `SF` | — | Sparse file descriptor; uncommon |

Notes:

- `rock_ridge.name()` returns `bytes`.  Decode as UTF-8 for most modern ISOs.
- `rock_ridge.symlink_path()` also returns `bytes`.  Decode as UTF-8.
- `rock_ridge.modify_time` is the best mtime source when RR is present.  It is a
  `DirectoryRecordDate` and must be manually converted (see section 3.3).
- If `rock_ridge` is `None` on a record despite the ISO having RR, fall back to
  the DR's own `date` field.

---

## 7. Mapping ISO metadata to ArchiveMember

| `ArchiveMember` field | ISO source | Notes |
|---|---|---|
| `filename` | RR `name()` → Joliet UTF-16 → ISO9660 (strip `;1`) | Priority in section 4.2; decode bytes with UTF-8 |
| `file_size` | `DR.data_length` | Always available |
| `compress_size` | Same as `file_size` | No compression in ISO9660 |
| `mtime_with_tz` | RR `rock_ridge.modify_time` → `DR.date` | Both are `DirectoryRecordDate`; convert with `dr_date_to_datetime()` |
| `type` | `DR.is_dir()` / `DR.is_symlink()` / `DR.is_file()` | Symlinks only with Rock Ridge |
| `mode` | `rock_ridge.posix_file_mode` | `None` if no Rock Ridge; use `stat.S_IMODE()` to strip type bits |
| `uid` | `rock_ridge.posix_uid` | `None` if no Rock Ridge |
| `gid` | `rock_ridge.posix_gid` | `None` if no Rock Ridge |
| `link_target` | `rock_ridge.symlink_path().decode("utf-8")` | `None` if no Rock Ridge or not a symlink |
| `compression_method` | `"stored"` | ISO9660 has no per-file compression |
| `crc32` | Not stored in ISO | `None` |
| `encrypted` | `False` | ISO9660 has no encryption |
| `comment` | None | No per-file comments in ISO9660 |
| `raw_info` | the `dr.DirectoryRecord` | Store for use in `_open_member()` |

Directory member filenames should have a trailing `/` appended (archivey
convention for all formats).

---

## 8. Archive-level metadata (ArchiveInfo)

From the Primary Volume Descriptor:

| `ArchiveInfo` field | Source |
|---|---|
| `format` | `ArchiveFormat.ISO` |
| `version` | `"ISO9660"` or the interchange level as a string (`"1"`, `"2"`, `"3"`) |
| `is_solid` | `False` — each file is stored at an independent extent; random access is always O(1) |
| `comment` | PVD `volume_identifier` (32-byte space-padded ASCII string), stripped |
| `extra` | Dict with `system_identifier`, `volume_set_identifier`, `publisher_identifier`, `creation_date`, `modification_date`; also `rock_ridge` version if present, `joliet` bool, `udf` bool |

The PVD is accessible via `iso.pvd` in pycdlib.  Volume identifier:
```python
pvd = iso.pvd
comment = pvd.volume_identifier.decode("ascii", errors="replace").rstrip()
```

---

## 9. Extension and filename detection

`EXTENSION_TO_FORMAT` in `src/archivey/formats/format_detection.py` currently
registers only `.iso`.  Other common ISO image extensions:

| Extension | Notes | Safe to add? |
|---|---|---|
| `.img` | Genuinely ambiguous — could be raw floppy, hard disk partition image, or CD-ROM image | **No** — rely on magic bytes only |
| `.bin` | Raw CD image, typically paired with `.cue`; the `.bin` file itself is usually ISO9660 | Risky without `.cue` context |
| `.nrg` | Nero Burning ROM — proprietary wrapper with its own header around ISO9660 | **No** — different format |
| `.mdf` | Alcohol 120% — another proprietary wrapper | **No** — different format |
| `.dmg` | macOS disk image — typically HFS+/APFS, not ISO9660 | **No** |

**Recommendation**: Do not add any of these to `EXTENSION_TO_FORMAT`.  The
magic-byte check at offset `0x8001` is already implemented and handles true
ISO9660 images regardless of extension.  Files that pass magic detection get
`ArchiveFormat.ISO` regardless of filename.

---

## 10. pycdlib API awkwardness summary

For each pain point, the implementation approach is noted.

| Pain point | Mitigation |
|---|---|
| Namespace selection required before every API call | Detect `rock_ridge`/`joliet_vd`/`udf_root` on open; store as instance variable; use a single `_walk_kwarg` dict like `{"rr_path": "/"}` throughout |
| `**kwargs` API, no typed path parameter | Helper: `def _rr(path)` returns `{"rr_path": path}`, then `iso.walk(**_rr("/"))` |
| No "best name" helper | Implement `_best_name(dr_record, walk_name)` that checks `rock_ridge.name()`, falls back to `walk_name` |
| ISO9660 `;1` suffix | `strip_version()` helper (see section 4.3) |
| `PyCdlibIO` is `RawIOBase`, not `BufferedIOBase` | Wrap in `io.BufferedReader` in `_open_member()` |
| Must call `__enter__` before reading | Call `raw.__enter__()` explicitly; call `raw.__exit__(None, None, None)` in the stream's `close()` |
| Two-pass walk: `walk()` gives names, `get_record()` gives metadata | Unavoidable with pycdlib; the per-file overhead is small for most ISOs |
| `DirectoryRecordDate` is not `datetime` | `dr_date_to_datetime()` helper (see section 3.3) |
| UDF uses `udf.UDFFileEntry`, not `dr.DirectoryRecord` | Defer UDF to a separate implementation milestone |
| pycdlib may report wrong RR if `ER` record absent | pycdlib resets `rock_ridge` to `""` if it doesn't see the RRIP `ER` system use entry (line 1257–1258 of `pycdlib.py`); no workaround needed — it handles this internally |

---

## 11. Native reader alternative

ISO9660 is specified in Ecma-119 (freely available).  A native reader without
pycdlib is feasible at roughly 350–450 lines:

```
1. Seek to sector 16 (offset 32768); read VDs until type 255
2. Pick PVD; note Joliet SVD and UDF if present
3. Recursively read directory records from pvd.root_directory_extent_location
4. For each DR: if file, record (extent_loc, data_length) for later opening
5. Open file: seek(extent_loc * 2048), read(data_length)
```

Rock Ridge parsing requires walking the System Use fields after the filename in
each DR and interpreting `PX`, `TF`, `SL`, `NM` entries.  The main complication
is **Continuation Areas** (`CE` entries): some DRs overflow into a separate
sector, requiring an additional seek.  pycdlib handles this; a native reader must
too.

Multi-extent files (bit 7 of `file_flags`) store a single logical file across
multiple non-contiguous extents.  These are uncommon but must be handled
correctly; they require chaining multiple reads.

**Comparison**:

| Criterion | pycdlib | Native |
|---|---|---|
| Lines of implementation code | ~150 (wrapping) | ~400 (full) |
| Rock Ridge / CE areas | Handled by pycdlib | Must implement |
| Multi-extent files | Handled by pycdlib | Must implement |
| UDF | Handled by pycdlib (could skip) | Not feasible without significant effort |
| El Torito boot records | Handled by pycdlib | Can skip (no member data) |
| New dependency | `pycdlib` | None |
| pycdlib authoring complexity leaking into reader | Yes | No |

**Recommendation**: Use pycdlib for the initial implementation.  It handles all
edge cases out of the box, and archivey already declares it as a dependency.  A
native reader can be considered later if pycdlib causes runtime issues (it has
occasional bugs in its RR parser for unusual discs) or if the dependency weight
becomes a concern.

---

## 12. Implementation checklist

The following steps are required to add a working `IsoReader`:

1. **Create `src/archivey/formats/iso_reader.py`** inheriting from
   `BaseArchiveReader` with `members_list_supported=True`, `streaming_only=False`.

2. **Constructor**: `pycdlib.pycdlib.PyCdlib().open(path)` or
   `PyCdlib().open_fp(stream)`.  Detect `has_rr`, `has_joliet`, `has_udf`.
   Choose the preferred namespace kwarg; store as `self._ns`.

3. **`iter_members_for_registration()`**: Call `iso.walk(**{ns: "/"})`.  For each
   `(dirpath, dirnames, filenames)`:
   - Yield a directory member for `dirpath` (skip the root `/` itself).
   - For each filename, call `iso.get_record(**{ns: full_path})` and convert to
     `ArchiveMember`.

4. **`_open_member(member)`**: Call
   `iso.open_file_from_iso(**{ns: member.raw_info_path})`, call `__enter__()`,
   wrap in `io.BufferedReader`, return.

5. **`get_archive_info()`**: Read `iso.pvd`; build and return `ArchiveInfo` with
   `is_solid=False`, `version="ISO9660"`, `comment=volume_identifier.strip()`.

6. **`_translate_exception(e)`**: Map
   `pycdlib.pycdlibexception.PyCdlibException` → `ArchiveCorruptedError`; return
   `None` for others.

7. **`_close_archive()`**: Call `iso.close()`.

8. **Register the reader**: Add
   `ContainerFormat.ISO: IsoReader` to `_FORMAT_TO_READER` in
   `src/archivey/core.py`.

9. **Enable tests**: Uncomment the ISO filenames in `SKIP_TEST_FILENAMES` in
   `tests/archivey/sample_archives.py`.

---

## 13. Known pycdlib bugs and quirks

- **Rock Ridge false-negative**: If an ISO was created without the RRIP `ER`
  system use entry, pycdlib resets `rock_ridge` to `""` even if `PX`/`TF`/`NM`
  entries are present.  This causes RR metadata to be silently ignored.  There is
  no workaround short of patching pycdlib or falling back to raw DR parsing.
- **`PyCdlibIO` requires `__enter__`**: Calling `read()` before `__enter__()` raises
  `AttributeError: 'PyCdlibIO' object has no attribute '_fp'`.
- **`open_file_from_iso` is not reentrant**: Each call creates a new `PyCdlibIO`
  bound to its own file descriptor context.  Multiple concurrent opens of
  different files should be safe, but the pycdlib `PyCdlib` object itself is not
  thread-safe.
- **Joliet and RR name disagreement**: When both are present, the same file may
  have a 64-character-truncated Joliet name and a full RR name.  Always prefer RR
  for filenames to avoid silent truncation.
- **Multi-extent files**: pycdlib handles the chaining internally; `data_length`
  on a multi-extent DR is the length of that extent only, not the full logical
  file.  Use `iso.get_iso_file_length()` for the total size of multi-extent files
  if needed.
- **pycdlib is an authoring library**: Much of the library code handles writing
  ISOs; the read path is a secondary concern.  Error reporting for corrupt inputs
  is sometimes poor.

---

## 14. What pycdlib handles well (no workaround needed)

| Feature | Status |
|---|---|
| Multi-extent files | Chained automatically |
| El Torito boot records | Parsed but not exposed as members (correct behaviour) |
| All three of RR / Joliet / UDF simultaneously | Handled; pick one namespace |
| Interchange levels 1, 2, 3 | All supported |
| Volume descriptor chain iteration | `iso.open()` reads it fully |
| Directory record padding and even-byte alignment | Handled internally |
| Rock Ridge continuation areas (CE entries) | Handled internally |
| Large ISOs (> 4 GB via relaxed sector counts) | Handled |

---

## 15. Useful references

- Ecma-119 (ISO9660 specification): https://www.ecma-international.org/publications-and-standards/standards/ecma-119/
- Rock Ridge Interchange Protocol specification: IEEE P1282
- Joliet specification: https://en.wikipedia.org/wiki/Joliet_(file_system) (Microsoft did not publish a formal spec)
- pycdlib source: `/tmp/pycdlib_extracted/pycdlib/` (in this dev environment)
- pycdlib documentation: https://clalancette.github.io/pycdlib/
- Test archive creation: `tests/archivey/create_archives.py` (`create_iso_archive_with_pycdlib`, `create_iso_archive_with_genisoimage`)
- Format detection: `src/archivey/formats/format_detection.py`
- Reader registration: `src/archivey/core.py` (`_FORMAT_TO_READER`)

---

## Corrections from technical review (2026-06-10)

- **§13 `iso.get_iso_file_length()` does not exist** in pycdlib 1.16.0 (verified
  against the installed package). Multi-extent total size must be computed by
  summing the directory records of the extent group (or by another API); the
  recommendation as written is uncallable.
- Confirmed accurate: `PyCdlibIO` binds `_fp` only in `__enter__`, and the Rock
  Ridge ER-reset quirk exists in pycdlib source.
