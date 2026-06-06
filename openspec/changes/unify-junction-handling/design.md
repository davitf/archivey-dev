# Design

## Representation: flagged symlink (Model A)

Junctions are represented as `MemberType.SYMLINK` with `extra["is_junction"] = True`,
honoring the contract already documented on `ArchiveMember.is_junction`. This reuses
the existing link machinery (`is_link`, `resolve_link`, the extraction symlink path)
and keeps `MemberType` at five values.

A new field records the link's target type so callers don't have to resolve a link
to learn it points to a directory:

```python
link_target_type: Optional[MemberType] = None
# DIR/FILE/... when the format records it; None when unknown.
```

- Junctions: `link_target_type = MemberType.DIR` (a junction is, by definition, a
  directory mount point — known without resolving anything).
- Symlinks: set when the format states the target is a directory (RAR5 and 7z carry
  a directory flag); otherwise `None`.
- `is_dir` is **unchanged** (`type == DIR`). Target dir-ness is read from
  `link_target_type`, not by overloading `is_dir` with link resolution. This keeps
  `ArchiveMember` a context-free dataclass — it never needs the archive to answer a
  property — and `None` ("unknown") is the least-surprising value when a format
  doesn't say.

```
          type            is_link   is_junction   link_target_type
  ───────────────────────────────────────────────────────────────
  file     FILE             F          F            None
  dir      DIR              F          F            None
  symlink  SYMLINK          T          F            DIR | FILE | None
  hardlink HARDLINK         T          F            (target's type, if known)
  junction SYMLINK          T          T            DIR
```

## Why not a new MemberType.JUNCTION

A first-class `MemberType.JUNCTION` would be more honest, but it ripples through
every `type ==` switch, all extraction paths, the filters, and all 20 live specs —
for a Windows-only edge case. The flagged-symlink model gets correct reading and a
truthful `is_junction` with far less blast radius. We can revisit if junction-specific
behaviour accumulates (e.g. faithful extraction).

## Filter behaviour (pending spike confirmation)

If junction targets are absolute host paths (expected), they refer to nothing inside
the archive, so `resolve_link` returns `None` and the default `DATA`/`TAR` filters
reject them as absolute paths — the safe default. `FULLY_TRUSTED` preserves them.
This is consistent with Principle 3 (safe by default): an archive should not be able
to plant a `C:\Windows`-style mount point on extraction.

If the spike shows targets can be **relative and in-archive**, those specific
junctions behave like ordinary in-archive symlinks and pass the filters; the
requirement will be split accordingly.

## Spike: confirm how RAR and 7z store junction targets

**Goal:** determine, for each link kind, what type/flags the libraries report and
whether the stored target is an **absolute** host path (e.g. `C:\...`) or a
**relative** path (e.g. `..\real_dir`), and how slashes are encoded.

Run on a **Windows** machine with WinRAR and 7-Zip installed, plus a Python env with
`pip install rarfile py7zr`.

### Step 1 — Build a fixture tree with all link kinds

In an **elevated** prompt (junctions need no admin, but dir/file symlinks do, unless
Developer Mode is on). Use `cmd.exe`:

```bat
mkdir C:\jtest\real_dir
echo hello> C:\jtest\real_dir\file.txt
cd /d C:\jtest

rem Junction into the archived tree (relative-ish target candidate)
mklink /J junction_inside  C:\jtest\real_dir

rem Junction to a path OUTSIDE the archived tree (absolute target)
mklink /J junction_outside C:\Windows\Temp

rem Directory symlink and file symlink, for comparison
mklink /D symlink_dir      C:\jtest\real_dir
mklink    symlink_file     C:\jtest\real_dir\file.txt
```

### Step 2 — What does Python itself see? (folder_reader basis)

```python
import os, stat
for name in ["junction_inside", "junction_outside", "symlink_dir", "symlink_file"]:
    p = os.path.join(r"C:\jtest", name)
    st = os.lstat(p)
    print(name,
          "| isjunction:", os.path.isjunction(p),          # 3.12+
          "| islink:", os.path.islink(p),
          "| reparse_tag:", hex(getattr(st, "st_reparse_tag", 0)),
          "| S_ISDIR:", stat.S_ISDIR(st.st_mode),
          "| readlink:", repr(os.readlink(p)))
```

Record: which predicate distinguishes a junction, and whether `os.readlink` returns
an absolute or relative target. (`IO_REPARSE_TAG_MOUNT_POINT == 0xA0000003`.)

### Step 3 — Archive with WinRAR and inspect via rarfile

Create the archive saving links/junctions (GUI: Advanced → "Save symbolic links as
links"; CLI shown below — `-ol` saves symlinks/reparse points):

```bat
"C:\Program Files\WinRAR\Rar.exe" a -r -ol C:\jtest.rar C:\jtest\*
```

```python
import rarfile
rf = rarfile.RarFile(r"C:\jtest.rar")
for i in rf.infolist():
    print(i.filename,
          "| is_symlink:", i.is_symlink(),
          "| is_dir:", i.is_dir(),
          "| file_redir:", getattr(i, "file_redir", None))
# file_redir is (redir_type, flags, target_str). Note:
#   RAR5_XREDIR_UNIX_SYMLINK=1, WIN_SYMLINK=2, WIN_JUNCTION=3, HARD_LINK=4, FILE_COPY=5
```

Record, for `junction_inside` and `junction_outside`: the `redir_type` value, the
exact `target_str` (absolute vs relative, slash direction), and whether `is_dir()`
is true.

### Step 4 — Archive with 7-Zip and inspect via py7zr

GUI: add to a `.7z` with "Store symbolic links" enabled; or CLI (`-snl` stores
symbolic links as links — confirm junctions are captured, not followed):

```bat
"C:\Program Files\7-Zip\7z.exe" a -snl C:\jtest.7z C:\jtest\*
"C:\Program Files\7-Zip\7z.exe" l -slt C:\jtest.7z   rem inspect attributes/reparse
```

```python
import py7zr
with py7zr.SevenZipFile(r"C:\jtest.7z", "r") as z:
    for f in z.list():
        print(f.filename,
              "| is_symlink:", f.is_symlink,
              "| is_junction:", f.is_junction,
              "| is_directory:", f.is_directory)
    z.reset()
    # The reparse/symlink target is stored in the entry's data; read it:
    for name, bio in z.read([f.filename for f in z.list() if not f.is_directory]).items():
        print(name, "->", bio.read()[:200])
```

Record: which entries report `is_junction`, and the target bytes (absolute NT path
like `\??\C:\...` vs a plain path), and any slash/encoding quirks.

### Step 5 — Write up the findings

Capture in this design (a short table is enough):

| entry | rar redir_type / target | 7z is_junction / target | python isjunction / readlink |
|-------|-------------------------|-------------------------|------------------------------|
| junction_inside  | … | … | … |
| junction_outside | … | … | … |
| symlink_dir      | … | … | … |
| symlink_file     | … | … | … |

**Decision driven by the table:**
- If junction targets are always absolute → keep the "default filter drops them"
  requirement as written.
- If WinRAR/7z rewrite in-archive junction targets to relative → add a requirement
  that relative, in-archive junction targets resolve like symlinks and pass the
  filters.
- Note any normalization needed (e.g. `\??\` NT prefix stripping, backslash →
  forward slash) so `link_target` is presented consistently.

### Step 6 — Keep the archives as fixtures

Copy `C:\jtest.rar` and `C:\jtest.7z` (and a zipped copy of the folder tree if
useful) into `tests/test_archives_external/` so the junction behaviour has
regression coverage and `create_archives.py` doesn't need to synthesize junctions
on non-Windows CI.

## Extraction (deferred — non-goal here)

Recreating a real junction needs `_winapi.CreateJunction` / `mklink /J` and Windows
privileges; there is no portable `os` API. Until the separate extraction change,
junctions follow the existing symlink extraction path, and absolute-target junctions
are generally dropped by the default filter. The extraction change will decide the
non-Windows fallback (directory symlink vs skip-with-warning) under Windows CI.
