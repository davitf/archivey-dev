# Dependency Licensing Analysis

This document analyses the license of every runtime and optional dependency of
archivey, evaluates compatibility with archivey's own MIT license, and covers
the proposed major rewrite described in PR #207 and the associated design docs.

---

## archivey's own license

archivey is distributed under the **MIT License** (see `LICENSE`).  MIT is a
permissive license with two obligations on downstream users: preserve the
copyright notice and the permission text when copying the software.  MIT imposes
no copyleft conditions.

---

## Current dependency inventory

### Required core dependencies

| Package | Version | License | Compatible? | Notes |
|---|---|---|---|---|
| `tqdm` | ≥4.67.1 | MIT AND MPL-2.0 | ✅ Yes | Dual-licensed; MPL-2.0 is file-level copyleft but applies only to tqdm's own source files, not to archivey's code |
| `typing-extensions` | ≥4.0.0 | PSF-2.0 | ✅ Yes | Python Software Foundation license; fully permissive |
| `backports-strenum` | ≥1.3.1 | MIT | ✅ Yes | Python 3.10 only; same license as archivey |

### Optional dependencies — archive formats

| Package | Version | License | Compatible? | Notes |
|---|---|---|---|---|
| `rarfile` | ≥4.2 | ISC | ✅ Yes | Permissive; functionally equivalent to MIT |
| `py7zr` | ≥1.0.0 | **LGPL-2.1** | ⚠️ Conditional | See §3 |
| `pycdlib` | ≥1.14.0 | **LGPL-2.1-only** | ⚠️ Conditional | See §3; currently test-only, but planned for runtime use in ISO reader |
| `cryptography` | ≥45.0.3 | Apache-2.0 OR BSD-3-Clause | ✅ Yes | Both alternatives are permissive |
| `pycryptodome` | ≥3.23.0 | BSD-2-Clause | ✅ Yes | Permissive; free-threaded build alternative to `cryptography` |

### Optional dependencies — compressed stream backends

| Package | Version | License | Compatible? | Notes |
|---|---|---|---|---|
| `rapidgzip` | ==0.14.5 | MIT AND Apache-2.0 | ✅ Yes | Dual-licensed; both permissive |
| `indexed_bzip2` | ≥1.6.0 | MIT AND Apache-2.0 | ✅ Yes | Same as rapidgzip |
| `python-xz` | ≥0.5.0 | MIT | ✅ Yes | Permissive |
| `pyzstd` | ≥0.17.0 | BSD-3-Clause | ✅ Yes | Permissive |
| `zstandard` | ≥0.23.0 | BSD-3-Clause | ✅ Yes | Permissive |
| `lz4` | ≥4.4.4 | BSD-3-Clause | ✅ Yes | Permissive |
| `lzip` | ≥1.2.0 | **GPL-3.0** | 🚨 Problematic | See §2 — most significant current issue |
| `uncompresspy` | ≥0.4.0 | BSD-3-Clause | ✅ Yes | Permissive |
| `brotli` | ≥1.1.0 | MIT | ✅ Yes | Google's official Brotli Python binding |

### libarchive (proposed optional backend — PR #200)

There are **two completely separate PyPI packages** for libarchive.  This matters
because they have very different licenses:

| Package | PyPI name | License | Notes |
|---|---|---|---|
| **`libarchive-c`** | `libarchive-c` | **CC0** (public domain) | What PR #200 adds; effectively no restrictions at all |
| **`libarchive`** | `libarchive` | **GPL-2** | A different, unrelated wrapper; archivey does NOT use this |

PR #200 adds `libarchive-c>=5.3` to `pyproject.toml`.  See §5a for full analysis.

### External binary tool (not a Python package)

| Tool | License | Compatible? | Notes |
|---|---|---|---|
| `unrar` (CLI) | Proprietary freeware (RARLab) | ✅ Yes | See §4 |

### Development-only dependencies

All dev dependencies (`pytest`, `ruff`, `mkdocs`, etc.) are MIT or BSD and
are never distributed as part of archivey itself.

---

## §2 — Critical issue: `lzip` is GPL-3.0

**This is the most significant licensing problem in the current codebase.**

The `lzip` PyPI package (version 1.2.0) is licensed under **GPL-3.0**.  GPL-3.0
is a strong copyleft license: any work that is a "work based on" a GPL program —
including one that imports and uses its API — is generally required to be
distributed under GPL-3.0 as well.

Unlike LGPL (see §3), the GPL has no "library linking" exception.  The FSF's
position is clear: if your software is specifically designed to use a GPL library
and would be incomplete without it, the combined work is subject to GPL.

### How archivey currently uses lzip

`src/archivey/formats/compressed_streams.py` does a conditional import:

```python
try:
    import lzip
    import lzip_extension
except ImportError:
    lzip = None
    lzip_extension = None
```

This import provides `.lz` and `.tar.lz` archive support.  The code is
specifically written to call `lzip`'s API; it is not a "mere aggregation" on
a storage medium.

### Why "optional dependency" does not fully resolve this

The optional nature of lzip reduces the practical exposure (users without lzip
installed are not affected), but it does not resolve the legal issue.  archivey
ships code that imports and uses a GPL-3.0 library, and that code is part of the
archivey distribution.  A conservative legal reading says the code that bridges
archivey to lzip must be GPL-3.0, and distributing it under MIT creates a
license conflict.

### What the `lzip` Python package actually wraps

The `lzip` PyPI package (by neuromorphicsystems) bundles a compiled C extension
(`lzip_extension.so`) that wraps **lzlib 1.13** — a C library by the same
author as the `lzip` tool (Antonio Diaz Diaz).  lzlib is also GPL-2+, not
BSD-licensed.  So neither the Python package nor its bundled C library offers
an escape from the GPL issue via a "wrap the C library ourselves" approach
using the same underlying code.

### Recommended fix (in order of preference)

**Option A — Pure-Python decompression using stdlib `lzma` only** ✅ **(verified)**

The lzip format is LZMA1 with a 6-byte header and 20-byte trailer.  Python's
built-in `lzma` stdlib module already implements LZMA1 decompression
(`FORMAT_ALONE`); lzip support requires only ~30 lines of pure Python on top of
it to handle the header, multi-member concatenation, and CRC32 trailer
verification (using stdlib `zlib`).  This has been verified:

```python
# Proof-of-concept (single member, from testing):
import lzma, struct, zlib

LZIP_MAGIC = b'LZIP'

def decompress_lzip_member(data: bytes, offset: int) -> tuple[bytes, int]:
    coded_dict_size = data[offset + 5]
    dict_size = 1 << (coded_dict_size & 0x1f)
    lc, lp, pb = 3, 0, 2
    props = bytes([(pb * 5 + lp) * 9 + lc])
    lzma_raw = data[offset + 6:]
    dec = lzma.LZMADecompressor(format=lzma.FORMAT_ALONE)
    plaintext = dec.decompress(
        props + struct.pack('<I', dict_size) + b'\xff' * 8 + lzma_raw
    )
    trailer_offset = offset + 6 + (len(lzma_raw) - len(dec.unused_data))
    crc32_stored, data_size, member_size = struct.unpack_from('<IQQ', data, trailer_offset)
    assert len(plaintext) == data_size
    assert zlib.crc32(plaintext) & 0xFFFFFFFF == crc32_stored
    return plaintext, member_size
```

Test results (single-member, multi-member, and CRC corruption detection all
pass): confirmed correct output and no false negatives.

**Benefits**: zero new dependencies, no license concerns, slightly faster than a
subprocess (no process spawn), works on all platforms without any system package.
This is the recommended path.

**Option B — Call the `lzip` CLI binary via subprocess**

Replace the Python-package import with a subprocess call to the system `lzip`
binary, the same way archivey calls `unrar` for RAR.  Using a GPL binary as an
external process does not require the calling code to be GPL.

```python
# subprocess.Popen(["lzip", "-d", "-c", archive_path], stdout=PIPE)
```

Requires users to install `lzip` via their OS package manager (`apt install lzip`,
`brew install lzip`).  Slightly more complex than Option A and adds an external
tool dependency.  Viable if Option A proves difficult for any reason, but Option A
is simpler.

**Option C — Write a ctypes/CFFI wrapper around a BSD-licensed C library**

If a BSD-2-Clause lzip C library exists (separate from lzlib, which is GPL-2+),
writing a ctypes or CFFI wrapper would be a valid option — the wrapper would be
archivey-owned MIT code calling BSD-2-Clause C.  This is the same pattern as
`libarchive-c` (CC0 code calling BSD-2-Clause C).

Caution: verify the specific library carefully.  The main known lzip C library
(lzlib by Antonio Diaz Diaz) is GPL-2+, not BSD.  Given that Option A (pure
Python, stdlib only) already works correctly, a C wrapper adds complexity and a
new compile-time dependency for no benefit.

**Option D — Remove lzip support**

`.lz` and `.tar.lz` are a niche format.  Removing it entirely is a fallback if
none of the above approaches are taken.

---

## §3 — LGPL dependencies: `py7zr` and `pycdlib`

Both `py7zr` (LGPL-2.1) and `pycdlib` (LGPL-2.1-only) carry the GNU Lesser
General Public License.  LGPL is weaker than GPL: it explicitly permits using
the library in a "work that uses the library" without requiring the calling code
to become LGPL, provided users can substitute a modified version of the library.

### Why LGPL is generally acceptable here

For Python packages distributed via pip, the widely accepted interpretation in
the Python ecosystem is:

1. **Dynamic "linking"**: Python's import mechanism is considered analogous to
   dynamic linking — archivey does not bundle py7zr or pycdlib source code.
2. **User replaceability**: `pip install py7zr==<modified-version>` satisfies
   the LGPL requirement that users can substitute a modified library.
3. **Optional install**: Both are optional extras; users who do not install them
   are unaffected.

Under this interpretation — shared by the PSF, major Python projects, and
most legal practitioners who advise on open source — using py7zr and pycdlib as
pip dependencies is compatible with archivey's MIT license.

### Caveats worth noting

- **LGPL-2.1-only** (not "or later"): pycdlib is locked to LGPL-2.1.  Some
  interpretations of LGPL-2.1 Section 6 are stricter than LGPL-3.0.  The
  practical impact for Python pip dependencies is minimal, but it means pycdlib
  cannot be relicensed to LGPL-3.0 in future.
- **Commercial product policies**: Some organizations have internal policies that
  prohibit LGPL dependencies regardless of the dynamic-linking argument.  Users
  of archivey in those contexts would need to not install `py7zr` or `pycdlib`.
  This should be documented.
- **No bundling**: The LGPL compatibility analysis holds only as long as
  archivey does not copy py7zr or pycdlib source code into the archivey package
  itself.  The planned native parsers (§5) are written from scratch, not derived
  from these libraries.

### Current status: `pycdlib` is test-only

`pycdlib` is currently declared in `pyproject.toml`'s `optional` extras but is
only used in test archive creation (`tests/archivey/create_archives.py`).  It is
not imported in any `src/archivey/` production module.  The ISO reader (`IsoReader`)
proposed in the rewrite would make it a runtime dependency for the first time.

---

## §4 — External binary: `unrar` (RARLab)

The `unrar` command-line tool distributed by RARLab (the WinRAR authors) is
proprietary freeware.  Its key license terms:

- **Decompression is unrestricted**: The license permits using `unrar` to
  decompress RAR archives in any software for any purpose.
- **The one hard restriction**: You cannot use `unrar`'s *source code* (or the
  source of the unrar library distributed by RARLab) to re-create the RAR
  **compression** algorithm.  archivey never creates RAR archives, so this
  restriction is entirely irrelevant.
- **No redistribution in archivey**: archivey does not bundle or ship the
  `unrar` binary.  Users install it separately via their OS package manager.
  The binary is not included in any wheel or sdist.

**Verdict**: No license violation.  The use of `unrar` as an external tool for
decompression-only is fully permitted by RARLab's license.

### RAR format itself

The RAR archive format is proprietary (WinRAR GmbH holds rights to the
compression algorithm).  However:
- Implementing a **reader** (parsing and decompression) is legally unobstructed —
  RARLab publishes the RAR5 technical specification (`technote.txt`) for exactly
  this purpose.
- The RAR3 format is reverse-engineered but has been widely implemented across
  the open source ecosystem for decades without challenge.
- archivey never compresses data into RAR format.

---

## §5 — The planned rewrite: licensing impact

The rewrite is described in the spec docs added in PR #207:
`rar-native-reader-design.md`, `sevenzip-native-reader-design.md`,
`format-architecture-comparison.md`, `iso-pycdlib-analysis.md`,
`tar-stdlib-limitations.md`, and `zip-stdlib-limitations.md`.

### What the rewrite removes

| Removed dependency | License | Effect |
|---|---|---|
| `rarfile` ≥4.2 | ISC | Eliminates a benign permissive dependency; neutral |
| `py7zr` ≥1.0.0 (phase 2) | LGPL-2.1 | Eliminates the LGPL-2.1 concern for 7z ✅ |

### What the rewrite adds (new or promoted)

| Dependency | License | Change | Assessment |
|---|---|---|---|
| `pycdlib` ≥1.14.0 | LGPL-2.1-only | Promoted from test-only to runtime | LGPL concern for pycdlib; acceptable per §3 analysis |
| `inflate64` | BSD-3-Clause | New (Deflate64 in ZIP + 7z) | ✅ Permissive |
| `pyppmd` | BSD-3-Clause | New (PPMd in 7z) | ✅ Permissive |
| `bcj` / `pybcj` | BSD-3-Clause | New (BCJ filters in 7z) | ✅ Permissive |
| `libarchive-c` (PR #200) | **CC0** | New optional backend | ✅ Public domain; wraps libarchive C library (BSD-2-Clause dominant) — see §5a |

### Native RAR parser

The rewrite replaces `rarfile` with a from-scratch Python parser for RAR3/RAR5
metadata.  The design doc is explicit that decompression still delegates to the
`unrar` binary.  Licensing considerations:

- **Not derived from rarfile source**: The native parser is implemented from the
  published RAR5 technical note and from reading `rarfile.py` for format
  understanding, not by copying its code.  MIT and ISC licenses require copyright
  preservation only when distributing copies of the code, not when using it as a
  reference.
- **AES decryption**: The native parser will require `cryptography` or
  `pycryptodome` for header decryption, both already optional dependencies.
  Both are permissive; no concern.
- **Result**: Removing `rarfile` is a net improvement — one dependency gone,
  no new license issues introduced.

### Native 7z metadata parser (phase 1)

Phase 1 keeps `py7zr` for decompression but replaces its use for metadata
parsing.  The LGPL-2.1 concern for `py7zr` remains until phase 2.

### Phase 2: removing `py7zr` entirely

The design doc lists replacing `py7zr`'s decompression with native Python codecs
(lzma stdlib for LZMA/LZMA2, `pybcj`, `pyppmd`, `inflate64`, `brotli`) and
optionally an external `7z` binary.  All proposed codec dependencies are
permissive.  **Completing phase 2 eliminates the LGPL-2.1 concern for 7z.**
This is a recommended goal.

### ISO reader with pycdlib (IsoReader)

The planned `IsoReader` makes `pycdlib` a runtime dependency.  The design doc
also documents a native reader alternative (~400 lines, no external dependency).
If the LGPL concern for pycdlib is unacceptable to users of archivey, the native
reader is worth implementing.  For the initial version, pycdlib is acceptable
under the §3 analysis.

### The lzip GPL issue remains unaddressed

Neither the rewrite plan nor any of the spec documents address the `lzip`
GPL-3.0 issue.  This should be resolved independently — the subprocess approach
(Option A in §2) is the lowest-effort fix and is consistent with how archivey
already handles `unrar`.

---

## §5a — libarchive in depth: two packages, very different licenses

There is significant potential for confusion here because there are **two entirely
separate Python packages** on PyPI that provide bindings for the libarchive C
library, and they have very different licenses.

### Package 1: `libarchive-c` (what archivey uses)

- **PyPI name**: `libarchive-c`
- **License**: **CC0 1.0 Universal** (effectively public domain)
- **Author**: Changaco / Christophe Combelles
- **Homepage**: https://github.com/Changaco/python-libarchive-c
- **What it is**: A thin CFFI/ctypes wrapper around the system `libarchive` shared
  library (`libarchive.so`).  It ships no compiled C code of its own — the
  actual decompression logic lives in the separately installed system library.
- **Compatibility with MIT**: ✅ CC0 imposes zero restrictions; fully compatible.

PR #200 adds `libarchive-c>=5.3` to `pyproject.toml`.  This is the correct package.

### Package 2: `libarchive` (a different, unrelated package)

- **PyPI name**: `libarchive`
- **License**: **GPL-2**
- **What it is**: A different, older Python binding; the latest version is 0.4.7.
- **Compatibility with MIT**: 🚨 Incompatible — GPL-2 would require archivey to be GPL-2.
- **Archivey uses this**: **No.**  This package is not referenced anywhere in archivey.

The only relationship between these two packages is that they both wrap the same
underlying C library.  The package names are confusingly similar; always verify
you are looking at `libarchive-c`, not `libarchive`.

### The libarchive C library itself

The libarchive C library (`libarchive.so`, installed via the OS package manager
as `libarchive-dev` / `libarchive`) is licensed primarily under **BSD-2-Clause**
with a handful of files under other permissive licenses:

| Files | License |
|---|---|
| Core library, CLI tools | BSD-2-Clause |
| Some contrib / awk scripts | Expat (MIT-equivalent) |
| BLAKE2 implementation | Apache-2.0 OR CC0-1.0 OR OpenSSL+SSLeay |
| PPMd implementation | Public Domain |
| Historical UCB files | BSD-4-clause-UCB (non-advertising) |
| RAR5 reader | BSD-2-Clause AND Expat |
| Some contrib files | Apache-2.0 |

All of these are permissive; there is **no GPL or LGPL anywhere in the C library**.
The "weird BSD-like" impression comes from the mix of BSD variants and the
BSD-4-clause-UCB historical files (which include the old "advertising clause"
that was dropped from modern BSD).  BSD-4-clause-UCB is unusual but permissive
and compatible with MIT in practice — it requires an acknowledgement in
advertising materials, but courts and the FSF consider the clause unenforceable
and many distributions explicitly grant an exception to it.

### How `libarchive-c` uses the C library at runtime

`libarchive-c` loads `libarchive.so` at runtime via ctypes/CFFI.  It does not
bundle or statically link libarchive's code.  This means:

- archivey's wheel contains only `libarchive-c`'s own (CC0) Python code.
- The BSD-2-Clause C library is installed separately by the user (system package
  or via conda).
- No C code from libarchive is shipped inside archivey's distribution.

This is the same pattern as the `unrar` binary: archivey relies on a separately
installed system component, does not bundle it, and therefore does not need to
reproduce its license in archivey's own distribution.

### Format support libarchive adds

Based on PR #200, libarchive would be an **optional streaming-only backend**
(`config.use_libarchive=True`).  libarchive the C library supports a very wide
range of formats (tar, zip, 7z, rar, cab, lha, iso, cpio, xar, ar, …) including
many that archivey does not currently support natively.  Using it as a streaming
fallback could cover legacy or exotic formats without adding per-format Python
dependencies.  The trade-off is the requirement for `libarchive.so` to be
installed on the user's system.

### Verdict for libarchive

| Layer | License | Assessment |
|---|---|---|
| `libarchive-c` Python wrapper | CC0 | ✅ No restrictions whatsoever |
| libarchive C library (system package) | BSD-2-Clause (dominant) | ✅ Permissive; not bundled by archivey |
| `libarchive` PyPI package (NOT used) | GPL-2 | 🚨 Would be problematic — but archivey does not use this |

Adding `libarchive-c` as an optional dependency introduces **no new licensing
concerns** and is fully compatible with archivey's MIT license.

---

## §6 — Attribution and notices

archivey has **no NOTICE file or THIRD_PARTY_LICENSES document**.

### What is required

- **MIT dependencies** (most): no obligation beyond preserving their own license
  text, which pip handles automatically.
- **Apache-2.0 dependencies** (`cryptography`): the Apache-2.0 license requires
  reproducing copyright notices in documentation or binary distributions.  Since
  archivey does not bundle `cryptography`'s source or compiled artifacts in its
  wheel, this requirement is met by `cryptography`'s own package metadata on the
  user's machine.  No additional NOTICE file is strictly required.
- **LGPL-2.1 dependencies** (`py7zr`, `pycdlib`): LGPL requires that users can
  obtain the library's source and substitute a modified version.  PyPI provides
  this.  Archivey is not required to reproduce their copyright text in its own
  distribution.

### Recommendation

While no current distribution obligation is unmet, adding a brief
`THIRD_PARTY_LICENSES.md` is good practice and improves transparency for
commercial users doing license audits.  It should list each optional dependency,
its license, and where to find the license text.

---

## §7 — Summary and action items

### Things to fix (license violations or serious risks)

| # | Issue | Severity | Recommended action |
|---|---|---|---|
| 1 | `lzip` is GPL-3.0; archivey imports it | 🚨 High | Replace Python package import with subprocess call to system `lzip` binary (mirrors how `unrar` is used), or implement a pure-Python lzip reader using the `lzma` stdlib |

### Things that are acceptable but worth documenting

| # | Issue | Severity | Notes |
|---|---|---|---|
| 2 | `py7zr` is LGPL-2.1 | ⚠️ Medium | Acceptable for pip dependency; will be eliminated in rewrite phase 2 |
| 3 | `pycdlib` is LGPL-2.1-only; will become runtime in ISO reader | ⚠️ Medium | Acceptable for pip dependency; native ISO reader (~400 lines) would remove dependency |
| 4 | `tqdm` is MPL-2.0 AND MIT | ℹ️ Low | MPL-2.0 is file-level copyleft; no impact as long as tqdm source files are not modified or bundled |
| 5 | No THIRD_PARTY_LICENSES document | ℹ️ Low | Not legally required given packaging structure, but useful for commercial users |

### Things that are fine

- All other permissive dependencies (ISC, MIT, BSD, Apache, PSF) — no issues
- Use of the `unrar` binary via subprocess — explicitly permitted by RARLab license
- Implementing RAR3/RAR5 metadata parsing from the published spec — no restriction
- The planned native parsers being written from scratch — no copyleft contamination
- The rewrite's removal of `rarfile` and eventual removal of `py7zr` — positive improvements

### Does the rewrite introduce new license problems?

**No.** The proposed changes in PR #207 and associated branches are either
license-neutral or improve the situation.  The only pre-existing problem (`lzip`
GPL-3.0) is not addressed by the rewrite and must be handled separately.

### Can the rewrite be done without violating any licenses?

**Yes**, with one condition: the `lzip` situation must be resolved before the
rewrite ships.  Everything else is clean.

---

## Appendix: License quick-reference

| SPDX ID | Type | Copyleft? | Notes |
|---|---|---|---|
| MIT | Permissive | No | Most common; only attribution required |
| ISC | Permissive | No | Functionally identical to MIT |
| BSD-2-Clause | Permissive | No | Minimal; no advertising clause |
| BSD-3-Clause | Permissive | No | Adds non-endorsement clause |
| Apache-2.0 | Permissive | No | Patent grant; copyright notices required |
| PSF-2.0 | Permissive | No | Python-specific; broadly permissive |
| MPL-2.0 | Weak copyleft | File-level | Modified MPL files must remain MPL |
| LGPL-2.1 | Weak copyleft | Library-level | Dynamic linking / pip use is generally OK |
| LGPL-2.1-only | Weak copyleft | Library-level | Cannot upgrade to LGPL-3 |
| LGPL-3.0 | Weak copyleft | Library-level | More explicit about dynamic linking |
| GPL-3.0 | Strong copyleft | Full program | Imports/links require GPL on calling code |
