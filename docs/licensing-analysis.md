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

### Recommended fix (in order of preference)

**Option A — Call the `lzip` CLI binary via subprocess** (preferred)

Replace the Python-package import with a subprocess call to the `lzip` system
tool, exactly as archivey calls `unrar` for RAR decompression.  The `lzip`
binary tool itself is also GPL, but using a GPL binary as an external
tool invoked via subprocess does not require your own code to be GPL —
only bundling or linking the GPL code does.

```python
# Instead of: import lzip
# Do: subprocess.Popen(["lzip", "-d", "-c", archive_path], stdout=PIPE)
```

This approach requires users to install the system `lzip` tool, which is
universally available in Linux package managers (`apt install lzip`,
`brew install lzip`).

**Option B — Implement pure-Python lzip decompression**

The lzip format is documented and relatively simple (LZMA stream with a small
lzip header).  A small pure-Python reader using Python's `lzma` stdlib module
(which implements LZMA) would eliminate the external dependency entirely and
have no license concerns.

**Option C — Separate the lzip adapter into a GPL plug-in**

Move the lzip Python-package integration to a separately distributed GPL-3.0
"adapter" package (`archivey-lzip`).  archivey itself remains MIT and exposes
a registration point for external codec providers.  This is significant
architectural work.

**Option D — Remove lzip support**

`.lz` and `.tar.lz` are a niche format.  Removing it entirely eliminates the
concern.

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
| `libarchive-c` (PR #200) | BSD-2-Clause | New optional backend | ✅ Permissive; wraps libarchive (BSD-2-Clause) |

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
