## MODIFIED Requirements

### Requirement: ArchiveyConfig exposes documented behavior flags

`ArchiveyConfig` SHALL provide the following fields with the stated defaults. The
optional-backend selection flags `use_rapidgzip`, `use_indexed_bzip2`, `use_zstandard`,
`use_python_xz`, and `use_rar_stream` SHALL each be **tri-state** — accepting `True`,
`False`, or the string literal `"auto"` — with a default of `"auto"`. The remaining
fields are unchanged: `use_single_file_stored_metadata` (False), `tar_check_integrity`
(True), `overwrite_mode` (`OverwriteMode.ERROR`), and `extraction_filter`
(`ExtractionFilter.DATA`).

#### Scenario: Default configuration values
- **WHEN** an `ArchiveyConfig()` is created with no arguments
- **THEN** every optional-backend flag is `"auto"`, `use_single_file_stored_metadata` is
  `False`, `tar_check_integrity` is `True`, `overwrite_mode` is `ERROR`, and
  `extraction_filter` is `DATA`

### Requirement: Optional-backend flags select alternative libraries

The `use_*` backend flags SHALL select alternative backend libraries instead of the
default implementation for their respective formats (rapidgzip for gzip, indexed_bzip2
for bzip2, zstandard instead of pyzstd for zstd, python-xz for xz, and the
unrar-streaming reader for RAR). Each flag is tri-state:

- `True` — always use the alternative; raise `PackageNotInstalledError` if its package
  is not installed.
- `False` — never use the alternative; always fall back to the default implementation,
  regardless of `access_intent`.
- `"auto"` (default) — use the alternative **iff** its package is installed **and** the
  resolved `access_intent` makes it worthwhile. What is "worthwhile" is per flag,
  because only some alternatives change the access-cost class relative to the default
  backend:
  - `use_rapidgzip` and `use_indexed_bzip2`: activated by `RANDOM` (they turn an
    `EXPENSIVE` rewind backend into a `LIMITED` indexed one); not by `AUTO` or
    `SEQUENTIAL`.
  - `use_python_xz`: never activated implicitly — the default XZ backend
    (`XzDecompressorStream`) already provides block-level seeking, so the alternative
    does not improve the cost class under any intent.
  - `use_zstandard`: never activated implicitly — its reopen-on-backward-seek wrapper
    has no random-access advantage over the default backend.
  - `use_rar_stream`: never activated implicitly in this change (it changes the
    iteration strategy for solid RAR, not seekability; whether `SEQUENTIAL` should
    activate it is an open question recorded in the design).

#### Scenario: Forcing a backend on
- **WHEN** `ArchiveyConfig(use_rapidgzip=True)` is used to open a gzip stream
- **THEN** the rapidgzip backend is used instead of the stdlib gzip module (or
  `PackageNotInstalledError` is raised if rapidgzip is not installed)

#### Scenario: Forcing a backend off
- **WHEN** `ArchiveyConfig(use_rapidgzip=False)` is used and the archive is opened with
  `access_intent="random"`
- **THEN** the stdlib gzip module is used (the explicit `False` overrides intent)

#### Scenario: Auto activates under RANDOM intent
- **WHEN** the default `use_rapidgzip="auto"` is in effect, rapidgzip is installed, and a
  gzip-compressed archive is opened with `access_intent="random"`
- **THEN** the rapidgzip backend is used

#### Scenario: Auto stays off without RANDOM intent
- **WHEN** the default `use_rapidgzip="auto"` is in effect and an archive is opened with
  the default intent (`AUTO`)
- **THEN** the stdlib gzip module is used (no optional backend is activated)

## ADDED Requirements

### Requirement: access_intent resolves into backend selection

`access_intent` SHALL be resolved into the same backend selection governed by the `use_*`
configuration — a high-level shorthand over one selection mechanism, not a parallel one.
For a flag left at `"auto"`, the resolved choice SHALL depend on intent: `RANDOM` enables
an installed seekable/indexed backend; `AUTO` and `SEQUENTIAL` do not enable it. A flag
set explicitly to `True` or `False` SHALL override intent (force or forbid). Resolution
SHALL be best-effort: when `RANDOM` cannot be honored — a preferred package is absent, or
the format cannot random-access cheaply (a solid 7z, a single-block xz) — archivey SHALL
fall back to an available backend and the cost properties (`member_access_cost`,
`seek_cost`) SHALL report the realized cost rather than raising. archivey SHALL raise
solely on account of intent only when random access is impossible because the source is
non-seekable.

#### Scenario: RANDOM enables an installed auto backend
- **WHEN** a `tar.gz` is opened with `access_intent="random"`, rapidgzip is installed,
  and the configuration is left at defaults
- **THEN** the rapidgzip backend is used and `member_access_cost` is `AccessCost.LIMITED`

#### Scenario: RANDOM falls back when the package is missing
- **WHEN** a `tar.gz` is opened with `access_intent="random"` but rapidgzip is not
  installed
- **THEN** the stdlib backend is used, `member_access_cost` is `AccessCost.EXPENSIVE`,
  and no exception is raised

#### Scenario: Explicit False overrides RANDOM
- **WHEN** a `tar.gz` is opened with `use_rapidgzip=False` and `access_intent="random"`
- **THEN** the stdlib backend is used

#### Scenario: Default intent selects no optional backend
- **WHEN** a `tar.gz` is opened with the default configuration and default intent
- **THEN** no optional backend is selected (behavior identical to the previous all-`False`
  default)
