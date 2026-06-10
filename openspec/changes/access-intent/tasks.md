# Implementation Tasks: Access intent (§8.F)

> Depends on `base-reader-architecture-extensions` (cost surface: `member_access_cost`,
> `seek_cost`). Land after it.

## 1. AccessIntent enum and the open parameter

- [ ] 1.1 Add an `AccessIntent` `StrEnum` (`AUTO` / `SEQUENTIAL` / `RANDOM`) to `types.py`
- [ ] 1.2 Add an `access_intent` parameter to `open_archive` (default `AccessIntent.AUTO`,
      accepting the string literal `"auto"`/`"sequential"`/`"random"` too)
- [ ] 1.3 Pass `access_intent` into the reader constructor in place of
      `streaming_only=streaming` (`core.py:212`); derive forward-only mode from
      `access_intent == SEQUENTIAL` or a non-seekable source

## 2. Remove streaming / streaming_only

- [ ] 2.1 Remove the `streaming` and `streaming_only` parameters from `open_archive`
- [ ] 2.2 Rebind the non-seekable-source check (`core.py:145`): raise
      `ArchiveStreamNotSeekableError` when the source is non-seekable and intent is
      `AUTO` or `RANDOM`; accept it under `SEQUENTIAL` (raising only if the format cannot
      operate sequentially)
- [ ] 2.3 Migrate internal callers, tests, docs, and CLI from `streaming=` to
      `access_intent=` (`streaming=True` → `"sequential"`, `streaming=False` → default)

## 3. Tri-state backend configuration

- [ ] 3.1 Change the optional-backend fields (`use_rapidgzip`, `use_indexed_bzip2`,
      `use_zstandard`, `use_python_xz`, `use_rar_stream`) to `bool | Literal["auto"]`,
      default `"auto"`; update `ConfigOverrides` and the string-literal conversion
- [ ] 3.2 At each backend-selection site (gzip/bzip2/zstd/xz/rar picks), implement the
      tri-state: `True` = use or raise `PackageNotInstalledError`; `False` = never;
      `"auto"` = use iff installed **and** the resolved intent warrants it
- [ ] 3.3 Resolve `access_intent` into the effective backend choice per the per-flag
      mapping in the configuration delta: `AUTO`/`RANDOM` activate installed
      rapidgzip/indexed_bzip2 on a seekable source; `SEQUENTIAL` activates
      `use_rar_stream`; `use_python_xz`/`use_zstandard` never activate implicitly;
      explicit `True`/`False` override intent
- [ ] 3.4 Before enabling rapidgzip/indexed_bzip2 by default under `AUTO`, verify they
      are safe with multiple concurrently-open member streams (coordinate with the
      `concurrent-member-access` exploration); if unsafe, gate the default-on behavior
      or defer the flip for that backend

## 4. Best-effort semantics

- [ ] 4.1 Make `RANDOM` fall back (no raise) when a preferred optional package is missing
      or the format cannot random-access cheaply, leaving `member_access_cost` /
      `seek_cost` to report the realized (`EXPENSIVE`) outcome
- [ ] 4.2 Keep explicit `True` flags mandatory (still raise `PackageNotInstalledError`)

## 5. Validation

- [ ] 5.1 `openspec validate access-intent --type change --strict`
- [ ] 5.2 `hatch run lint` and `hatch run test`; confirm the default open path is
      unchanged when no optional packages are installed, and that with rapidgzip /
      indexed_bzip2 installed the default (`AUTO`) path uses them on seekable sources
      with identical decompressed output and metadata
