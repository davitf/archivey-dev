# Implementation Tasks: Declarative test-suite parametrization

## 1. Marker + generation hook

- [x] 1.1 Define the `sample_archives` marker (register in `conftest.py` /
      `pyproject.toml` `markers`) with selectors: `extensions`, `container`, `stream`,
      `features`, `custom`/predicate, and `configs`
- [x] 1.2 Implement `pytest_generate_tests(metafunc)` in `conftest.py`: read the
      marker, resolve the archive subset via `filter_archives`, parametrize
      `sample_archive` with `ids=lambda a: a.filename`
- [x] 1.3 When `configs=` is present, parametrize an `archivey_config` argument with
      the requested `ArchiveyConfig` variants and readable ids
- [x] 1.4 Provide a small registry of named config variants
      (`default`, `alt`/alternative backends, `rarstream`, …)

**Quality Gate:** PASSED

## 2. Centralized skipping

- [x] 2.1 Move `skip_if_package_missing` logic into the fixture/hook so a missing
      optional package or `unrar` yields a **skip** automatically for the relevant
      (format, config) combination
- [x] 2.2 Keep `sample_archive_path`'s on-demand build + `PackageNotInstalledError`
      → skip behavior
- [x] 2.3 Ensure genuine errors still fail (skip only for missing deps)

**Quality Gate:** PASSED

## 3. Migrate test modules

- [x] 3.1 Replace hand-written `@pytest.mark.parametrize("sample_archive", filter_archives(...))`
      stacks with `@pytest.mark.sample_archives(...)` across `tests/archivey/test_*.py`
- [x] 3.2 Replace per-test `alternative_packages` / `use_rar_stream` decorators with
      the marker's `configs=`
- [x] 3.3 Remove now-redundant `skip_if_package_missing` / `importorskip` calls
- [x] 3.4 Delete dead helpers left unused after migration

**Quality Gate:** PASSED

## 4. Verify coverage is preserved

- [x] 4.1 Capture the collected test-id set before the change (per tox env via
      `pytest --collect-only -q`)
- [x] 4.2 Confirm the same archives/configs are exercised after (same ids modulo
      intended renames; same passed/skipped counts per environment)
- [x] 4.3 Confirm `-k` selectors still work (e.g. `hatch run test -k .zip`)

**Quality Gate:** PASSED

## 5. Validation

- [x] 5.1 `hatch run lint` (ruff check passes)
- [x] 5.2 `hatch run test` green locally (1417 passed, 401 skipped, 74 xfailed)
- [ ] 5.3 `openspec validate test-suite-parametrization --type change --strict`
