## Why

The test suite parametrizes over a central registry of sample archives
(`SAMPLE_ARCHIVES` in `tests/archivey/sample_archives.py`), but every test module
wires that up by hand with stacked decorators:

```python
@pytest.mark.parametrize(
    "sample_archive",
    filter_archives(SAMPLE_ARCHIVES, extensions=["zip"]),
    ids=lambda x: x.filename,
)
@pytest.mark.parametrize("alternative_packages", [False, True], ids=["defaultlibs", "altlibs"])
def test_...(sample_archive, sample_archive_path, alternative_packages):
    config = ALTERNATIVE_CONFIG if alternative_packages else None
    skip_if_package_missing(sample_archive.creation_info.format, config)
    ...
```

This boilerplate is repeated across ~20 test modules, the config/backend axis
(`alternative_packages`, `use_rar_stream`) is combined ad-hoc per test, and
optional-dependency skipping is scattered between `skip_if_package_missing`,
`pytest.importorskip`, and manual `get_dependency_versions()` checks.

A bot took three runs at fixing this (`pytest_generate_tests` / dynamic fixture):
PRs #204 and #205 are closed, and #206 is a **stale draft with red CI** (15/18 jobs
failing, based on a `main` that is now ~8 merges behind). The idea is sound — the
diff is net-negative — but the execution isn't landable. This change captures the
approach cleanly and re-implements it on current `main` rather than reviving the
bot branch.

## What Changes

- **New capability `test-harness`**: a declarative `@pytest.mark.sample_archives(...)`
  marker plus a `pytest_generate_tests` hook that parametrizes the `sample_archive`
  argument by selecting from the registry (by extension / container / stream /
  feature / predicate), with the archive filename as the test id.
- **Config-variant axis** folded into the marker: a test can request default and/or
  alternative-backend `ArchiveyConfig` variants (`rapidgzip`, `indexed_bzip2`,
  `zstandard`, `use_rar_stream`, …) without stacking a second decorator.
- **Centralized skipping**: missing optional packages / external tools (`unrar`)
  produce a **skip**, not a failure, in one place (folding in `skip_if_package_missing`
  and the on-demand `sample_archive_path` build).
- **Migrate existing tests** from the hand-written decorator stacks to the marker,
  removing the duplicated `filter_archives(...)` / `importorskip` boilerplate.

The change is **test-infrastructure only** — no library (`src/`) behavior changes,
and the set of archives exercised must stay the same (the migration is behavior-
preserving for what gets tested).

## Capabilities

### New Capabilities

- `test-harness`: declarative, registry-driven parametrization of the test suite
  over sample archives and config variants, with centralized dependency skipping.

### Modified Capabilities

- (none — no library capability changes)

## Non-Goals

- Changing what the library does, or which archives/configurations are covered
  (coverage must be preserved, not expanded, in this change).
- Reviving or rebasing the stale bot branch (PR #206); we re-implement on current main.
- Replacing pytest, the tox matrix, or `create_archives.py` generation.

## Dependencies / Sequencing

**Land first — before all other pending changes.**

The declarative `@pytest.mark.sample_archives` harness is the verification layer
for every subsequent change. Migrating tests first means the native-reader and
junction changes can be validated with the cleaner parametrized suite rather than
adding more boilerplate while removing old boilerplate in the same PR.

Recommended order across all pending changes:
1. **this change** — declarative test harness
2. `base-reader-architecture-extensions` — §8.B–§8.E (§8.D enum prerequisite for 7z native)
3. `rar-native-metadata-reader` + `sevenzip-native-reader` (in parallel; also run junction Windows spike during this phase)
4. `unify-junction-handling` — after native readers (junction detection in native parsers)

## Impact

- **Files**: `tests/archivey/conftest.py` (the hook + marker), `testing_utils.py`
  (fold in skip logic), and the `tests/archivey/test_*.py` modules (adopt the marker).
- **Live spec touched**: new `test-harness` capability only.
- **Validation**: `hatch run test` must produce the same passed/skipped set per tox
  environment as before; CI green across the matrix (the bar PR #206 failed to clear).
- **Reference**: stale PR #206 (`feature/refactor-test-generation`) for the original
  `pytest_generate_tests`/`sample_archives`-marker sketch.
