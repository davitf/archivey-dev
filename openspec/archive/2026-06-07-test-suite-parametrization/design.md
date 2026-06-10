# Design

## Current shape (the "before")

- `tests/archivey/sample_archives.py` defines `SAMPLE_ARCHIVES: list[SampleArchive]`,
  each carrying `creation_info` (format, features), `contents` (files, passwords,
  comment), and flags like `skip_test`.
- `filter_archives(SAMPLE_ARCHIVES, extensions=..., custom_filter=...)` selects a
  subset.
- `conftest.py`'s `sample_archive_path` fixture materializes the archive for a
  `sample_archive` parameter on demand (building via `create_archive`) and skips on
  `PackageNotInstalledError`.
- Each test module supplies its own `@pytest.mark.parametrize("sample_archive", ...)`
  plus, where needed, a second `@pytest.mark.parametrize` for the backend/config axis
  (`alternative_packages` → `ALTERNATIVE_CONFIG`, or `use_rar_stream`), and calls
  `skip_if_package_missing(...)` / `pytest.importorskip(...)` by hand.

## Target shape (the "after")

A single marker drives selection and config variants; a `pytest_generate_tests` hook
expands it:

```python
@pytest.mark.sample_archives(extensions=["zip"])
def test_read_zip(sample_archive, sample_archive_path): ...

@pytest.mark.sample_archives(container=ContainerFormat.TAR, configs=["default", "alt"])
def test_read_tar(sample_archive, sample_archive_path, archivey_config): ...
```

- `pytest_generate_tests(metafunc)` reads the `sample_archives` marker, resolves the
  archive subset from the registry (reusing `filter_archives`' predicates), and
  parametrizes `sample_archive` with `ids=lambda a: a.filename`.
- When `configs=` is given, it also parametrizes an `archivey_config` argument with
  the requested `ArchiveyConfig` variants, using readable ids (e.g. `defaultlibs`,
  `altlibs`, `rarstream`). The cross-product replaces today's stacked decorators.
- Missing optional packages / `unrar` resolve to **skips**, centralized: the
  existing `skip_if_package_missing` logic moves into the fixture/hook so individual
  tests no longer call it.

## Cross-check against the live specs

There is **no library-behavior change**, so no existing capability spec is touched.
The new `test-harness` capability documents the harness contract so future test
modules follow it (and so the migration is verifiable: same archives, same skips).

## Why re-implement rather than revive PR #206

- #206 is based on `main@d91a1c3`, ~8 merges behind; it predates the ISO reader,
  native-xz, stdlib-zstd, and our spec/doc work — a rebase would be a large conflict
  resolution on a bot branch.
- Its CI is red on 15/18 jobs, so it is not a working baseline.
- We keep its *ideas* (the `sample_archives` marker name, the `pytest_generate_tests`
  hook, the net-negative consolidation) and its conftest sketch as a reference, but
  build on current `main`.

## Risks

- **Coverage drift**: the migration must not silently drop or add cases. Mitigation:
  compare the collected test-id set (and per-environment passed/skipped counts)
  before and after, per tox environment.
- **Skip vs fail semantics**: centralizing skips must preserve today's behavior
  (missing lib → skip; genuine error → fail). The `skip_if_package_missing` matrix in
  `testing_utils.py` is the reference for which (format, config) combos skip.
- **id stability**: `-k` selectors used in docs/CI (`hatch run test -k .zip`) must keep
  working, so filenames remain the primary id component.
