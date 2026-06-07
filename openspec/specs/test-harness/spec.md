# Spec: Test Harness

## Purpose

Defines the test harness for parametrized test-suite execution, including the sample archive registry, declarative test selection, config variant parametrization, on-demand archive materialization, dependency-based skipping, and stable test ids.

## Requirements

### Requirement: Sample archives are defined once in a central registry

The test suite SHALL define every sample archive once, as a `SampleArchive` entry in
a single registry (`tests/archivey/sample_archives.py`), carrying its creation info
(format, features), contents, and any test flags. Tests SHALL NOT redeclare archive
definitions inline.

#### Scenario: Single source of truth
- **WHEN** a new sample archive is needed by multiple tests
- **THEN** it is added once to the registry and referenced by selection, not copied
  into each test

### Requirement: Tests select sample archives declaratively via a marker

A `@pytest.mark.sample_archives(...)` marker SHALL select a subset of the registry by
extension, container format, stream format, feature, and/or a custom predicate, and a
`pytest_generate_tests` hook SHALL parametrize the test's `sample_archive` argument
over that subset. Tests SHALL NOT need hand-written
`@pytest.mark.parametrize("sample_archive", ...)` decorators.

#### Scenario: Select by extension
- **WHEN** a test is marked `@pytest.mark.sample_archives(extensions=["zip"])`
- **THEN** it is parametrized once per ZIP sample archive in the registry

#### Scenario: Select by predicate
- **WHEN** a test is marked with a custom predicate over `SampleArchive`
- **THEN** it is parametrized over exactly the archives for which the predicate is true

### Requirement: Config variants are parametrized through the marker

The marker SHALL support requesting one or more named `ArchiveyConfig` variants
(for example default versus alternative backends such as `rapidgzip`,
`indexed_bzip2`, `zstandard`, or `use_rar_stream`) so a test runs against each
variant without stacking a second `parametrize` decorator. The selected config SHALL
be available to the test (e.g. via an `archivey_config` argument).

#### Scenario: Default and alternative backends
- **WHEN** a test requests both the default and alternative-backend config variants
- **THEN** it is parametrized over the cross-product of selected archives and those
  config variants

### Requirement: Archive files are materialized on demand

A fixture SHALL provide the on-disk path for the selected `sample_archive`, building
the archive when it does not already exist and reusing the committed fixture
otherwise.

#### Scenario: Build when absent
- **WHEN** a selected sample archive has no committed file
- **THEN** the fixture builds it (into a temporary location) and yields its path

### Requirement: Missing optional dependencies skip rather than fail

The harness SHALL skip (rather than fail) any affected test centrally when a
selected archive or config variant requires an optional package or external tool
(such as `py7zr`, `pycdlib`, `rarfile`, `lz4`, a zstd backend, or the `unrar`
binary) that is not available, rather than requiring each test to call
`importorskip`. Genuine errors SHALL still fail.

#### Scenario: Optional package missing
- **WHEN** a selected archive needs an optional package that is not installed
- **THEN** the parametrized test is skipped, not failed

#### Scenario: External tool missing
- **WHEN** a RAR test variant needs the `unrar` binary and it is not installed
- **THEN** that variant is skipped

#### Scenario: Real failure still fails
- **WHEN** a test fails for a reason other than a missing optional dependency
- **THEN** it is reported as a failure, not a skip

### Requirement: Test ids are stable and human-readable

Generated test ids SHALL include the sample archive's filename (and a readable config
-variant label when config variants are used), so subsets remain selectable with
pytest's `-k` (for example `hatch run test -k .zip`).

#### Scenario: Filename-based id
- **WHEN** a test is parametrized over sample archives
- **THEN** each case's id contains the archive filename

#### Scenario: Selecting a subset with -k
- **WHEN** `hatch run test -k .zip` is run
- **THEN** the ZIP-archive cases are selected by their filename-based ids
