# Configuration Specification

## Purpose

Define `ArchiveyConfig` — the dataclass that controls optional backends and
behavior — together with the context-variable-based default configuration and the
helpers used to read, set, and temporarily override it.

## Requirements

### Requirement: ArchiveyConfig exposes documented behavior flags

`ArchiveyConfig` SHALL provide the following fields with the stated defaults:
`use_rapidgzip` (False), `use_indexed_bzip2` (False), `use_zstandard` (False),
`use_python_xz` (False), `use_rar_stream` (False),
`use_single_file_stored_metadata` (False), `tar_check_integrity` (True),
`overwrite_mode` (`OverwriteMode.ERROR`), and `extraction_filter`
(`ExtractionFilter.DATA`).

#### Scenario: Default configuration values
- **WHEN** an `ArchiveyConfig()` is created with no arguments
- **THEN** all optional-backend flags are `False`, `tar_check_integrity` is
  `True`, `overwrite_mode` is `ERROR`, and `extraction_filter` is `DATA`

### Requirement: Optional-backend flags select alternative libraries

The `use_*` flags SHALL select alternative backend libraries instead of the
default implementation for their respective formats (rapidgzip for gzip,
indexed_bzip2 for bzip2, zstandard instead of pyzstd for zstd, python-xz for xz,
and the unrar-streaming reader for RAR).

#### Scenario: Enabling rapidgzip
- **WHEN** `ArchiveyConfig(use_rapidgzip=True)` is used to open a gzip stream
- **THEN** the rapidgzip backend is used instead of the stdlib gzip module

### Requirement: A process-wide default configuration is available

`get_archivey_config()` SHALL return the current default configuration, and
`set_archivey_config()` SHALL replace it. The default is stored in a
`contextvars.ContextVar`, so it is isolated per context.

#### Scenario: Setting and reading the default
- **WHEN** `set_archivey_config(cfg)` is called and then `get_archivey_config()`
- **THEN** the configuration `cfg` is returned

#### Scenario: open_archive uses the default when no config is passed
- **WHEN** `open_archive(path)` is called without `config`
- **THEN** the configuration returned by `get_archivey_config()` is used

### Requirement: archivey_config temporarily overrides the configuration

The `archivey_config()` context manager SHALL set a configuration (or override
individual fields of the current one) for the duration of the `with` block, and
SHALL restore the previous configuration on exit.

#### Scenario: Temporary override
- **WHEN** code runs inside `with archivey_config(use_rapidgzip=True):`
- **THEN** `get_archivey_config().use_rapidgzip` is `True` inside the block

#### Scenario: Restoration on exit
- **WHEN** the `archivey_config()` block exits
- **THEN** the configuration reverts to its previous value

### Requirement: String literals are accepted for enum-valued fields

String literals (e.g. `"skip"`, `"data"`) SHALL be accepted and converted to the
corresponding enum value when overriding `overwrite_mode` or `extraction_filter`.
An unrecognized literal SHALL raise `ValueError`.

#### Scenario: String literal for overwrite mode
- **WHEN** `archivey_config(overwrite_mode="skip")` is used
- **THEN** the effective `overwrite_mode` is `OverwriteMode.SKIP`

#### Scenario: Invalid literal
- **WHEN** an unrecognized literal is provided for an enum field
- **THEN** a `ValueError` is raised

### Requirement: set_archivey_config_fields updates selected fields

`set_archivey_config_fields(**overrides)` SHALL produce a new default
configuration that is a copy of the current one with the provided fields
replaced, ignoring overrides whose value is `None`.

#### Scenario: Update a single field
- **WHEN** `set_archivey_config_fields(use_indexed_bzip2=True)` is called
- **THEN** the new default has `use_indexed_bzip2=True` and all other fields
  unchanged
