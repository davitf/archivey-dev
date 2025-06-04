# Developer Notes

This project follows a **src layout**. The Python package lives in
`src/archivey` and tests are under the `tests` directory.  Test archives live in
`tests/test_archives` and helper scripts are in `tests/create_archives.py`.

Tox configurations are provided to run the suite against multiple Python
versions and dependency sets (`tox -e <env>`).  Continuous integration executes
these tox environments via the workflow in `.github/workflows/tox-tests.yml`.

To help development, install **uv** and **hatch**:

```bash
pip install uv hatch
```


## Running the tests

To run the tests:

```bash
uv run --extra optional pytest
```

To run a specific test or a subset of tests, pass `-k` with a pattern. For
example, to run only tests related to zip archives:

```bash
uv run --extra optional pytest -k .zip
```

## Updating test files

```bash
uv run --extra optional python -m tests.create_archives [file_pattern]
```

E.g. to update only zip archives:

```bash
uv run --extra optional python -m tests.create_archives "*.zip"
```

If no file_pattern is specified, all the files will be created.


## Repository layout

- `src/archivey` – implementation modules (readers, CLI, helpers).
- `tests` – pytest suite.
  - `archivey` – test utilities and main test file.
  - `test_archives` – sample archives used by the tests.
  - `test_archives_external` – external archives for specific scenarios.
- `pyproject.toml` – project metadata and tooling configuration.
- `tox.ini` – defines tox environments used in CI.
- `.github/workflows/tox-tests.yml` - defines Github actions for running the tox tests.

A command line script that can be used to test if the modules are working is:

```bash
uv run --extra optional python -m archivey.cli [archive_files]
```

It will print the contents of the archive, as read by the corresponding ArchiveReader,
along with the file hashes (computed by reading the archive members).


## Code 