# Developer Notes

This project follows a **src layout**. The Python package lives in
`src/archivey` and tests are under the `tests` directory.  Test archives live in
`tests/test_archives` and helper scripts are in `tests/archivey/create_archives.py`.

Tox configurations are provided to run the suite against multiple Python
versions and dependency sets (`tox -e <env>`).  Continuous integration executes
these tox environments via the workflow in `.github/workflows/tox-tests.yml`.

To help development, install **uv** and **hatch**. Also install the **unrar** tool:

```bash
pip install uv hatch
sudo apt-get install -y unrar
```

If you can't install unrar, the RAR-related tests may fail; just ignore them.

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
uv run --extra optional python -m tests.archivey.create_archives [file_pattern]
```

E.g. to update only zip archives:

```bash
uv run --extra optional python -m tests.archivey.create_archives "*.zip"
```

If no file_pattern is specified, all the files will be created.


## Repository layout

- `src/archivey` – implementation modules (readers, CLI, helpers).
- `tests` – pytest suite.
  - `archivey` – conftest, test utilities and main test modules.
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


## Best practices

- All exceptions raised by libraries should be wrapped in an exception defined in
`src/archivey/exceptions.py`.
