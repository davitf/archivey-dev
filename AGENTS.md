# Developer Notes

This project follows a **src layout**. The Python package lives in
`src/archivey` and tests are under the `tests` directory.  Test archives live in
`tests/test_archives` and helper scripts are in `tests/create_archives.py`.

## Installing in development mode

The project uses **Hatch** to manage the development environment.  Install
Hatch and create the default environment which includes the `dev` dependency
group:

```bash
pip install hatch
hatch env create
```

Once the environment is created, install the package in editable mode with the
optional extras so that additional archive formats are supported:

```bash
hatch run pip install -e ".[optional]"
```

## Running the tests

The project uses **pytest**.  After activating the Hatch environment, run all
tests from the repository root with:

```bash
hatch run pytest
```

To run a specific test or a subset of tests, pass `-k` with a pattern. For
example, to run tests whose name contains `archive_name`:

```bash
hatch run pytest -k archive_name
```

Tox configurations are provided to run the suite against multiple Python
versions and dependency sets (`tox -e <env>`).  Continuous integration executes
these tox environments via the workflow in `.github/workflows/tox-tests.yml`.

## Repository layout

- `src/archivey` – implementation modules (readers, CLI, helpers).
- `tests` – pytest suite.
  - `archivey` – test utilities and main test file.
  - `test_archives` – sample archives used by the tests.
  - `test_archives_external` – external archives for specific scenarios.
- `pyproject.toml` – project metadata and tooling configuration.
- `tox.ini` – defines tox environments used in CI.

The command line entry point is defined in `src/archivey/cli.py` and can be
invoked as `archivey` once installed.

