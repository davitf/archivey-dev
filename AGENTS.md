# Developer Notes

This project follows a **src layout**. The Python package lives in
`src/archivey` and tests are under the `tests` directory.  Test archives live in
`tests/test_archives` and helper scripts are in `tests/create_archives.py`.

## Installing in development mode

Use an editable install so local changes are picked up without reinstalling:

```bash
pip install -e .
```

Optional dependencies that enable additional archive formats can be installed
with the `optional` extra:

```bash
pip install -e ".[optional]"
```

To work on the codebase with the recommended development tools you can also
create a Hatch environment which installs the `dev` dependency group defined in
`pyproject.toml`:

```bash
pip install hatch
hatch env create
```

Inside the hatch shell you can then install the package with the optional
extras as shown above:

```bash
hatch run pip install -e ".[optional]"
```

## Running the tests

The project uses **pytest**.  Run all tests from the repository root with:

```bash
pytest
```

To run a specific test or a subset of tests, pass `-k` with a pattern. For
example, to run tests whose name contains `archive_name`:

```bash
pytest -k archive_name
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

