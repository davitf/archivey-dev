# Developer Notes

This project follows a **src layout**. The Python package lives in
`src/archivey` and tests are under the `tests` directory.  Test archives live in
`tests/test_archives` and helper scripts are in `tests/archivey/create_archives.py`.

Tox configurations are provided to run the suite against multiple Python
versions and dependency sets (`tox -e <env>`).  Continuous integration executes
these tox environments via the workflow in `.github/workflows/tox-tests.yml`.

To help development, install **uv** and **hatch**. Also install the **unrar** tool
and Node.js (required for `npx pyright` in the lint script). Install tox via `uv`
with the `tox-uv` plugin:

```bash
pip install uv hatch
uv tool install tox --with tox-uv
sudo apt-get install -y unrar nodejs npm
```

If you can't install unrar, the RAR-related tests may fail; just ignore them.

## Linting and running tests

Use the Hatch scripts defined in `pyproject.toml`.

To run the linters (ruff and pyright):

```bash
hatch run lint
```

On the first run `npx` will download Pyright; accept the prompt if asked.

To run the full test suite:

```bash
hatch run test
```

To run a specific test or subset of tests, pass `-k` with a pattern. For example,
to run only tests related to zip archives:

```bash
hatch run test -k .zip
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


## XZ decompression backend

The default XZ backend is `XzDecompressorStream`, a native implementation built
on stdlib `lzma`. It requires no external packages and supports random access via
a backward index scan that builds block-level seek points without reading the full
file.

`python-xz` is available as an optional alternative backend. To use it, install
the package and set `use_python_xz=True` in `ArchiveyConfig`. If the flag is
`True` but the package is not installed, `PackageNotInstalledError` is raised.
This follows the same pattern as `use_rapidgzip` and `use_indexed_bzip2`.

```python
from archivey import open_archive
from archivey.config import ArchiveyConfig

# Default: uses XzDecompressorStream (no extra packages needed)
arc = open_archive("file.tar.xz")

# Optional: use python-xz if installed
arc = open_archive("file.tar.xz", config=ArchiveyConfig(use_python_xz=True))
```

To benchmark XZ decompression performance across backends:

```bash
uv run python benchmarks/bench_xz.py
uv run python benchmarks/bench_xz.py --quick   # shorter run
```

See `benchmarks/README.md` for details on what each variant tests.

## Best practices

- All exceptions raised by libraries should be wrapped in an exception defined in
`src/archivey/exceptions.py`.
