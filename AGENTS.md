# Developer Notes

Please read `docs/developer_guide.md` for complete contributor guidelines.
The project uses a **src layout** with code under `src/archivey` and tests
under `tests/`.  Sample archives live in `tests/test_archives`.

## Quick commands

Install [uv](https://github.com/astral-sh/uv) and [hatch](https://github.com/pypa/hatch)
for running tests and building docs.  RAR tests require the optional
`unrar` tool.

Run the test suite:

```bash
uv run --extra optional pytest
```

Regenerate sample archives:

```bash
uv run --extra optional python -m tests.create_archives [pattern]
```

Build the documentation site:

```bash
hatch run docs
```

A simple CLI can inspect archive files:

```bash
uv run --extra optional python -m archivey.cli [archive_files]
```

It prints the detected format, lists members and displays their hashes.

## Exception handling

Wrap exceptions from third‑party libraries in subclasses of
`archivey.api.exceptions.ArchiveError`.  Avoid catching generic
`Exception`; catch the specific library base class or builtin
exceptions instead.  See the *Exception handling* section in the
developer guide for details.
