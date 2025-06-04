# Archivey

Archivey is a small library for reading the contents of many common archive formats. It provides a unified interface on top of packages like `zipfile`, `py7zr`, `rarfile` and `pycdlib`.

## Features

- Support for ZIP, 7z, TAR (including compressed tar variants), RAR and ISO files
- Transparent handling of single-file compressed formats (`.gz`, `.bz2`, `.xz`, `.zst`, `.lz4`)
- Stream or random access reading of archive members
- Simple command line utility for inspecting archives

## Installation

```
pip install archivey
```

Some features require optional dependencies. See `pyproject.toml` for details. RAR support relies on the `unrar` tool.

## Usage

```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    for member, stream in archive.iter_members_with_io():
        print(member.filename, member.file_size)
        if stream:
            data = stream.read()
```

A small command line tool is also available:

```
uv run --extra optional python -m archivey.cli example.zip
```

## Running the Tests

Tests are executed with `pytest`:

```
uv run --extra optional pytest
```

RAR related tests require the `unrar` binary. If it is not available, these tests may fail.
