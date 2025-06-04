# Archivey

Archivey provides a single interface for reading many archive formats. It
automatically detects the format and exposes a common API similar to
`zipfile`.

## Supported Formats

- Zip (`.zip`)
- RAR (`.rar`)
- 7-Zip (`.7z`)
- Tar and compressed tar archives (`.tar`, `.tar.gz`, `.tar.bz2`, `.tar.xz`,
  `.tar.zstd`, `.tar.lz4`)
- Single-file compressed archives (`.gz`, `.bz2`, `.xz`, `.zst`, `.lz4`)
- ISO images (`.iso`)
- Plain folders on disk

Optional dependencies are used for some formats (`rarfile`, `py7zr`,
`zstandard`, `lz4`, `pycdlib`).

## Usage

The main entry point is `ArchiveStream`. It can be used either in **random
access** mode or **streaming** mode depending on how you obtain the list of
members.

### Random Access

```python
from archivey import ArchiveStream

with ArchiveStream("example.zip") as archive:
    # Load all member information at once
    members = archive.infolist()
    print("format:", archive.get_format())

    # Open a specific member
    info = archive.getinfo("file.txt")
    with archive.open(info) as f:
        data = f.read()
        print(len(data))
```

`infolist()` returns a list of `ArchiveMember` objects. You can also call
`namelist()` to get just the filenames.

### Streaming

For very large archives you may not want to load the entire list of members
into memory. Use `info_iter()` to iterate lazily:

```python
from archivey import ArchiveStream

with ArchiveStream("large.tar.zst") as archive:
    for member in archive.info_iter():
        print(member.filename)
        if member.is_file:
            with archive.open(member) as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    process(chunk)
```

`info_iter()` yields each `ArchiveMember` as it is parsed from the archive so
memory usage stays low.

### Command Line

A simple CLI is provided via the `archivey` entry point which lists archive
contents and computes checksums:

```bash
archivey sample.zip
```

Run `archivey --help` for available options.

## Development

Tests are located in the `tests/` directory and require the optional
dependencies for full coverage.
