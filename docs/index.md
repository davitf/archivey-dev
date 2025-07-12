# Archivey

Archivey is a library for reading many common archive formats through a simple, consistent interface. It uses several builtin modules and optional external packages for handling different formats, and also adds some features that are missing from them.

## Features

- Automatic file format detection
- Support for ZIP, TAR (including `.tar.gz`, `.tar.bz2`, etc.), RAR and 7z archives
- Support for single-file compressed formats like gzip, bzip2, xz, zstd and lz4
- Consistent handling of symlinks, file times, permissions, and passwords
- Consistent exception hierarchy
- Optimized for sequential iteration over archive members

## Installation

Install with pip with all external libraries:
```
pip install archivey[optional]
```

If you'd rather manage dependencies yourself, install only the extras you need. RAR support requires the `unrar` tool, which you may need to install separately.

| Format | Builtin module | Python package | System requirement |
| --- | --- | --- | --- |
| ZIP archives | [`zipfile`](https://docs.python.org/3/library/zipfile.html) | | |
| TAR archives | [`tarfile`](https://docs.python.org/3/library/tarfile.html) | | |
| RAR archives | | [`rarfile`](https://pypi.org/project/rarfile)<br>[`cryptography`](https://pypi.org/project/cryptography) (for encrypted headers) | `unrar` binary |
| 7z archives | | [`py7zr`](https://pypi.org/project/py7zr) | |
| Gzip | [`gzip`](https://docs.python.org/3/library/gzip.html) | [`rapidgzip`](https://pypi.org/project/rapidgzip) (multithreaded decompression and random access) | |
| Bzip2 | [`bz2`](https://docs.python.org/3/library/bz2.html) | [`indexed_bzip2`](https://pypi.org/project/indexed-bzip2) (multithreaded decompression and random access) | |
| XZ | [`lzma`](https://docs.python.org/3/library/lzma.html) | [`python-xz`](https://pypi.org/project/python-xz) (random access) | |
| Zstandard | | [`pyzstd`](https://pypi.org/project/pyzstd) (preferred) or [`zstandard`](https://pypi.org/project/zstandard) | |
| LZ4 | | [`lz4`](https://pypi.org/project/lz4) | |

## Usage

These are the basic features of the library. For more details, see the **[User guide](user_guide.md)** and **[API reference](api.md)**.

### Extracting files

```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    archive.extractall(path="/tmp/destpath", filter="data")
```

You can use filters when extracting to avoid security issues, similarly to [tarfile](https://docs.python.org/3/library/tarfile.html#extraction-filters).

### Random access

```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    members = archive.get_members()
    # Read the contents of the last file in the archive
    member_to_read = members[-1]
    if member_to_read.is_file:
        stream = archive.open(member_to_read)
        data = stream.read()
        print(member_to_read.filename, data[:20])
```

`open_archive` can open standalone compressed files (e.g. `example.gz`) as well. They are handled as archives containing a single member.

### Streaming access

Some libraries may decompress parts of the archive multiple times if you access files individually with `archive.open()`. If you don't need to open arbitrary files, and just need to perform an operation on all (or some) files of an archive, iterating through the files avoids extra re-reads and decompressions:

```python
from archivey import open_archive

with open_archive("example.tar.gz", streaming_only=True) as archive:
    for member, stream in archive.iter_members_with_io():
        data = stream and stream.read(20)
        print(member.filename, member.file_size, data)
```

`streaming_only` is an optional argument; if set, it disallows some methods to ensure your code doesn't accidentally perform expensive operations.

### Single-file compressed streams

Open a compressed file (e.g., `.gz` or `.xz`) to work with the uncompressed stream:

```python
from archivey import open_compressed_stream

with open_compressed_stream("example.txt.gz") as f:
    data = f.read()
```

### Configuration
You can enable optional features and libraries by passing an `ArchiveyConfig` to `open_archive` and `open_compressed_stream`.

```python
from archivey import (
    open_archive,
    ArchiveyConfig,
    ExtractionFilter,
    OverwriteMode,
)

config = ArchiveyConfig(
    use_rar_stream=True,
    use_rapidgzip=True,
    overwrite_mode=OverwriteMode.SKIP,
    extraction_filter=ExtractionFilter.TAR,
)
with open_archive("file.rar", config=config) as archive:
    archive.extractall("out_dir")
```

### Command line usage

Archivey contains a small command line tool simply called `archivey`. If not installed by the package manager, you can also invoke it via `python -m archivey`.
The CLI is primarily meant for testing and exploring the library, but can be used for basic archive listing and extraction.

```bash
archivey my_archive.zip
archivey --extract --dest out_dir my_archive.zip
```

You can filter member names using shell patterns placed after `--`:

```bash
archivey --list my_archive.zip -- "*.txt"
```

---

## Documentation

For more detailed information on using and extending `archivey`, please refer to the following resources:

*   **[User Guide](user_guide.md)**: how to use this library to open and interact with archives, configuration options and so on
*   **[Developer Guide](developer_guide.md)**: if you'd like to add support for new archive formats or libraries
*   **[API Reference](api.md)**: detailed documentation of all public classes, methods, and functions

## Future plans

Some things on my radar for future versions. Feel free to pick some to contribute!

*   Archive format support: [ar archives](https://en.wikipedia.org/wiki/Ar_(Unix)) (`.ar`, `.deb`), [ISO images](https://en.wikipedia.org/wiki/Optical_disc_image) (`.iso`)
*   Compression format support: [UNIX compress format](https://en.wikipedia.org/wiki/Compress_(software)) (`.Z`), [Brotli](https://en.wikipedia.org/wiki/Brotli)
*   Add [libarchive](https://pypi.org/project/libarchive/) as a backend, see what it allows us to do
*   Opening self-extracting (SFX) RAR and 7z archives
*   Non-seeking access to ZIP archives (similar approach to [`stream-unzip`](http://pypi.org/project/stream-unzip))
*   Use [builtin Zstandard](https://docs.python.org/3.14/whatsnew/3.14.html#whatsnew314-pep784) in Python 3.14
*   Auto-select libraries or implementations to use based on what is installed and/or required features
*   Archive writing support
*   Bug: ZIP filename decoding can be wrong in some cases (see sample archive `tests/test_archives_external/encoding_infozip_jules.zip`)
*   Split the IO wrappers (`src/archivey/internal/io_helpers.py`) into a separate library, as it seems to be generally useful
*   Test under Windows / Mac
*   Add additional metadata fields (uid, gid, Windows permissions)
*   Add Pathlib-compatible wrapper that allows accessing files inside archives
