# Archivey

Archivey is a library for reading many common archive formats through a simple, consistent interface. It uses several builtin modules and optional external packages for handling different formats, and also adds some features that are missing from them.

The full documentation is kept under [`docs/`](docs/) and published via MkDocs.

## Features

- Automatic file format detection
- Support for ZIP, TAR (including `.tar.gz`, `.tar.bz2`, etc.), RAR and 7z archives
- Support for single-file compressed formats like gzip, bzip2, xz, zstd and lz4
- Optimized streaming access reading of archive members
- Consistent handling of symlinks, file times, permissions, and passwords
- Consistent exception hierarchy

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

These are the basic features of the library. For more details, see the **[User guide](docs/user_guide.md)** and **[API reference](docs/api/archivey/index.html)**.

### Single-file compressed streams

Open a compressed file (e.g., `.gz` or `.xz`) to work with the uncompressed stream:

```python
from archivey import open_compressed_stream

with open_compressed_stream("example.txt.gz") as f:
    data = f.read()
```

### Extracting files

You can use filters when extracting to avoid security issues, similarly to [tarfile](https://docs.python.org/3/library/tarfile.html#extraction-filters).

```
from archivey import open_archive

with open_archive("example.zip") as archive:
    archive.extractall(path="/tmp/destpath", filter=ExtractionFilter.DATA)
```

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
```

You can open standalone compressed files as well. They are handled as an archive containing a single member.

### Streaming access

Some libraries may decompress parts of the archive multiple times if you access files individually, as in the example above. If you only need to perform some operation on all (or some) files of an archive, this mode avoids extra re-reads and decompressions:
```python
from archivey import open_archive

with open_archive("example.zip", streaming_only=True) as archive:
    for member, stream in archive.iter_members_with_io():
        print(member.filename, member.file_size)
        if stream:
            data = stream.read()
```

`streaming_only` is an optional argument; if set, it disallows some methods to ensure your code doesn't accidentally perform expensive operations.

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

*   **[User Guide](docs/user_guide.md)**: how to use this library to open and interact with archives, configuration options and so on
*   **[Developer Guide](docs/developer_guide.md)**: if you'd like to add support for new archive formats or libraries
*   **[API Reference](docs/api/archivey/index.html)**: detailed documentation of all public classes, methods, and functions (generated by pdoc)

## Future plans

*   [UNIX compress format](https://en.wikipedia.org/wiki/Compress_(software)) (`.Z`)
*   [ar archives](https://en.wikipedia.org/wiki/Ar_(Unix)) (`.ar`, `.deb`)
*   [ISO images](https://en.wikipedia.org/wiki/Optical_disc_image) (`.iso`)
*   Add [libarchive](https://pypi.org/project/libarchive/) as a backend, see what it allows us to do
*   Support non-seeking access to ZIP archives (similar approach to [`stream-unzip`](http://pypi.org/project/stream-unzip))
*   Support [builtin Zstandard](https://docs.python.org/3.14/whatsnew/3.14.html#whatsnew314-pep784) in Python 3.14
*   Auto-select libraries or implementations to use based on what is installed and/or required features
*   Archive writing support
*   Bug: ZIP filename decoding can be wrong in some cases (see [sample archive](tests/test_archives_external/encoding_infozip_jules.zip))
*   Split the [IO wrappers](src/archivey/internal/io_helpers.py) into a separate library, as it seems to be generally useful
*   Improve hard link handling and add tests for RAR4 and duplicate filenames
