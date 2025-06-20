# Archivey

Archivey is a library for reading the contents of many common archive formats. It provides a simple, unified interface on top of several builtin modules and external packages, and improves on some of their shortcomings.


## Features

- Support for ZIP, TAR (including compressed tar variants), RAR, 7z and ISO files, and single-file compressed formats
- Optimized streaming access reading of archive members
- Consistent handling of symlinks, file times, permissions, and passwords
- Consistent exception hierarchy
- Automatic file format detection

### Supported Formats and Features

| Format Family | Specific Formats                                  | Random Access | Streaming Read | Password Protected | Archive Comment | Solid Archives | Multi-file Archives | Notes                                              |
|---------------|---------------------------------------------------|---------------|----------------|--------------------|-----------------|----------------|---------------------|----------------------------------------------------|
| ZIP           | `.zip`                                            | Yes           | Yes            | Yes                | Yes             | N/A            | Yes                 | Wide compatibility.                                |
| TAR           | `.tar`                                            | Yes           | Yes            | No                 | No              | N/A            | Yes                 | Basic TAR format.                                  |
|               | `.tar.gz`, `.tgz`                                 | No            | Yes            | No                 | No              | N/A            | Yes                 | TAR with Gzip compression.                         |
|               | `.tar.bz2`, `.tbz2`                               | No            | Yes            | No                 | No              | N/A            | Yes                 | TAR with Bzip2 compression.                        |
|               | `.tar.xz`, `.txz`                                 | No            | Yes            | No                 | No              | N/A            | Yes                 | TAR with XZ compression.                           |
|               | `.tar.zst`, `.tzst`                               | No            | Yes            | No                 | No              | N/A            | Yes                 | TAR with Zstandard compression.                    |
|               | `.tar.lz4`                                        | No            | Yes            | No                 | No              | N/A            | Yes                 | TAR with LZ4 compression.                          |
| RAR           | `.rar`                                            | Yes           | Yes (v5+)      | Yes                | Yes             | Yes            | Yes                 | Requires `unrar` binary. Streaming for RAR5+ only. |
| 7z            | `.7z`                                             | Yes           | Yes            | Yes                | No              | Yes            | Yes                 | Requires `py7zr`.                                  |
| ISO           | `.iso`                                            | Yes           | Yes            | No                 | No              | N/A            | Yes                 | ISO 9660 images. Requires `pycdlib`.               |
| Single File   | `.gz`                                             | No            | Yes            | No                 | No              | N/A            | No                  | Gzip compressed single file.                       |
|               | `.bz2`                                            | No            | Yes            | No                 | No              | N/A            | No                  | Bzip2 compressed single file.                      |
|               | `.xz`                                             | No            | Yes            | No                 | No              | N/A            | No                  | XZ compressed single file.                         |
|               | `.zst`                                            | No            | Yes            | No                 | No              | N/A            | No                  | Zstandard compressed single file.                  |
|               | `.lz4`                                            | No            | Yes            | No                 | No              | N/A            | No                  | LZ4 compressed single file.                        |
| Folder        | Directory                                         | Yes           | Yes            | N/A                | N/A             | N/A            | Yes                 | Treats a directory as an archive.                  |

## Installation

Recommended:
```
pip install archivey[optional]
```
Or, if you don't want to add all dependencies to your project, add only the ones you need.

RAR support relies on the `unrar` tool, which you'll need to install separately.

| Feature/Format | Python package | System requirement |
| --- | --- | --- |
| RAR archives | `rarfile` | `unrar` binary |
| 7z archives | `py7zr` | |
| ISO images | `pycdlib` | |
| Gzip (fast) | `rapidgzip` | |
| Bzip2 (indexed) | `indexed_bzip2` | |
| XZ (pure Python) | `python-xz` | |
| Zstandard | `zstandard` or `pyzstd` | |
| LZ4 | `lz4` | |

## Usage

### Streaming access
```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    for member, stream in archive.iter_members_with_io():
        print(member.filename, member.file_size)
        if stream:
            data = stream.read()
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

## Command line usage

Archivey installs a small command line tool simply called `archivey`.
You can also invoke it via `python -m archivey`.
The CLI is primarily meant for testing or exploring the library rather than
being a full-fledged archive management utility.

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

*   **[User Guide](docs/user_guide.md)**: Learn how to use `archivey` to open and interact with archives.
*   **[Developer Guide](docs/developer_guide.md)**: Information on adding support for new archive formats by creating custom `ArchiveReader` implementations.
*   **[API Reference](docs/api/archivey/index.html)**: Detailed documentation of all public classes, methods, and functions (generated by pdoc).

