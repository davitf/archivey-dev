# Archivey

Archivey is a library for reading the contents of many common archive formats. It provides a simple, unified interface on top of several builtin modules and external packages, and improves on some of their shortcomings.
Full API documentation can be found [here](docs/api/archivey/index.html).


## Features

- Support for ZIP, TAR (including compressed tar variants), RAR, 7z and ISO files, and single-file compressed formats
- Optimized streaming access reading of archive members
- Consistent handling of symlinks, file times, permissions, and passwords
- Consistent exception hierarchy
- Automatic file format detection

## Supported Formats

- ZIP (.zip)
- TAR (.tar, .tar.gz, .tgz, .tar.bz2, .tbz2, .tar.xz, .txz)
- RAR (.rar)
- 7z (.7z)
- ISO (.iso)
- Common single-file compression formats (e.g., .gz, .bz2, .xz, .lz4, .zst)

## Installation

Recommended:
```
pip install archivey[optional]
```
Or, if you don't want to add all dependencies to your project, add only the ones you need.

RAR support relies on the `unrar` tool, which you'll need to install separately.

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

## Contributing

Contributions are welcome! If you'd like to contribute to Archivey, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix (e.g., `git checkout -b feature/your-feature-name` or `git checkout -b fix/issue-number`).
3. Make your changes and commit them with clear, descriptive messages.
4. Add tests for your changes to ensure they work as expected and don't break existing functionality.
5. Ensure all tests pass by running `tox`.
6. Lint your code by running `tox -e lint` (or the specific lint command if you know it, e.g., `ruff check --fix . && ruff format .`).
7. Submit a pull request to the main repository.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
