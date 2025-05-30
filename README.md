# archivey

## Introduction

`archivey` is a Python library for reading various archive formats, providing a unified, stream-based interface. It aims to offer a `zipfile`-like experience for ease of use and compatibility where possible, allowing developers to interact with different archive types through a consistent API.

## Features

-   **Unified API**: A single class, `ArchiveStream`, is used to interact with different archive types, simplifying the process of reading various formats.
-   **Automatic Format Detection**: `archivey` can automatically detect the archive format from the file content and/or extension, making it easier to handle archives without knowing their specific type beforehand.
-   **Streaming Support**: The library supports iterating through archive members without loading the entire archive into memory. Use `info_iter()` for this purpose, which is particularly useful for large archives.
-   **Supported Formats**:
    -   ZIP (`.zip`)
    -   RAR (`.rar`)
    -   7z (`.7z`)
    -   TAR (`.tar`)
    -   TAR.GZ (`.tar.gz`, `.tgz`)
    -   TAR.BZ2 (`.tar.bz2`, `.tbz2`)
    -   TAR.XZ (`.tar.xz`, `.txz`)
    -   TAR.ZST (`.tar.zst`, `.tzst`) (requires zstandard)
    -   GZIP (`.gz`) (single file)
    -   BZIP2 (`.bz2`) (single file)
    -   XZ (`.xz`) (single file)
    -   ZSTD (`.zst`) (single file, requires zstandard)
    -   LZ4 (`.lz4`) (single file, requires lz4)

-   **Format-Specific Features Table**:

    | Format      | Read Members | Open Member | Extract Member | Read Comment | Encryption Support | Other                                     |
    | :---------- | :----------- | :---------- | :------------- | :----------- | :----------------- | :---------------------------------------- |
    | ZIP         | Yes          | Yes         | Yes            | Yes          | Yes (password)     |                                           |
    | RAR         | Yes          | Yes         | Yes            | Yes          | Yes (password)     | Solid archives                            |
    | 7z          | Yes          | Yes         | Yes            | No           | Yes (password)     | Solid archives                            |
    | TAR         | Yes          | Yes         | Yes            | No           | No                 | Supports various compression (gz, bz2, xz, zst) |
    | TAR.GZ      | Yes          | Yes         | Yes            | No           | No                 |                                           |
    | TAR.BZ2     | Yes          | Yes         | Yes            | No           | No                 |                                           |
    | TAR.XZ      | Yes          | Yes         | Yes            | No           | No                 |                                           |
    | TAR.ZST     | Yes          | Yes         | Yes            | No           | No                 | Requires `zstandard`                      |
    | GZIP        | Yes (single) | Yes         | Yes            | No           | No                 | Single file compression                   |
    | BZIP2       | Yes (single) | Yes         | Yes            | No           | No                 | Single file compression                   |
    | XZ          | Yes (single) | Yes         | Yes            | No           | No                 | Single file compression                   |
    | ZSTD        | Yes (single) | Yes         | Yes            | No           | No                 | Single file compression, requires `zstandard` |
    | LZ4         | Yes (single) | Yes         | Yes            | No           | No                 | Single file compression, requires `lz4`   |

-   **CLI**: `archivey` includes a command-line interface for listing archive contents, showing archive information, and checking member checksums.

## Installation

### From PyPI

```bash
pip install archivey
```

### Optional dependencies for specific formats

To install all optional dependencies:

```bash
pip install archivey[optional]
```

Alternatively, you can install them individually as needed:

-   For 7z support:
    ```bash
    pip install py7zr
    ```
-   For RAR support:
    ```bash
    pip install rarfile
    ```
-   For LZ4 and Zstandard support (if not covered by other packages or for single-file variants):
    ```bash
    pip install lz4 zstandard
    ```

## Usage Examples

### Library

#### Basic listing of archive contents

```python
from archivey import ArchiveStream

with ArchiveStream("my_archive.zip") as archive:
    for member in archive.infolist():
        print(member.filename)
```

#### Opening and reading a file from an archive

```python
from archivey import ArchiveStream

with ArchiveStream("my_archive.tar.gz") as archive:
    # Assuming "path/to/file_in_archive.txt" is a known member name
    # For TAR archives, member names are usually direct paths.
    # You might need to iterate infolist() first to find the exact member name.
    member_info = None
    for info in archive.infolist():
        if info.filename == "path/to/file_in_archive.txt":
            member_info = info
            break
    
    if member_info:
        with archive.open(member_info) as f:
            content = f.read()
            print(content.decode())
    else:
        print("File not found in archive.")
```

#### Extracting all files

```python
from archivey import ArchiveStream

# Example with password for a RAR archive
try:
    with ArchiveStream("my_archive.rar", pwd=b"password") as archive:
        archive.extractall("output_directory")
        print("Archive extracted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Example for a ZIP archive without password
try:
    with ArchiveStream("another_archive.zip") as archive:
        archive.extractall("output_directory_zip")
        print("Archive extracted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
```

### CLI

#### List contents

This will typically show filenames, sizes, and modification times. The CLI also calculates and displays CRC32 checksums by default.

```bash
archivey my_archive.zip
```

#### List contents with checksums and progress (default behavior)

```bash
archivey my_archive.tar.gz
```

#### Show archive info

Displays general information about the archive, such as type, and number of members.

```bash
archivey --info my_archive.7z
```

## Limitations

-   **Read-Only**: `archivey` is primarily focused on reading archives. Writing or creating archives is not currently supported.
-   **Libarchive Backend**: A backend leveraging `libarchive` is planned but not yet implemented. This could potentially expand format support and improve performance for some operations.
-   **Single File Archives**: For single-file compressed formats (GZIP, BZIP2, XZ, ZSTD, LZ4), `archivey` treats the compressed file as a single member within an "archive". The "filename" of this member is typically derived from the original archive filename.
-   **Password Handling**: While password-protected ZIP, RAR, and 7z archives are supported, password handling mechanisms and error reporting might vary slightly between formats.
-   **TAR Member Names**: When using `getinfo()` with TAR archives, ensure you provide the exact member name as it appears in `infolist()`. TAR archives store full paths.

## Contributing

Contributions are welcome! Please open an issue to discuss a bug or new feature, or submit a pull request with your changes.

## License

This project is licensed under the MIT License.