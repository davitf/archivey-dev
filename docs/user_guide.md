# archivey User Guide

This guide explains how to use the `archivey` library to work with various archive formats.

## Opening an Archive

The main entry point to the library is the `open_archive` function:

```python
from archivey import open_archive

try:
    with open_archive("my_archive.zip") as archive:
        # Work with the archive
        print("Successfully opened archive")
except FileNotFoundError:
    print("Archive not found at the specified path.")
except ArchiveNotSupportedError:
    print("The archive format is not supported.")
except ArchiveError as e:
    print(f"An archive-related error occurred: {e}")

```

The `open_archive` function takes the path to the archive file as its primary argument. It can also accept an optional `config` object and `streaming_only` flag.

## The ArchiveReader Object

When an archive is successfully opened, `open_archive` returns an `ArchiveReader` object. This object provides methods to interact with the archive's contents. It's recommended to use the `ArchiveReader` as a context manager (as shown above) to ensure resources are properly released.

Key methods of the `ArchiveReader` object:

*   **`close()`**: Closes the archive. Automatically called if using a context manager.
*   **`get_members_if_available() -> List[ArchiveMember] | None`**: Returns a list of all members in the archive if readily available (e.g., from a central directory). May return `None` for stream-based archives where the full list isn't known without reading through the archive.
*   **`get_members() -> List[ArchiveMember]`**: Returns a list of all members in the archive. For some archive types or streaming modes, this might involve processing a significant portion of the archive if the member list isn't available upfront.
*   **`iter_members_with_io(members: Optional[Collection[Union[ArchiveMember, str]]] = None, *, pwd: Optional[Union[bytes, str]] = None, filter: Optional[Callable[[ArchiveMember, Optional[str]], Optional[ArchiveMember]]] = None) -> Iterator[tuple[ArchiveMember, Optional[BinaryIO]]]`**: Iterates over members in the archive, yielding a tuple of `(ArchiveMember, BinaryIO_stream)` for each. The stream is `None` for non-file members like directories.
    *   `members`: Optionally specify a collection of member names or `ArchiveMember` objects to iterate over.
    *   `pwd`: Password for encrypted archives.
    *   `filter`: Callable applied to each member (with `None` as the destination path) that can return the member to include or `None` to skip.
*   **`get_archive_info() -> ArchiveInfo`**: Returns an `ArchiveInfo` object containing metadata about the archive itself (e.g., format, comments, solid status).
*   **`has_random_access() -> bool`**: Returns `True` if the archive supports random access to its members (i.e., `open()` and `extract()` can be used directly without iterating). Returns `False` for streaming-only access.
*   **`get_member(member_or_filename: Union[ArchiveMember, str]) -> ArchiveMember`**: Retrieves a specific `ArchiveMember` object by its name or by passing an existing `ArchiveMember` object (useful for identity checks).
*   **`open(member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[Union[bytes, str]] = None) -> BinaryIO`**: Opens a specific member of the archive for reading and returns a binary I/O stream. This is typically available if `has_random_access()` is `True`.
*   **`extract(member_or_filename: Union[ArchiveMember, str], path: Optional[Union[str, os.PathLike]] = None, pwd: Optional[Union[bytes, str]] = None) -> Optional[str]`**: Extracts a single member to the specified `path` (defaults to the current directory). Returns the path to the extracted file. This is typically available if `has_random_access()` is `True`.
*   **`extractall(path: Optional[Union[str, os.PathLike]] = None, members: Optional[Collection[Union[ArchiveMember, str]]] = None, *, pwd: Optional[Union[bytes, str]] = None, filter: Optional[Callable[[ArchiveMember, Optional[str]], Optional[ArchiveMember]]] = None) -> dict[str, ArchiveMember]`**: Extracts all (or a specified subset of) members to the given `path`.
    *   `path`: Target directory for extraction (defaults to current directory).
    *   `members`: A collection of member names or `ArchiveMember` objects to extract.
    *   `pwd`: Password for encrypted archives.
    *   `filter`: Callable invoked for each member with the member and destination path. Return the member to extract it, or `None` to skip.
*   Returns a dictionary mapping extracted file paths to their `ArchiveMember` objects.

Streaming-only archives (where `archive.has_random_access()` returns `False`) can be iterated only **once**. After calling `iter_members_with_io()` or `extractall()`, further attempts to read or extract members will raise a `ValueError`.

## Working with Archive Members

The `ArchiveMember` object contains metadata about an individual entry within the archive, such as its name, size, modification time, type (file, directory, link), etc.

### Example: Listing Archive Contents

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("my_archive.tar.gz") as archive:
        print(f"Archive Format: {archive.get_archive_info().format.value}")
        if archive.has_random_access():
            print("Archive supports random access.")
            members = archive.get_members()
            for member in members:
                print(f"- {member.filename} (Size: {member.file_size}, Type: {member.type.value})")
        else:
            print("Archive is streaming-only. Iterating to get members:")
            for member, stream in archive.iter_members_with_io():
                print(f"- {member.filename} (Size: {member.file_size}, Type: {member.type.value})")
                if stream:
                    stream.close() # Important to close the stream if not reading from it
except ArchiveError as e:
    print(f"Error: {e}")
```

## Configuration options

`open_archive` accepts an `ArchiveyConfig` object to enable optional features.
You can pass it directly or set it as the default using
`archivey.config.default_config()`.

```python
from archivey import open_archive, ArchiveyConfig

config = ArchiveyConfig(
    use_rar_stream=True,
    use_rapidgzip=True,
    use_indexed_bzip2=True,
    overwrite_mode=OverwriteMode.OVERWRITE,
)

with open_archive("file.rar", config=config) as archive:
    ...
```

Fields on `ArchiveyConfig` enable support for optional dependencies such as
`rapidgzip`, `indexed_bzip2`, `python-xz` and `zstandard`. Each flag requires the
corresponding package to be installed. `overwrite_mode` controls how extraction
handles existing files and may be `overwrite`, `skip` or `error`.

### ArchiveyConfig fields

* `use_rar_stream` – use rarfile's streaming API to avoid temporary files when possible.
* `use_single_file_stored_metadata` – preserve metadata for single-file compressed formats (`.gz`, `.bz2`, etc.) using sidecar files.
* `use_rapidgzip` – enable the `rapidgzip` backend for faster gzip decompression.
* `use_indexed_bzip2` – enable random access for bzip2 files via `indexed_bzip2`.
* `use_python_xz` – fall back to the `python-xz` implementation instead of `lzma`.
* `use_zstandard` – enable zstandard decompression using `zstandard` or `pyzstd`.
* `tar_check_integrity` – verify that TAR member sizes match the header while reading.
* `sevenzip_read_link_targets_eagerly` – read symlink targets up front in 7z archives.
* `overwrite_mode` – how extraction handles existing files.
* `extraction_filter` – control sanitization of extracted paths.

#### Example: additional configuration flags

```python
from archivey import open_archive, ArchiveyConfig

config = ArchiveyConfig(
    use_python_xz=True,
    use_zstandard=True,
    use_single_file_stored_metadata=True,
    tar_check_integrity=False,
    sevenzip_read_link_targets_eagerly=True,
)

with open_archive("example.tar.zst", config=config) as archive:
    print(archive.get_members()[0].filename)
```

### Example: Reading a File from an Archive

```python
from archivey import open_archive, ArchiveMemberNotFoundError, ArchiveError

try:
    with open_archive("my_archive.zip") as archive:
        if not archive.has_random_access():
            print("Cannot directly open members in this archive type for reading without iteration.")
        else:
            try:
                member_to_read = "path/to/file_in_archive.txt"
                with archive.open(member_to_read) as f_stream:
                    content = f_stream.read()
                print(f"Content of {member_to_read}:\n{content.decode()}")
            except ArchiveMemberNotFoundError:
                print(f"File '{member_to_read}' not found in archive.")
            except ArchiveError as e: # Other archive errors like decryption
                print(f"Error reading file: {e}")
except ArchiveError as e:
    print(f"Error opening archive: {e}")

```
### Example: Using `iter_members_with_io`

The `iter_members_with_io` method allows you to process archive members one by
one. Each stream is closed automatically when iteration advances to the next
member or when the generator is closed.

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("my_archive.tar") as archive:
        for member, stream in archive.iter_members_with_io():
            print(f"Processing {member.filename}")
            if stream:
                data = stream.read()
                print(f"  size: {len(data)} bytes")
            # stream is closed automatically on the next iteration
except ArchiveError as e:
    print(f"Error: {e}")
```


### Example: Extracting an Archive

```python
from archivey import open_archive, ArchiveError
import os

DESTINATION_DIR = "extracted_files"

try:
    with open_archive("my_archive.rar") as archive:
        print(f"Extracting all files from {archive.archive_path} to {DESTINATION_DIR}...")
        if not os.path.exists(DESTINATION_DIR):
            os.makedirs(DESTINATION_DIR)

        extracted_files = archive.extractall(path=DESTINATION_DIR)
        print(f"Successfully extracted {len(extracted_files)} files:")
        for path, member in extracted_files.items():
            print(f"  - {path} (Original: {member.filename})")

except ArchiveError as e:
    print(f"Error: {e}")
```

This guide provides a basic overview. For more detailed information on specific classes and methods, please refer to the [API documentation](./api/archivey.html).
