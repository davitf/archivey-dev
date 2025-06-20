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
*   **`iter_members_with_io(members: Optional[Collection[Union[ArchiveMember, str]]] = None, *, pwd: Optional[Union[bytes, str]] = None, filter: Optional[Callable[[ArchiveMember], Optional[ArchiveMember]]] = None) -> Iterator[tuple[ArchiveMember, Optional[BinaryIO]]]`**: Iterates over members in the archive, yielding a tuple of `(ArchiveMember, BinaryIO_stream)` for each. The stream is `None` for non-file members like directories.
    *   `members`: Optionally specify a collection of member names or `ArchiveMember` objects to iterate over.
    *   `pwd`: Password for encrypted archives.
    *   `filter`: A callable to filter members during iteration.
*   **`get_archive_info() -> ArchiveInfo`**: Returns an `ArchiveInfo` object containing metadata about the archive itself (e.g., format, comments, solid status).
*   **`has_random_access() -> bool`**: Returns `True` if the archive supports random access to its members (i.e., `open()` and `extract()` can be used directly without iterating). Returns `False` for streaming-only access.
*   **`get_member(member_or_filename: Union[ArchiveMember, str]) -> ArchiveMember`**: Retrieves a specific `ArchiveMember` object by its name or by passing an existing `ArchiveMember` object (useful for identity checks).
*   **`open(member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[Union[bytes, str]] = None) -> BinaryIO`**: Opens a specific member of the archive for reading and returns a binary I/O stream. This is typically available if `has_random_access()` is `True`.
*   **`extract(member_or_filename: Union[ArchiveMember, str], path: Optional[Union[str, os.PathLike]] = None, pwd: Optional[Union[bytes, str]] = None) -> Optional[str]`**: Extracts a single member to the specified `path` (defaults to the current directory). Returns the path to the extracted file. This is typically available if `has_random_access()` is `True`.
*   **`extractall(path: Optional[Union[str, os.PathLike]] = None, members: Optional[Collection[Union[ArchiveMember, str]]] = None, *, pwd: Optional[Union[bytes, str]] = None, filter: Optional[Callable[[ArchiveMember], Optional[ArchiveMember]]] = None) -> dict[str, ArchiveMember]`**: Extracts all (or a specified subset of) members to the given `path`.
    *   `path`: Target directory for extraction (defaults to current directory).
    *   `members`: A collection of member names or `ArchiveMember` objects to extract.
    *   `pwd`: Password for encrypted archives.
    *   `filter`: A callable to filter which members get extracted.
*   Returns a dictionary mapping extracted file paths to their `ArchiveMember` objects.
*   **`test_member(member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[bytes|str] = None) -> bool`**: Tests the integrity of an archive member, typically by attempting to decompress it and verify checksums. Returns `True` if valid, `False` otherwise.
*   **`resolve_link(member: ArchiveMember) -> Optional[ArchiveMember]`**: Resolves a symlink or hardlink member to its ultimate target. Returns the target `ArchiveMember` if found, or `None` if the link is broken or the target doesn't exist.

Streaming-only archives (where `archive.has_random_access()` returns `False`) can be iterated only **once**. After calling `iter_members_with_io()` or `extractall()`, further attempts to read or extract members will raise a `ValueError`.

## Working with Archive Members

The `ArchiveMember` object contains metadata about an individual entry within the archive. Key fields include:

*   `filename: str`: The full path of the member within the archive, using forward slashes as separators.
*   `file_size: Optional[int]`: Uncompressed size of the file in bytes. `None` if not applicable (e.g., for directories) or unknown.
*   `compress_size: Optional[int]`: Compressed size of the file in bytes. `None` if not applicable or unknown.
*   `mtime_with_tz: Optional[datetime]`: Last modification timestamp with timezone information (typically UTC).
*   `mtime: Optional[datetime]` (property): Last modification timestamp without timezone information (naive datetime).
*   `atime_with_tz: Optional[datetime]`: Last access timestamp with timezone information (typically UTC). `None` if not available.
*   `atime: Optional[datetime]` (property): Last access timestamp without timezone information.
*   `ctime_with_tz: Optional[datetime]`: Creation or metadata change timestamp with timezone information (typically UTC). `None` if not available. The exact meaning can depend on the archive format and originating OS.
*   `ctime: Optional[datetime]` (property): Creation or metadata change timestamp without timezone information.
*   `type: MemberType`: An enum (`MemberType.FILE`, `MemberType.DIR`, `MemberType.SYMLINK`, `MemberType.HARDLINK`, `MemberType.OTHER`) indicating the type of the member.
*   `mode: Optional[int]`: POSIX permission bits (e.g., `0o755`). `None` if not applicable or not available.
*   `uid: Optional[int]`: User ID of the owner. `None` if not available.
*   `gid: Optional[int]`: Group ID of the owner. `None` if not available.
*   `user_name: Optional[str]`: User name of the owner. `None` if not available or not applicable.
*   `group_name: Optional[str]`: Group name of the owner. `None` if not available or not applicable.
*   `link_target: Optional[str]`: For symlinks or hardlinks, the target path. `None` otherwise.
*   `encrypted: bool`: `True` if the member is encrypted, `False` otherwise.
*   `comment: Optional[str]`: A comment associated with the member. `None` if no comment.
*   `crc32: Optional[int]`: CRC32 checksum of the uncompressed file. `None` if not available.
*   `compression_method: Optional[str]`: String representing the compression method (e.g., "deflate", "bzip2"). `None` if not applicable or unknown.
*   `create_system: Optional[CreateSystem]`: An enum indicating the operating system on which the member was likely created (e.g., `CreateSystem.UNIX`, `CreateSystem.NTFS`).

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

### Example: Resolving a Symbolic Link and Testing Members

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("archive_with_links.zip") as archive: # Ensure you have an archive for testing
        for member in archive.get_members(): # Assuming random access for get_members()
            if member.is_link:
                print(f"Found link: {member.filename} -> {member.link_target}")
                target_member = archive.resolve_link(member)
                if target_member:
                    print(f"  Resolved target: {target_member.filename} (Type: {target_member.type.value})")
                else:
                    print(f"  Link is broken or target not found in archive.")
            elif member.is_file:
                # Example of testing a file member
                if archive.test_member(member):
                    print(f"Member {member.filename} tested OK.")
                else:
                    print(f"Member {member.filename} failed integrity test.")

except ArchiveError as e:
    print(f"Error: {e}")
```

This guide provides a basic overview. For more detailed information on specific classes and methods, please refer to the [API documentation](./api/archivey.html).
