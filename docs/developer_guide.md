# archivey Developer Guide: Creating New ArchiveReaders

This guide is for developers who want to extend `archivey` by adding support for new archive formats by creating custom `ArchiveReader` implementations.

## Overview

The core of `archivey`'s extensibility lies in the `ArchiveReader` abstract base class (defined in `archivey.base_reader`) and its helper concrete class `BaseArchiveReader`. Most new readers will want to inherit from `BaseArchiveReader`.

## Key Steps to Create a New Reader

1.  **Create a New Class:**
    *   Your new reader class should typically inherit from `archivey.base_reader.BaseArchiveReader`.
    *   Example: `class MyFormatReader(BaseArchiveReader):`

2.  **Implement the Constructor (`__init__`)**:
    *   Call the `super().__init__(...)` constructor.
    *   `super().__init__(format: ArchiveFormat, archive_path: str | bytes | os.PathLike, random_access_supported: bool, members_list_supported: bool)`
        *   `format`: An enum value from `archivey.formats.ArchiveFormat` representing the format your reader handles.
        *   `archive_path`: The path to the archive file.
        *   `random_access_supported`: Set to `True` if your reader can open arbitrary files from the archive directly (e.g., using `open()`). Set to `False` if it only supports sequential streaming of members (e.g., a raw compressed stream where you must read from the beginning).
        *   `members_list_supported`: Set to `True` if your reader can provide a complete list of archive members upfront (e.g., by reading a central directory). If `False`, `get_members()` might require iterating through a significant portion of the archive.
    *   Initialize any internal state or open resources specific to the archive format you are supporting (e.g., open the archive file using a third-party library).

3.  **Implement `iter_members_for_registration(self) -> Iterator[ArchiveMember]` (Crucial):**
    *   This is the most important method for `BaseArchiveReader`. It must be implemented to yield `archivey.types.ArchiveMember` objects one by one.
    *   `BaseArchiveReader` calls this method to discover and register all members in the archive.
    *   For each member in the archive, you need to:
        *   Gather its metadata (filename, size, modification time, type, permissions, etc.) from the underlying archive library/format.
        *   Create an `ArchiveMember` instance, populating its fields.
        *   `yield` this `ArchiveMember` instance.
    *   Ensure you correctly map the archive format's member types (file, directory, symbolic link, hard link) to `archivey.types.MemberType`.
    *   Populate `link_target` for symlinks and hardlinks if the format provides this information directly in the member's header. `BaseArchiveReader` will attempt to resolve `link_target_member` later based on the `link_target` string.

4.  **Implement `get_archive_info(self) -> ArchiveInfo`:**
    *   This method should return an `archivey.types.ArchiveInfo` object.
    *   Populate it with information about the archive as a whole, such as:
        *   `format`: The `ArchiveFormat` enum.
        *   `is_solid`: Whether the archive is solid (True/False).
        *   `comment`: Any archive-level comment.
        *   `extra`: A dictionary for any other format-specific information.

5.  **Implement `open(self, member_or_filename: Union[ArchiveMember, str], *, pwd: Optional[Union[bytes, str]] = None) -> BinaryIO`:**
    *   This method is required if `random_access_supported` was set to `True` in the constructor.
    *   It should open the specified archive member for reading and return a binary I/O stream (`BinaryIO`).
    *   The `member_or_filename` argument can be either an `ArchiveMember` object (previously yielded by `iter_members_for_registration`) or a string filename. `BaseArchiveReader` provides `_resolve_member_to_open()` to help get the definitive `ArchiveMember` object and handle links.
    *   `pwd`: Handle password for encrypted members if applicable.
    *   **Important**: Wrap the returned stream from the underlying library with `archivey.io_helpers.ExceptionTranslatingIO` (see section below).

6.  **Implement `close(self) -> None`:**
    *   Release any resources held by your reader (e.g., close file handles, cleanup temporary files).

## Optional Methods to Override

While the above are essential, you might override other methods from `BaseArchiveReader` for efficiency or specific behavior:

*   **`iter_members_with_io(...)`**:
    *   The default implementation in `BaseArchiveReader` iterates using `self.iter_members()` (which relies on `iter_members_for_registration`) and then calls `self.open_for_iteration()` (which defaults to `self.open()`) for each member.
    *   If your underlying library provides a more direct or efficient way to iterate through members and get their I/O streams simultaneously (especially for streaming formats), override this method.
*   **`get_members() -> List[ArchiveMember]`**:
    *   The default implementation in `BaseArchiveReader` ensures all members are registered by exhausting `iter_members_for_registration()` and then returns the internal list.
    *   If your format allows reading a full list of members very efficiently upfront (e.g., from a central directory in a ZIP file, without reading all member data), you can override this to populate `self._members` more directly and ensure `_all_members_registered` is set to `True`. Remember to call `_register_member` for each member.
*   **`_extract_pending_files(self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None)`**:
    *   `BaseArchiveReader.extractall()` uses an `ExtractionHelper`. If `has_random_access()` is true, it first identifies all files to extract and then calls `_extract_pending_files()`.
    *   The default implementation of `_extract_pending_files()` iterates through pending files and calls `self.open()` for each, then streams data.
    *   If your underlying library has a more optimized way to extract multiple files at once (e.g., `zipfile.ZipFile.extractall()`), override this method to use that more efficient approach.
*   **`open_for_iteration(self, member, pwd) -> BinaryIO`**:
    *   Called by the default `iter_members_with_io`. Defaults to `self.open()`. Override if opening a file during iteration needs special handling compared to a direct `open()` call.

## Exception Handling with `ExceptionTranslatingIO`

When you return a file stream from `open()` (or from `iter_members_with_io`), it's crucial to wrap it with `archivey.io_helpers.ExceptionTranslatingIO`. This ensures that exceptions raised by the underlying third-party library during stream operations (like `read()`, `seek()`) are translated into `archivey.exceptions.ArchiveError` subclasses.

```python
from archivey.io_helpers import ExceptionTranslatingIO
from archivey.exceptions import ArchiveCorruptedError, ArchiveIOError
# Import specific exceptions from your third-party library
from third_party_lib import ThirdPartyReadError, ThirdPartyCorruptError

# Inside your open() method:
try:
    raw_stream = self.underlying_library.open_member(member.raw_info) # Or however you get the stream
except ThirdPartySomeOpenError as e: # Handle errors during the open call itself
    raise ArchiveEncryptedError("Failed to open member, possibly encrypted") from e

def my_exception_translator(exc: Exception) -> Optional[ArchiveError]:
    if isinstance(exc, ThirdPartyCorruptError):
        return ArchiveCorruptedError(f"Archive data seems corrupted: {exc}")
    elif isinstance(exc, ThirdPartyReadError):
        return ArchiveIOError(f"I/O error while reading member: {exc}")
    # Add more specific translations as needed
    return None # Let other exceptions (or already ArchiveErrors) pass through

return ExceptionTranslatingIO(raw_stream, my_exception_translator)
```

**Key points for `ExceptionTranslatingIO`:**

*   **Specificity:** Your `exception_translator` function should be as specific as possible. Catch known exceptions from the third-party library you are using and map them to appropriate `ArchiveError` subclasses (e.g., `ArchiveCorruptedError`, `ArchiveIOError`, `ArchiveEncryptedError`).
*   **Avoid Generic `Exception`:** Do NOT just catch `Exception` and translate it. This can hide bugs or unexpected behavior.
*   **Testing:** It's highly recommended to write tests that specifically trigger various error conditions in the underlying library to ensure your translator handles them correctly. This might involve creating corrupted or specially crafted archive files.
*   **Return `None`:** If an exception occurs that your translator doesn't specifically handle (or if it's already an `ArchiveError`), return `None` from the translator. `ExceptionTranslatingIO` will then re-raise the original exception.

## `ArchiveMember` Object

The `archivey.types.ArchiveMember` class is used to represent individual entries within an archive. When implementing `iter_members_for_registration`, you'll construct these objects. Key fields to populate include:

*   `filename: str`
*   `file_size: int` (uncompressed size)
*   `compress_size: int` (compressed size, if available)
*   `mtime_with_tz: Optional[datetime]` (modification time with timezone, ideally UTC. The `.mtime` property provides a naive datetime.)
*   `atime_with_tz: Optional[datetime]` (access time with timezone, ideally UTC.)
*   `ctime_with_tz: Optional[datetime]` (creation or metadata change time with timezone, ideally UTC.)
*   `type: MemberType` (e.g., `MemberType.FILE`, `MemberType.DIR`, `MemberType.SYMLINK`)
*   `mode: Optional[int]` (file permissions)
*   `uid: Optional[int]` (User ID of owner)
*   `gid: Optional[int]` (Group ID of owner)
*   `user_name: Optional[str]` (User name of owner)
*   `group_name: Optional[str]` (Group name of owner)
*   `link_target: Optional[str]` (for symlinks/hardlinks, the path they point to)
*   `encrypted: bool`
*   `raw_info: Any` (store the original member info object from the underlying library here; it's used by `ZipReader` for `_archive.open()`)
*   Other fields like `crc32`, `compression_method`, `comment`, `create_system`, `extra`.

Refer to the `ArchiveMember` class definition in `archivey.types` for all available fields.

## Registering Your Reader

To register your reader, you'll typically need to:
1. Add your reader class to the `_FORMAT_TO_READER` dictionary in `src/archivey/core.py`, mapping your `ArchiveFormat` enum to your reader class.
2. If your archive format requires specific signature detection beyond common magic numbers or existing file extension logic, update the `SIGNATURES`, `_EXTRA_DETECTORS`, or extension mappings in `src/archivey/formats.py`.

By following these guidelines, you can contribute robust and well-integrated support for new archive formats to `archivey`.
