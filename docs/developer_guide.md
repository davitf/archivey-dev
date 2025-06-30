# archivey Developer Guide: Creating New ArchiveReaders

If you'd like to teach `archivey` about a new archive format, this guide shows how to build your own `ArchiveReader` implementation.

## Overview

At the heart of `archivey` are the `ArchiveReader` abstract base class and the `BaseArchiveReader` helper (both found in `archivey.internal.base_reader`). Most readers will simply inherit from `BaseArchiveReader`.

Archivey's modules are organized into three packages:
`archivey.api` (public API utilities like `open_archive`),
`archivey.internal` (base classes and helpers, including `BaseArchiveReader`), and
`archivey.formats` (format-specific readers).

## Key Steps to Create a New Reader

1.  **Create a New Class:**
    *   Your new reader class should typically inherit from `archivey.internal.base_reader.BaseArchiveReader`.
    *   Example: `class MyFormatReader(BaseArchiveReader):`

2.  **Implement the Constructor (`__init__`)**:
    *   Call the `super().__init__(...)` constructor with the correct parameters:
    *   `super().__init__(format: ArchiveFormat, archive_path: BinaryIO | str | bytes | os.PathLike, streaming_only: bool, members_list_supported: bool, pwd: bytes | str | None = None)`
        *   `format`: An enum value from `archivey.api.types.ArchiveFormat` representing the format your reader handles.
        *   `archive_path`: The path to the archive file or a file-like object.
        *   `streaming_only`: Set to `True` if the archive format or underlying library only supports sequential, forward-only access (e.g., a raw compressed stream). If `True`, methods like `open()` (for random access) and `extract()` will be disabled, and iteration might be a one-time operation. Set to `False` if random access to members is possible.
        *   `members_list_supported`: Set to `True` if your reader can typically provide a complete list of archive members upfront (e.g., by reading a central directory like in ZIP files) without parsing the entire archive. If `False`, `get_members()` might require iterating through a significant portion of the archive if not already done.
    *   Initialize any internal state or open resources specific to the archive format you are supporting (e.g., open the archive file using a third-party library).

3.  **Implement `iter_members_for_registration(self) -> Iterator[ArchiveMember]` (Crucial Abstract Method):**
    *   This is the most important method for `BaseArchiveReader`. It must be implemented to yield `archivey.api.types.ArchiveMember` objects one by one.
    *   `BaseArchiveReader` calls this method to discover and register all members in the archive.
    *   For each member in the archive, you need to:
        *   Gather its metadata (filename, size, modification time, type, permissions, etc.) from the underlying archive library/format.
        *   Create an `ArchiveMember` instance, populating its fields.
        *   Store any library-specific, original member object in the `raw_info` field of the `ArchiveMember`. This can be crucial for `_open_member` to correctly identify the member for the underlying library.
        *   `yield` this `ArchiveMember` instance.
    *   Ensure you correctly map the archive format's member types (file, directory, symbolic link, hard link) to `archivey.api.types.MemberType`.
    *   Populate `link_target` for symlinks and hardlinks if the format provides this information directly in the member's header. `BaseArchiveReader` will attempt to resolve the target member later based on the `link_target` string.
    *   **Guarantees:** `BaseArchiveReader` handles the registration of yielded members. This iterator is generally consumed once to build the initial member list.

4.  **Implement `get_archive_info(self) -> ArchiveInfo` (Abstract Method):**
    *   You need to implement this method. It should return an `archivey.api.types.ArchiveInfo` object.
    *   Populate it with information about the archive as a whole, such as:
        *   `format`: The `ArchiveFormat` enum.
        *   `is_solid`: Whether the archive is solid (True/False).
        *   `comment`: Any archive-level comment.
        *   `extra`: A dictionary for any other format-specific information (e.g., version).

5.  **Implement `_open_member(self, member: ArchiveMember, *, pwd: Optional[Union[bytes, str]] = None, for_iteration: bool = False) -> BinaryIO` (Abstract Method):**
    *   You need to implement this method. (While `BaseArchiveReader` defines it as abstract, its direct usage might be bypassed if a reader is strictly `streaming_only` and only overrides `iter_members_with_io` without ever needing to open individual members through the base class's `open` or `extract` methods. However, for full compatibility and to support all base features, it should still be implemented.)
    *   It should open the specified archive member for reading and return a binary I/O stream (`BinaryIO`).
    *   The `member` argument is an `ArchiveMember` object. Use `member.raw_info` to access the original library-specific member object if needed.
    *   `pwd`: Handle password for encrypted members if applicable.
    *   `for_iteration`: This boolean flag is a hint. If `True`, the open request is part of a sequential iteration (e.g., via `iter_members_with_io`). Subclasses can use this to optimize if opening for iteration is different or cheaper than a random access `open()` call.
    *   **Important**: Always wrap the returned stream with `archivey.internal.io_helpers.ExceptionTranslatingIO` (see section below) to ensure proper error handling.
    *   **Guarantees:**
        *   Called only for members where `member.is_file` is `True` (after link resolution).
        *   `member.raw_info` (if populated by your `iter_members_for_registration`) is available.
        *   In `streaming_only=True` mode with `for_iteration=True`: called for the member just yielded by `iter_members()`, before iteration proceeds, implying the underlying stream should be positioned for sequential read.
        *   Streams returned via the public `BaseArchiveReader.open()` (which calls this method) are tracked and auto-closed by `BaseArchiveReader.close()`.

6.  **Implement `_close_archive(self) -> None` (Abstract Method):**
    *   You need to implement this method. Release any resources held by your reader (e.g., close file handles opened by the underlying library, cleanup temporary files). This is called by the public `close()` method.
    *   **Guarantee:** Called at most once by the public `close()` method if the archive is not already closed.

## Optional Methods to Override

While the above are essential, you might override other methods from `BaseArchiveReader` for efficiency or specific behavior:

*   **`_prepare_member_for_open(self, member: ArchiveMember, *, pwd: bytes | str | None, for_iteration: bool) -> ArchiveMember`**:
    *   This is a hook called by `BaseArchiveReader` just before it calls your `_open_member` method.
    *   You can override this to perform tasks like fetching additional metadata required for opening, or decrypting member-specific headers, if not done during `iter_members_for_registration`.
    *   The `for_iteration` flag has the same meaning as in `_open_member` and can be used here if preparation steps differ for sequential access.
    *   The base implementation simply returns the member unmodified.
    *   **Guarantee:** This method receives the `ArchiveMember` as initially resolved by `get_member()`. `_open_member` will then be called with the target of this member if it's a link (after internal resolution), or the same member if not a link.
*   **`iter_members_with_io(...)`**:
    *   The default implementation in `BaseArchiveReader` iterates using `self.iter_members()` (which relies on `iter_members_for_registration`) and then calls an internal open mechanism (which in turn uses your `_prepare_member_for_open` and `_open_member` methods with the `for_iteration=True` flag) for each member.
    *   If your underlying library provides a more direct or efficient way to iterate through members and get their I/O streams simultaneously (especially for streaming formats or if it avoids repeated overhead), override this method.
    *   **Important:** If overridden, you are responsible for correctly applying filtering logic based on the `members` and `filter` arguments. `BaseArchiveReader._build_filter` can be a useful utility for this.
*   **`_extract_pending_files(self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None)`**:
    *   `BaseArchiveReader.extractall()` uses an `ExtractionHelper`. If the reader supports random access (`streaming_only=False`), it first identifies all files to extract and then calls `_extract_pending_files()`.
    *   The default implementation of `_extract_pending_files()` iterates through pending files and calls the public `self.open()` for each, then streams data.
    *   If your underlying library has a more optimized way to extract multiple files at once (e.g., `zipfile.ZipFile.extractall()`), override this method to use that more efficient approach.
    *   **Note:** The `extractall` method in `BaseArchiveReader` handles overall filtering. If you override `_extract_pending_files` specifically, it receives a list of already filtered members to extract from the `extraction_helper`. If you were to override `extractall` itself, you'd need to manage filtering.

## Exception Handling with `ExceptionTranslatingIO`

Whenever you return a file stream from `_open_member()` (or from a custom `iter_members_with_io`), always wrap it with `archivey.internal.io_helpers.ExceptionTranslatingIO`. This ensures that exceptions raised by the underlying third-party library during stream operations (like `read()`, `seek()`) are translated into `archivey.api.exceptions.ArchiveError` subclasses.

```python
from archivey.internal.io_helpers import ExceptionTranslatingIO # Corrected path
from archivey.api.exceptions import ArchiveCorruptedError, ArchiveIOError # Corrected path
# Import specific exceptions from your third-party library
from third_party_lib import ThirdPartyReadError, ThirdPartyCorruptError

# Inside your _open_member() method:
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

The `archivey.api.types.ArchiveMember` class is used to represent individual entries within an archive. When implementing `iter_members_for_registration`, you'll construct these objects. Key fields to populate include:

*   `filename: str`
*   `file_size: int` (uncompressed size)
*   `compress_size: int` (compressed size, if available)
*   `mtime: datetime` (modification time)
*   `type: MemberType` (e.g., `MemberType.FILE`, `MemberType.DIR`, `MemberType.SYMLINK`)
*   `mode: Optional[int]` (file permissions)
*   `link_target: Optional[str]` (for symlinks/hardlinks, the path they point to)
*   `encrypted: bool`
*   `raw_info: Any` (store the original member info object from the underlying library here; it's used by `ZipReader` for `_archive.open()`)
*   Other fields like `crc32`, `compression_method`, `comment`, `create_system`, `extra`.

Refer to the `ArchiveMember` class definition in `archivey.api.types` for all available fields.

## Registering Your Reader

Once your reader is implemented, you'll need to modify `archivey.api.core.open_archive` to detect the archive format and instantiate your reader. (Details of this registration process might evolve, check the current `open_archive` function).

With these steps, you can help the community add support for even more archive formats to `archivey`.
