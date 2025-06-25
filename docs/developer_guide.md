# archivey Developer Guide: Adding Support for New Archive Formats

Welcome! This guide is for anyone looking to help `archivey` support even more archive formats by creating custom `ArchiveReader` implementations. We're excited to see what you build!

## How `archivey` Works with Readers

At the heart of `archivey`'s ability to handle different formats is the `ArchiveReader` abstract base class and its handy helper, `BaseArchiveReader`. You'll find these in `archivey.internal.base_reader`. Most of the time, you'll want your new reader to build on `BaseArchiveReader`.

`archivey`'s code is organized into a few key areas:
*   `archivey.api`: This is what users of the library interact with (e.g., `open_archive`).
*   `archivey.internal`: Contains the foundational pieces like `BaseArchiveReader` and other helpers.
*   `archivey.formats`: This is where all the specific readers for different formats (like ZIP, TAR, etc.) live. Your new reader will go here too!

## Key Steps to Create Your New Reader

Here's a walkthrough of what you'll need to do:

1.  **Create Your Reader Class:**
    *   Typically, your new reader class will inherit from `archivey.internal.base_reader.BaseArchiveReader`.
    *   For example: `class MyFormatReader(BaseArchiveReader):`

2.  **Set Up the Constructor (`__init__`)**:
    *   You'll need to call the `super().__init__(...)` constructor from `BaseArchiveReader`. It expects a few things:
    *   `super().__init__(format: ArchiveFormat, archive_path: BinaryIO | str | bytes | os.PathLike, streaming_only: bool, members_list_supported: bool, pwd: bytes | str | None = None)`
        *   `format`: An enum value from `archivey.api.types.ArchiveFormat`. This tells `archivey` what format your reader handles (e.g., `ArchiveFormat.ZIP`).
        *   `archive_path`: The path to the archive file or a file-like object that `archivey` can read from.
        *   `streaming_only`: Set this to `True` if the archive format (or the library you're using to read it) can only read files one after another, from start to finish (like a raw compressed stream). If `True`, things like `open()` (for jumping to a specific file) and `extract()` won't be available, and you might only be able to go through the files once. Set it to `False` if you can jump around and access files directly.
        *   `members_list_supported`: Set this to `True` if your reader can usually get a full list of all files in the archive right away (like ZIP files, which have a central directory) without needing to read the whole archive. If `False`, getting the list of members (`get_members()`) might mean your reader has to scan through a good chunk of the archive.
    *   After calling `super().__init__`, you can set up anything else your reader needs, like opening the archive file with a third-party library.

3.  **Implement `iter_members_for_registration(self) -> Iterator[ArchiveMember]` (A Really Important Method):**
    *   This is a key method for `BaseArchiveReader`. Your job here is to `yield` `archivey.api.types.ArchiveMember` objects, one for each file or entry in the archive.
    *   `BaseArchiveReader` uses this method to find out what's inside the archive.
    *   For every item in the archive, you'll want to:
        *   Collect its details: filename, size, when it was last modified, its type (file, folder, link), permissions, and so on, using the library or format tools you have.
        *   Create an `ArchiveMember` object and fill in its details.
        *   If the library you're using gives you its own object representing the member, store that in the `raw_info` field of your `ArchiveMember`. This is often very helpful for the `_open_member` method later on, so it knows exactly which file to open in the underlying library.
        *   `yield` the `ArchiveMember` object you just created.
    *   Make sure you correctly translate the member types from your archive format (like file, directory, symbolic link, hard link) to `archivey.api.types.MemberType`.
    *   If the format tells you where symbolic links or hardlinks point to right in their header information, fill in the `link_target` field. `BaseArchiveReader` will try to figure out the actual target file later based on this `link_target` string.
    *   **Good to know:** `BaseArchiveReader` takes care of registering the members you yield. This iterator is usually run once to get the initial list of members.

4.  **Implement `get_archive_info(self) -> ArchiveInfo`:**
    *   You'll need to implement this method. It should return an `archivey.api.types.ArchiveInfo` object.
    *   Fill this object with information about the archive as a whole:
        *   `format`: The `ArchiveFormat` enum for your format.
        *   `is_solid`: Is the archive "solid"? (True/False). Solid archives compress multiple files together, which can make them smaller but sometimes slower to extract individual files from.
        *   `comment`: Any comment stored in the archive itself.
        *   `extra`: A dictionary where you can put any other interesting details specific to your format (like the version of the format).

5.  **Implement `_open_member(self, member: ArchiveMember, *, pwd: Optional[Union[bytes, str]] = None, for_iteration: bool = False) -> BinaryIO`:**
    *   This method is also one you'll need to implement. (While `BaseArchiveReader` marks it as abstract, if your reader is strictly `streaming_only` and you only override `iter_members_with_io` without needing the base class's `open` or `extract`, you might not directly use it. However, for the best compatibility and to support all features, it's a good idea to implement it.)
    *   Its job is to open a specific archive member for reading and return a binary I/O stream (something you can `read()` bytes from).
    *   The `member` argument is an `ArchiveMember` object. You can use `member.raw_info` (which you stored earlier) if you need the original library-specific object to help open the file.
    *   `pwd`: If the archive or member is encrypted, this is where the password will be provided.
    *   `for_iteration`: This is a little hint. If `True`, it means this open request is part of reading through files one by one (like with `iter_members_with_io`). You can use this hint to optimize things if opening for iteration is different or quicker than a direct `open()` call.
    *   **Very Important**: The stream you return **must** be wrapped with `archivey.internal.io_helpers.ExceptionTranslatingIO` (we'll talk more about this below). This helps make sure errors are handled smoothly.
    *   **Good to know:**
        *   This method is only called for members that are actual files (`member.is_file` is `True`), after any links have been resolved.
        *   `member.raw_info` (if you populated it in `iter_members_for_registration`) will be available.
        *   If you're in `streaming_only=True` mode and `for_iteration=True`, this will be called for the member that `iter_members()` just yielded. This implies that the underlying stream should be ready for a sequential read.
        *   Streams that users get from the public `BaseArchiveReader.open()` (which calls your method) are tracked and automatically closed when `BaseArchiveReader.close()` is called.

6.  **Implement `_close_archive(self) -> None`:**
    *   You'll need to implement this one too. This is where you clean up any resources your reader was using. For example, close any file handles that the underlying library opened, or get rid of temporary files. This method is called by the public `close()` method.
    *   **Good to know:** This will be called at most once by the public `close()` method, and only if the archive isn't already closed.

## Optional Methods You Can Override

While the methods above are the main ones, you might find it useful to override others from `BaseArchiveReader` to make things more efficient or to add special behavior:

*   **`_prepare_member_for_open(self, member: ArchiveMember, *, pwd: bytes | str | None, for_iteration: bool) -> ArchiveMember`**:
    *   `BaseArchiveReader` calls this hook just before it calls your `_open_member` method.
    *   You can override this if you need to do any last-minute preparations, like fetching extra metadata needed to open the file, or decrypting member-specific headers, if you didn't do that during `iter_members_for_registration`.
    *   The `for_iteration` flag means the same thing as in `_open_member` and can be used if your preparation steps are different for sequential access.
    *   The default version of this method just returns the member as-is.
    *   **Good to know:** This method gets the `ArchiveMember` as it was initially figured out by `get_member()`. `_open_member` will then be called with the target of this member if it's a link (after internal resolution), or the same member if it's not a link.
*   **`iter_members_with_io(...)`**:
    *   The default way `BaseArchiveReader` does this is by using `self.iter_members()` (which relies on your `iter_members_for_registration`) and then internally calling your `_prepare_member_for_open` and `_open_member` (with `for_iteration=True`) for each file.
    *   If the library you're using has a more direct or faster way to go through members and get their I/O streams at the same time (this is especially useful for streaming formats or if it avoids doing the same work over and over), you should override this method.
    *   **Important:** If you override this, you'll be responsible for correctly applying any filtering based on the `members` and `filter` arguments. `BaseArchiveReader._build_filter` can be a helpful tool for this.
*   **`_extract_pending_files(self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None)`**:
    *   When `BaseArchiveReader.extractall()` is called, it uses something called an `ExtractionHelper`. If your reader can access files randomly (`streaming_only=False`), it first figures out all the files to extract and then calls `_extract_pending_files()`.
    *   The default `_extract_pending_files()` goes through the list of pending files and calls the public `self.open()` for each one, then streams out the data.
    *   If the library you're using has a more optimized way to extract many files at once (like `zipfile.ZipFile.extractall()`), you can override this method to use that faster approach.
    *   **Note:** The `extractall` method in `BaseArchiveReader` already handles the overall filtering of which files to extract. If you specifically override `_extract_pending_files`, it will get a list of already-filtered members to extract from the `extraction_helper`. If you were to override `extractall` itself, you'd need to handle the filtering.

## Handling Errors with `ExceptionTranslatingIO`

When your `_open_member()` method (or a custom `iter_members_with_io` override) returns a file stream, it's **very important** to wrap it with `archivey.internal.io_helpers.ExceptionTranslatingIO`. This is a wrapper that helps ensure that if any errors happen in the third-party library while reading the stream (like during `read()` or `seek()`), they get turned into `archivey.api.exceptions.ArchiveError` subclasses. This makes error handling much more consistent for users of `archivey`.

```python
from archivey.internal.io_helpers import ExceptionTranslatingIO # Corrected path
from archivey.api.exceptions import ArchiveCorruptedError, ArchiveIOError, ArchiveEncryptedError # Corrected path
# Import specific exceptions from your third-party library
from third_party_lib import ThirdPartyReadError, ThirdPartyCorruptError, ThirdPartySomeOpenError

# Inside your _open_member() method:
try:
    # This is how you might get the raw stream from your underlying library
    raw_stream = self.underlying_library.open_member(member.raw_info)
except ThirdPartySomeOpenError as e: # Handle errors that happen when you try to open it
    # It's good practice to wrap the original error
    raise ArchiveEncryptedError("Failed to open member, maybe it's encrypted or password protected?") from e

# This is your custom function that knows about your third-party library's errors
def my_exception_translator(exc: Exception) -> Optional[ArchiveError]:
    if isinstance(exc, ThirdPartyCorruptError):
        return ArchiveCorruptedError(f"The archive data seems to be corrupted: {exc}")
    elif isinstance(exc, ThirdPartyReadError):
        return ArchiveIOError(f"An I/O error happened while reading a member: {exc}")
    # Add more translations for other specific errors from your library
    return None # If it's not an error you know, or if it's already an ArchiveError, let it pass through

# Wrap the raw stream with ExceptionTranslatingIO and your translator
return ExceptionTranslatingIO(raw_stream, my_exception_translator)
```

**Key things to remember for `ExceptionTranslatingIO`:**

*   **Be Specific:** Your `exception_translator` function should try to be as specific as possible. Catch known error types from the third-party library you're using and map them to the right `ArchiveError` subclasses (like `ArchiveCorruptedError`, `ArchiveIOError`, `ArchiveEncryptedError`).
*   **Avoid Generic `Exception`:** Try not to just catch the general `Exception` type and translate it. This can hide bugs or unexpected problems.
*   **Testing is Your Friend:** It's a great idea to write tests that deliberately cause different errors in the underlying library. This way, you can make sure your translator handles them correctly. You might need to create some corrupted or specially made archive files for this.
*   **Return `None` for Unknowns:** If an error happens that your translator doesn't specifically handle (or if it's already an `ArchiveError`), just return `None` from your translator function. `ExceptionTranslatingIO` will then re-raise the original error.

## The `ArchiveMember` Object

The `archivey.api.types.ArchiveMember` class is what `archivey` uses to keep track of each individual entry (file, folder, etc.) within an archive. When you're implementing `iter_members_for_registration`, you'll be creating these objects. Here are some of the key fields you'll want to fill in:

*   `filename: str`
*   `file_size: int` (this should be the uncompressed size)
*   `compress_size: int` (the compressed size, if you know it)
*   `mtime: datetime` (when the file was last modified)
*   `type: MemberType` (e.g., `MemberType.FILE`, `MemberType.DIR`, `MemberType.SYMLINK`)
*   `mode: Optional[int]` (file permissions, like `0o755`)
*   `link_target: Optional[str]` (for symlinks/hardlinks, this is the path they point to)
*   `encrypted: bool` (is the member encrypted?)
*   `raw_info: Any` (this is where you should store the original member info object from the library you're using. It's super helpful, for example, the `ZipReader` uses it for `_archive.open()`)
*   Other fields like `crc32`, `compression_method`, `comment`, `create_system`, `extra`.

Take a look at the `ArchiveMember` class definition in `archivey.api.types` to see all the fields available.

## Letting `archivey` Know About Your Reader

Once you've built your awesome new reader, you'll need to tell `archivey.api.core.open_archive` about it so it can detect the archive format and use your reader. (The exact details of how this registration works might change over time, so it's a good idea to check the current `open_archive` function to see how it's done).

By following these ideas, you can help `archivey` become even more versatile by adding support for new archive formats. We appreciate your contributions!
