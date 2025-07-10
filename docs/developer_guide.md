# Developer guide

This guide explains how to extend `archivey` by creating custom `archivey.archive_reader.ArchiveReader` classes for new archive formats.

## Overview

The library's modules are organized into these packages:
* `archivey.core` and related modules – public API functions, types and classes
* `archivey.internal` – base classes and helpers
* `archivey.formats` – format-specific readers

The library exposes an `archivey.archive_reader.ArchiveReader` abstract base class for users with the public API. The actual format readers extend from the helper class `archivey.internal.base_reader.BaseArchiveReader`, which implements most of the public API by delegating to some simpler methods that the readers must implement. New readers will almost always want to inherit from `BaseArchiveReader`.

## Registering Your Reader

For your reader to be called, you'll need to:

*   add the format(s) your reader handles to `archivey.types.ArchiveFormat`;
*   detect archives of these formats by signature and/or filename, in `archivey.formats.format_detection`;
*   create your reader class, see below;
*   modify `archivey.core.open_archive` to associate the archive format with your reader.


## Creating a new format reader

It's probably easiest to understand what to do by looking at existing reader implementations. `archivey.formats.zip_reader.ZipReader` may be a good starting point.

Your reader should implement a few required methods, and may also implement some optional ones in special cases. These are the required methods:


### Constructor (`__init__`):

Should accept the same parameters as other `ArchiveReader`s, so it can be called from `archivey.core.open_archive`:

```python
    def __init__(
        self,
        archive_path: BinaryIO | str | bytes | os.PathLike,
        format: ArchiveFormat,
        *,
        pwd: bytes | str | None = None,
        streaming_only: bool = False,
    ):
```

*   `archive_path`: the path to the archive file, or a stream or file-like object.
*   `format`: an enum value from `archivey.types.ArchiveFormat` with the detected archive format. It will be one of the format your reader handles.
*   `pwd`: the password to use for encrypted members and/or archive headers.
*   `streaming_only`: set to `True` if the archive is being opened only for sequential, forward-only access. If `True`, methods like `open()` (for random access) and `extract()` will be disabled, and iteration will be a one-time operation.

The constructor should call `archivey.internal.base_reader.BaseArchiveReader.__init__` with the same parameters, plus another one:

```python
   super().__init__(format, archive_path, pwd, streaming_only, members_list_supported)
```

*   `members_list_supported`: set this to `True` if your reader can provide a complete list of archive members upfront (e.g., by reading a central directory like in ZIP files) without reading the entire archive. If `False` and if the file is opened in streaming-only mode, `get_members_if_available()` will not return a list of members until after the archive has been iterated through.

The constructor should initialize any internal state or open resources specific to the archive format you are supporting (e.g., open the archive file using a third-party library).


### `_close_archive(self) -> None`

This method is called exactly once when the ArchiveReader is closed or destructed. It should release any resources held by the reader (e.g., close the archive object provided by the underlying library, cleanup temporary files).


### `iter_members_for_registration(self) -> Iterator[ArchiveMember]`

This method is called by `BaseArchiveReader` to discover and register all members in the archive. It should be implemented to yield [`archivey.types.ArchiveMember`][] objects one by one, from the members in the archive. It will be iterated through only once as the members are needed, so you can use an iterator from the inner library if available.

For each member in the archive, you need to convert the metadata (filename, size, modification time, type, permissions, etc.) read from the underlying archive library/format, and create an `ArchiveMember` instance with the appropriate field values. Typically you'll store the library-specific, original member object in the `raw_info` field of the `ArchiveMember`. Don't set `_archive_id` or `_member_id`, they'll be set by `BaseArchiveReader`.

Pay special attention to the modification time field. Some archive formats store member times as UTC timestamps, others using the local timezone, some may even contain both. You should set the `mtime_with_tz` field to a `datetime.datetime` object with the timezone set if known (likely UTC), or without a timezone if it's in local time.


### `get_archive_info(self) -> ArchiveInfo`

Return an [`archivey.types.ArchiveInfo`][] object with all archive-level metadata:

*   `format`: the `ArchiveFormat` enum for this archive.
*   `is_solid`: whether the archive is solid (`True` means that decompressing a specific member may require other members to be uncompressed first).
*   please look at the `ArchiveInfo` class for other fields
*   `extra`: A dictionary for any other format-specific information that doesn't have a dedicated field


### `_open_member(self, member: ArchiveMember, *, pwd: Optional[Union[bytes, str]] = None, for_iteration: bool = False) -> BinaryIO`

This method is called by `BaseArchiveReader` to open a specific archive member, when the user calls `open()`, or during iteration or extraction. It should return a binary I/O stream (`BinaryIO`) to read the member's contents. Ideally the reader should avoid loading the whole contents to memory at once, and decompress the data as it's read by the client.

It is guaranteed to be called only for `MemberType.FILE` members (i.e. not directories or links). If the archive has been opened in `streaming_only` mode, it is guaranteed to be called only after the member has been yielded by `iter_members_for_registration` and before that iterator continues (but it is not guaranteed to be called for all files, and the stream is not guaranteed to be fully read), and is guaranteed to be closed before the iterator continues.

Arguments:

*   `member`: an `ArchiveMember` object previously yielded by `iter_members_for_registration`.
*   `pwd`: password for encrypted members if applicable. If the user does not specify a file-specific password, this will default to the password passed in the constructor, if any.
*   `for_iteration`: if `True`, the open request is part of a sequential iteration (e.g., via `iter_members_with_io`). When `streaming_only=True`, this is guaranteed to be `True`.

Tips:

*   As some archive formats (e.g. tar) may contain multiple members with the same file name, you should pass `member.raw_info` object to the underlying library if possible instead of the filename, to avoid opening a different member with the same name.
*   The returned stream should be wrapped with `archivey.internal.io_helpers.ExceptionTranslatingIO` (see section below) to ensure proper error handling.


## Optional Methods to Override

While the above are essential, you might override other methods from `BaseArchiveReader` for efficiency or specific behavior:

*   **`_prepare_member_for_open(self, member: ArchiveMember, *, pwd: bytes | str | None, for_iteration: bool) -> ArchiveMember`**:
    *   This is a hook called by `BaseArchiveReader` just before it calls your `_open_member` method.
    *   The base implementation simply returns the member unmodified; you can override this to perform tasks like fetching additional metadata required for opening, or decrypting member-specific headers, if not done during `iter_members_for_registration`.
    *   This method receives the `ArchiveMember` as initially resolved from the filename or iteration, which may be a `MemberType.LINK`. `_open_member` will then be called with the target of this member if it's a link (after internal resolution), or the same member if not a link.

*   **`iter_members_with_io(...)`**:
    *   The default implementation in `BaseArchiveReader` iterates using `self.iter_members()` (which relies on `iter_members_for_registration`) and then calls the internal open mechanism (which in turn uses your `_prepare_member_for_open` and `_open_member` methods with the `for_iteration=True` flag) for each member.
    *   If your underlying library requires a different approach to iterate through members and get their I/O streams, you can override this method directly.
    *   **Important:** If overridden, you are responsible for correctly applying filtering logic based on the `members` and `filter` arguments. `BaseArchiveReader._build_filter` can be a useful utility for this. Please look at existing implementations for details.

*   **`_extract_pending_files(self, path: str, extraction_helper: ExtractionHelper, pwd: bytes | str | None)`**:
    *   `BaseArchiveReader.extractall()` uses an `ExtractionHelper`. If the reader was opened for random access (`streaming_only=False`), the extraction first identifies all files to extract by applying any filters, handling directories and links, and then calls `_extract_pending_files()` with only the `MemberType.FILE` members that need to be extracted.
    *   The default implementation of `_extract_pending_files()` simply calls the public `self.open()` method for each file and extracts it, sequentially. If your underlying library has a more optimized (e.g. multithreaded) way to extract multiple files at once (e.g., `zipfile.ZipFile.extractall()`), override this method to use that more efficient approach.
    *   Your implementation should read pending extractions via `extraction_helper.get_pending_extractions()`, extract the files and then call `extraction_helper.process_file_extracted(member, extracted_file_path)`. See the implementation in `archivey.formats.sevenzip_reader.SevenZipReader` for an example.
    *   **Note:** The `extractall` method in `BaseArchiveReader` handles overall filtering. If you override `_extract_pending_files` specifically, it receives a list of already filtered members to extract from the `extraction_helper`. If you were to override `extractall` itself, you'd need to manage filtering and other details.


## Exception handling

Libraries often have their own exception base classes, or raise builtin exceptions such as `OSError` when there's a problem with an archive. Archivey tries to guarantee that all exceptions raised due to archive issues are subclasses of [`archivey.exceptions.ArchiveError`][], and so readers need to translate all exceptions raised by the libraries into them.

When you return a file stream provided by an underlying library from `_open_member()` (or from a custom `iter_members_with_io` override), you should wrap it with `archivey.internal.io_helpers.ExceptionTranslatingIO`. This ensures that exceptions raised by the underlying third-party library during stream operations (like `read()`, `seek()`) are translated into `ArchiveError` subclasses.

For this, you need to implement a translation function that receives any exception raised by the underlying library and returns an `ArchiveError`. Example:

```python
from archivey.internal.io_helpers import ExceptionTranslatingIO # Corrected path
from archivey.exceptions import ArchiveCorruptedError, ArchiveIOError # Corrected path
# Import specific exceptions from your third-party library
from third_party_lib import ThirdPartyReadError, ThirdPartyCorruptError

class MyReader(BaseArchiveReader):
    ...

    def _translate_exception(exc: Exception) -> Optional[ArchiveError]:
        if isinstance(exc, ThirdPartyCorruptError):
            return ArchiveCorruptedError(f"Archive data seems corrupted: {exc}")
        elif isinstance(exc, ThirdPartyReadError):
            return ArchiveIOError(f"I/O error while reading member: {exc}")
        # Add more specific translations as needed
        return None # Let other exceptions (or already ArchiveErrors) pass through

    def _open_member(self, member: ArchiveMember, pwd: Optional[str | bytes] = None, for_iteration: bool = False):
        ...
        def _open_stream() -> IO[bytes]:
            return underlying_library.open_member(member.raw_info)
        
        return ExceptionTranslatingIO(_open_stream, self._translate_exception, self.path_str, member.filename)
```

When opening an archive in your reader `__init__()`, an exception may also be raised. You may use the same translation function by wrapping your archive opening with `archivey.internal.io_helpers.run_with_exception_translation`.

Be sure to add extensive tests of your format, especially of corrupted archives, wrong passwords and any other corner cases you can think of, so that you can be sure all possible exceptions are being caught.

Tips:

*   **Specificity:** Your `exception_translator` function should be as specific as possible. Catch known exceptions from the third-party library you are using and map them to appropriate `ArchiveError` subclasses (e.g., `ArchiveCorruptedError`, `ArchiveIOError`, `ArchiveEncryptedError`).
*   **Avoid generic `Exception`:** It's fine to catch the base exception class from your library and raise a base `ArchiveError`, but you should *not* add a catch-all to convert any `Exception` that may be received, as it can hide code bugs or unexpected behavior. If the exception is unknown, return `None` to raise the original exception. (yes, this will result in exceptions raised in client code, but then we'll know about it and can hopefully add them to the list)
*   **Testing:** It's highly recommended to write tests that specifically trigger various error conditions in the underlying library to ensure your translator handles them correctly. This might involve creating corrupted or specially crafted archive files, passing wrong passwords etc. See the Testing section below.


## Testing

To run the full test suite use the same command as in CI:

```bash
uv run --extra optional pytest
```

You can run a subset of tests with the `-k` option, e.g. to run only ZIP related
tests:

```bash
uv run --extra optional pytest -k .zip
```

Sample archives used by the tests are versioned in `tests/test_archives`.  If
you add new archives or change them, regenerate the files with:

```bash
uv run --extra optional python -m tests.archivey.create_archives [pattern]
```

Omit the optional pattern to rebuild all archives.  RAR tests require the
`unrar` tool.  If it's missing those tests will fail and can be ignored.

## Building the documentation

The API documentation is generated dynamically using
[mkdocstrings](https://mkdocstrings.github.io/) and assembled into a
static site with [MkDocs](https://www.mkdocs.org). Run Hatch to build
everything:

```bash
hatch run docs
```
This copies the project `README.md` to `docs/index.md` and then
builds the MkDocs site (into `site/`). Ensure the optional development
dependencies are installed (e.g. `pip install -e .[dev]`).

