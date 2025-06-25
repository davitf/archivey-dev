# archivey User Guide

Welcome to `archivey`! This guide will help you get started with using the library to easily work with all sorts of archive files.

## Opening an Archive: The First Step

The main way to start using `archivey` is with the `open_archive` function. It's pretty straightforward:

```python
from archivey import open_archive, ArchiveNotSupportedError, ArchiveError

try:
    # Just tell it where your archive file is!
    with open_archive("my_archive.zip") as archive:
        # Now you can work with the 'archive' object
        print("Hooray! Successfully opened the archive.")
except FileNotFoundError:
    print("Oops! Couldn't find an archive at that spot.")
except ArchiveNotSupportedError:
    print("Hmm, this archive format isn't supported by archivey yet.")
except ArchiveError as e:
    # This catches other archive-related issues
    print(f"An archive-related problem occurred: {e}")

```

You give `open_archive` the path to your archive file. You can also give it an optional `config` object (for more advanced settings) and a `streaming_only` flag if you know you're dealing with a stream.

## Meet the `ArchiveReader` Object

When you successfully open an archive, `open_archive` gives you back an `ArchiveReader` object. This is your main tool for peeking inside the archive and getting files out. It's a good idea to use the `ArchiveReader` with a `with` statement (like in the example above). This way, Python makes sure everything is cleaned up nicely when you're done.

Here are some of the cool things an `ArchiveReader` can do:

*   **`close()`**: Shuts down the archive. If you use a `with` statement, this happens automatically!
*   **`get_members_if_available() -> List[ArchiveMember] | None`**: Tries to give you a list of all files and folders in the archive if it can do so quickly (like with ZIP files that have a table of contents). For some stream-based archives, it might return `None` because it can't know the full list without reading through everything.
*   **`get_members() -> List[ArchiveMember]`**: Gets you a list of all members (files, directories, etc.) in the archive. For some types of archives or when you're in streaming mode, this might mean `archivey` has to read through a good part of the archive if it doesn't know the list upfront.
*   **`iter_members_with_io(...) -> Iterator[tuple[ArchiveMember, Optional[BinaryIO]]]`**: This is a neat way to go through each member in the archive one by one. For each member, it gives you a pair: the `ArchiveMember` object (with info about the file) and a stream (`BinaryIO`) to read its content. The stream will be `None` for things that aren't files (like directories).
    *   `members`: You can optionally give it a list of specific member names or `ArchiveMember` objects if you only want to iterate over those.
    *   `pwd`: Got an encrypted archive? Pass the password here.
    *   `filter`: A function you can provide to decide whether to include a member or skip it.
*   **`get_archive_info() -> ArchiveInfo`**: Returns an `ArchiveInfo` object. This has general info about the archive itself, like its format, any comments, and whether it's a "solid" archive.
*   **`has_random_access() -> bool`**: Tells you if the archive lets you jump directly to any file (`True`) or if it's streaming-only (`False`). If it's `True`, you can use `open()` and `extract()` to get individual files easily.
*   **`get_member(member_or_filename: Union[ArchiveMember, str]) -> ArchiveMember`**: Fetches a specific `ArchiveMember` object. You can ask for it by its filename or by giving it an `ArchiveMember` object you already have (useful for checking if it's the same one).
*   **`open(member_or_filename: Union[ArchiveMember, str], ...) -> BinaryIO`**: Opens a specific file from the archive for reading and gives you back a binary I/O stream. This usually works if `has_random_access()` is `True`.
*   **`extract(member_or_filename: Union[ArchiveMember, str], ...) -> Optional[str]`**: Pulls a single file out of the archive and saves it to the `path` you specify (or the current directory if you don't give one). It returns the path where the file was saved. This also usually works if `has_random_access()` is `True`.
*   **`extractall(path: Optional[Union[str, os.PathLike]] = None, ...) -> dict[str, ArchiveMember]`**: Extracts all files (or a specific list of them) to the folder you give in `path`.
    *   `path`: Where do you want to put the extracted files? (Defaults to the current folder).
    *   `members`: You can give it a list of member names or `ArchiveMember` objects if you only want to extract certain ones.
    *   `pwd`: Password for encrypted files.
    *   `filter`: A function that gets called for each member, letting you decide if it should be extracted or skipped.
*   It returns a dictionary that maps the paths of the extracted files to their `ArchiveMember` objects.

**Heads up for streaming-only archives!** If `archive.has_random_access()` is `False`, you can typically only go through the files **once**. After you use `iter_members_with_io()` or `extractall()`, trying to read or extract more files will usually cause a `ValueError`.

## Working with Archive Members (Files & Folders)

The `ArchiveMember` object holds all the interesting details about an individual item in your archive, like its name, size, when it was last changed, its type (is it a file, a directory, a link?), and so on.

### Example: Listing What's Inside an Archive

Let's see how you can list the contents of an archive:

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("my_archive.tar.gz") as archive:
        print(f"Archive Format: {archive.get_archive_info().format.value}")
        if archive.has_random_access():
            print("This archive lets us jump around to any file!")
            members = archive.get_members()
            for member in members:
                print(f"- {member.filename} (Size: {member.file_size} bytes, Type: {member.type.value})")
        else:
            print("This archive is streaming-only. We'll read through it to see the members:")
            # When streaming, we get the member and its data stream together
            for member, stream in archive.iter_members_with_io():
                print(f"- {member.filename} (Size: {member.file_size} bytes, Type: {member.type.value})")
                if stream:
                    # It's good practice to close the stream if you're not going to read from it
                    stream.close()
except ArchiveError as e:
    print(f"Bummer, an error occurred: {e}")
```

## Tweaking Behavior with Configuration Options

`open_archive` can take an `ArchiveyConfig` object if you want to turn on some optional features or change how things work. You can create one and pass it in, or set a default one using `archivey.config.default_config()`.

```python
from archivey import open_archive, ArchiveyConfig
from archivey.api.types import OverwriteMode, ExtractionFilter # Assuming these are the correct paths

# Example: Create a config object
config = ArchiveyConfig(
    use_rar_stream=True,        # Try to use streaming for RAR if possible
    use_rapidgzip=True,         # Use rapidgzip if installed (for faster GZip)
    use_indexed_bzip2=True,     # Use indexed_bzip2 if installed
    overwrite_mode=OverwriteMode.OVERWRITE, # If a file exists, overwrite it
    extraction_filter=ExtractionFilter.TAR, # Use tar-like rules for which files to extract
)

# Use this config when opening an archive
with open_archive("file.rar", config=config) as archive:
    # ...do your thing...
    pass # Placeholder for your code
```

The `ArchiveyConfig` has settings that let `archivey` use other helpful libraries if you have them installed, like `rapidgzip`, `indexed_bzip2`, `python-xz`, and `zstandard`. Each of these needs the corresponding package to be installed on your system.
The `overwrite_mode` setting tells `archivey` what to do if it tries to extract a file but another file with the same name already exists. You can set it to `overwrite`, `skip`, or `error`.

The `extraction_filter` option gives you control over which files get extracted and how their paths are cleaned up. You can use one of the built-in `ExtractionFilter` settings:
*   `ExtractionFilter.DATA`: This is the default. It's designed to be safe for extracting typical data archives and might be a bit stricter about filenames or paths.
*   `ExtractionFilter.TAR`: This tries to act like the standard `tar` command, which can be more relaxed about what it extracts.
*   `ExtractionFilter.FULLY_TRUSTED`: This setting assumes you completely trust the source of the archive and does very little (or no) checking of filenames or paths. **Use this one with care!**
You can also provide your own custom function here. It should take an `ArchiveMember` and the destination path, and then return the `ArchiveMember` (possibly modified) if you want to extract it, or `None` to skip it.

### Example: Reading a Specific File from an Archive

Want to grab the contents of a particular file? Here's how:

```python
from archivey import open_archive, ArchiveMemberNotFoundError, ArchiveError

try:
    with open_archive("my_archive.zip") as archive:
        if not archive.has_random_access():
            print("This archive type doesn't let us open files directly without reading through. Try iterating instead.")
        else:
            try:
                file_i_want = "path/to/file_in_archive.txt"
                with archive.open(file_i_want) as f_stream:
                    content = f_stream.read()
                # Assuming it's a text file, let's decode and print it
                print(f"Content of '{file_i_want}':\n{content.decode()}")
            except ArchiveMemberNotFoundError:
                print(f"Sorry, couldn't find '{file_i_want}' in the archive.")
            except ArchiveError as e: # For other issues like password problems
                print(f"Error reading file: {e}")
except ArchiveError as e:
    print(f"Error opening archive: {e}")

```
### Example: Looping Through Files with `iter_members_with_io`

The `iter_members_with_io` method is great for processing archive members one by one. A cool thing is that each file's stream is automatically closed when the loop moves to the next file, or when you're done with the loop.

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("my_archive.tar") as archive:
        for member, stream in archive.iter_members_with_io():
            print(f"Looking at: {member.filename}")
            if stream: # It's a file, so there's a stream
                data = stream.read() # Read its content
                print(f"  Read {len(data)} bytes from it.")
            # No need to manually close the stream, archivey handles it!
except ArchiveError as e:
    print(f"Something went wrong: {e}")
```


### Example: Extracting an Entire Archive

Need to get everything out? `extractall` is your friend:

```python
from archivey import open_archive, ArchiveError
import os

DESTINATION_DIR = "my_extracted_stuff" # Where the files will go

try:
    with open_archive("my_archive.rar") as archive:
        archive_path_display = archive.archive_path if isinstance(archive.archive_path, str) else "archive"
        print(f"Getting all files from {archive_path_display} and putting them in {DESTINATION_DIR}...")

        # Let's make the destination directory if it doesn't exist
        if not os.path.exists(DESTINATION_DIR):
            os.makedirs(DESTINATION_DIR)

        extracted_files = archive.extractall(path=DESTINATION_DIR)
        print(f"Successfully pulled out {len(extracted_files)} files:")
        for path, member in extracted_files.items():
            print(f"  - Saved '{member.filename}' to '{path}'")

except ArchiveError as e:
    print(f"Oh no, an error: {e}")
```

This guide gives you a taste of what `archivey` can do. For all the nitty-gritty details on specific classes and methods, check out the [API documentation](./api/archivey.html). Happy archiving!
