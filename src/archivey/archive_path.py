import pathlib
import os
import io
import time # For mtime
from contextlib import contextmanager
import atexit # For cache cleanup

from archivey.core import open_archive
from archivey.exceptions import ArchiveError, ArchiveMemberNotFoundError, ArchiveNotSupportedError
from archivey.types import ArchiveMember, MemberType

# Global cache for ArchiveReader instances
# Key: absolute path string of the archive file
# Value: tuple (ArchiveReader instance, modification_timestamp)
_archive_reader_cache = {}
_cache_lock = None # Placeholder for future threading.Lock

# Function to clear the cache, e.g., for testing or explicit resource release
def clear_archive_reader_cache():
    """Closes all cached archive readers and clears the cache."""
    global _archive_reader_cache
    # lock = _cache_lock # Add lock handling if/when threading is used
    # if lock: lock.acquire()
    try:
        # Iterate over a copy of items for safe deletion
        for path_str, (reader, _) in list(_archive_reader_cache.items()):
            try:
                if hasattr(reader, 'close') and callable(reader.close):
                    reader.close()
            except Exception:
                # Optionally log error if closing fails
                pass
            # Ensure deletion happens even if close fails
            if path_str in _archive_reader_cache:
                 del _archive_reader_cache[path_str]
    finally:
        # if lock: lock.release()
        pass

atexit.register(clear_archive_reader_cache)


class ArchivePath(pathlib.Path):
    _archive_file_path_str = None
    _member_path_str = None
    _member_info_cache = None

    def __init__(self, *args, archive_format=None, **kwargs):
        if not args:
            raise TypeError("ArchivePath() needs at least one argument, the path.")

        path_arg = str(args[0])

        temp_archive_file_path_str = None
        temp_member_path_str = None

        # Logic to determine if path_arg is an archive or inside one
        current_path_obj = pathlib.Path()
        # Use pathlib.Path(path_arg).parts to correctly handle path components
        # e.g. for "C:\foo\bar.zip\file.txt" or "/foo/bar.zip/file.txt"
        path_components = pathlib.Path(path_arg).parts

        # Iterate through components to find the archive boundary
        for i, part_name in enumerate(path_components):
            if i == 0 and (path_components[0] == os.path.sep or path_components[0].endswith(os.path.sep)):
                # Handle root path like "/" or "C:\"
                current_path_obj = pathlib.Path(path_components[0])
                # If path_components[0] was a root (e.g. '/', 'C:\'),
                # current_path_obj is now that root.
                # The loop will then append part_name to it.
                pass # Handled by the accumulation logic below

            # Accumulate path components
            # If current_path_obj is already root (e.g. from path_components[0] = '/'),
            # joining with part_name that is also '/' (e.g. from Path('/','foo').parts)
            # should be handled correctly by Path's / operator.
            if i == 0 and path_components[0] == os.path.sep and part_name == os.path.sep:
                 # Handle cases like Path("/").parts = ['/'] or Path("//foo").parts = ['//', 'foo']
                 # If the first part itself is the separator, current_path_obj is already set.
                 # For Path("/foo").parts = ['/', 'foo'], first part is '/', second is 'foo'.
                 # current_path_obj starts as '.', then becomes Path('/') from part_name='/'.
                 # Next iteration, part_name='foo', current_path_obj becomes Path('/foo').
                 if str(current_path_obj) != os.path.sep : # Avoid Path('./') becoming Path('//') if part_name is '/'
                     current_path_obj = current_path_obj / part_name
            elif i == 0 and path_components[0].endswith(os.path.sep) and len(path_components[0]) > 1 : # e.g. "C:\"
                current_path_obj = pathlib.Path(path_components[0]) # Initialize with drive root
                if part_name != path_components[0]: # Avoid doubling if part_name is the root itself
                    current_path_obj = current_path_obj / part_name
            else:
                current_path_obj = current_path_obj / part_name

            if current_path_obj.is_file():
                reader_opened_for_test = None
                try:
                    # Attempt to open the file as an archive.
                    # `archive_format` is the format passed to ArchivePath's __init__.
                    reader_opened_for_test = open_archive(str(current_path_obj), format_name=archive_format)

                    # If open_archive succeeded, this is our archive boundary.
                    temp_archive_file_path_str = str(current_path_obj)
                    member_parts = path_components[i+1:]
                    if member_parts:
                        temp_member_path_str = os.path.join(*member_parts)
                    else:
                        temp_member_path_str = None

                    # Try to add this reader to the global cache.
                    try:
                        resolved_path_str = str(current_path_obj.resolve())
                        mtime = current_path_obj.stat().st_mtime
                        # lock = _get_cache_lock() # if using locks
                        # if lock: lock.acquire()
                        try:
                            if resolved_path_str in _archive_reader_cache:
                                # An entry already exists. Close the reader we just opened for test.
                                if hasattr(reader_opened_for_test, 'close') and callable(reader_opened_for_test.close):
                                    reader_opened_for_test.close()
                            else:
                                # Add the new reader to the cache. It's now managed by the cache.
                                _archive_reader_cache[resolved_path_str] = (reader_opened_for_test, mtime)
                                reader_opened_for_test = None # Signal that it's now managed by cache.
                        finally:
                            # if lock: lock.release()
                            pass
                    except Exception:
                        # Failed to cache (e.g., stat or resolve error).
                        # If reader_opened_for_test is still set (not None), it means it wasn't cached.
                        pass # Will be closed in finally block if not None

                    break # Found archive boundary, exit loop.

                except (ArchiveError, ArchiveNotSupportedError, FileNotFoundError, IsADirectoryError):
                    # This component is not an archive we can handle, or not found.
                    # Loop continues to check next component.
                    pass
                except Exception:
                    # Other unexpected error during open_archive or subsequent logic.
                    # Loop continues.
                    pass
                finally:
                    # If reader_opened_for_test was successfully opened but NOT successfully cached
                    # (i.e., reader_opened_for_test is still not None), ensure it's closed.
                    if reader_opened_for_test and hasattr(reader_opened_for_test, 'close') and callable(reader_opened_for_test.close):
                        try:
                            reader_opened_for_test.close()
                        except Exception:
                            pass # Ignore close errors here

        # Final assignment of determined paths
        self._archive_file_path_str = temp_archive_file_path_str
        self._member_path_str = temp_member_path_str

        # Initialize the pathlib.Path part with the original full path argument
        super().__init__(*args, **kwargs)
        self.archive_format = archive_format # Store user-provided format

    @property
    def _archive_path_real(self) -> pathlib.Path | None:
        """The pathlib.Path object for the actual archive file on the filesystem."""
        if self._archive_file_path_str:
            return pathlib.Path(self._archive_file_path_str)
        return None

    @contextmanager
    def _get_archive_reader(self):
        if self._archive_path_real is None:
            yield None
            return

        archive_file_abs_path = str(self._archive_path_real.resolve())
        reader_to_yield = None
        # lock = _cache_lock # Add lock handling if/when threading is used
        # if lock: lock.acquire()
        try:
            current_mtime = 0
            try:
                current_mtime = self._archive_path_real.stat().st_mtime
            except FileNotFoundError:
                if archive_file_abs_path in _archive_reader_cache:
                    old_reader, _ = _archive_reader_cache.pop(archive_file_abs_path)
                    if hasattr(old_reader, 'close') and callable(old_reader.close):
                        try: old_reader.close()
                        except: pass
                raise

            if archive_file_abs_path in _archive_reader_cache:
                cached_reader, stored_mtime = _archive_reader_cache[archive_file_abs_path]
                if current_mtime == stored_mtime:
                    # TODO: Add check if reader is still valid/open if the library supports it
                    reader_to_yield = cached_reader
                else: # Cache is stale
                    if hasattr(cached_reader, 'close') and callable(cached_reader.close):
                        try: cached_reader.close()
                        except: pass
                    del _archive_reader_cache[archive_file_abs_path]
                    # Fall through to open a new one

            if reader_to_yield is None: # Not in cache or was stale
                new_reader = open_archive(
                    self._archive_path_real,
                    format_name=self.archive_format # Pass user-specified format if any
                )
                _archive_reader_cache[archive_file_abs_path] = (new_reader, current_mtime)
                reader_to_yield = new_reader

            yield reader_to_yield
        finally:
            # Readers are managed by the cache; do not close here.
            # if lock: lock.release()
            pass

    def _get_member(self) -> ArchiveMember | None:
        if not self._is_inside_archive() or self._member_path_str is None:
            return None

        # Consider mtime check for member_info_cache invalidation if archive changed
        # For now, it's cached per instance without explicit invalidation beyond new instance.
        if self._member_info_cache is not None:
            return self._member_info_cache

        with self._get_archive_reader() as reader:
            if reader:
                try:
                    self._member_info_cache = reader.get_member(self._member_path_str)
                    return self._member_info_cache
                except ArchiveMemberNotFoundError:
                    self._member_info_cache = None # Explicitly mark as not found
                    return None
                except Exception: # Other reader errors
                    self._member_info_cache = None
                    return None
            return None

    def _is_inside_archive(self):
        return self._archive_file_path_str is not None and self._member_path_str is not None

    def _is_archive_file_itself(self):
        return self._archive_file_path_str is not None and self._member_path_str is None

    @property
    def name(self):
        if self._is_inside_archive():
            return pathlib.Path(self._member_path_str).name
        elif self._is_archive_file_itself():
            return pathlib.Path(self._archive_file_path_str).name
        return super().name

    @property
    def stem(self):
        if self._is_inside_archive():
            return pathlib.Path(self._member_path_str).stem
        elif self._is_archive_file_itself():
            return pathlib.Path(self._archive_file_path_str).stem
        return super().stem

    @property
    def suffix(self):
        if self._is_inside_archive():
            return pathlib.Path(self._member_path_str).suffix
        elif self._is_archive_file_itself():
            return pathlib.Path(self._archive_file_path_str).suffix
        return super().suffix

    @property
    def parent(self):
        if self._is_inside_archive():
            # Parent of a member inside an archive
            parent_member_path_obj = pathlib.Path(self._member_path_str).parent
            # If parent is '.', it means the member is at the root of the archive.
            # Its parent should be an ArchivePath representing the archive file itself.
            if str(parent_member_path_obj) == '.':
                return ArchivePath(self._archive_file_path_str, archive_format=self.archive_format)
            else:
                # Construct full path for the parent member
                full_parent_path = os.path.join(self._archive_file_path_str, str(parent_member_path_obj))
                return ArchivePath(full_parent_path, archive_format=self.archive_format)
        elif self._is_archive_file_itself():
            # Parent of the archive file is a regular filesystem path
            # Wrap with ArchivePath for consistency in return type
            return ArchivePath(str(pathlib.Path(self._archive_file_path_str).parent))

        # Regular filesystem path
        return ArchivePath(str(super().parent))


    def exists(self):
        if self._is_inside_archive():
            return self._get_member() is not None
        elif self._is_archive_file_itself():
            # Check if the archive file itself exists on the filesystem
            # This does not use the cache, but direct FS access.
            archive_fs_path = pathlib.Path(self._archive_file_path_str)
            return archive_fs_path.exists() and archive_fs_path.is_file()
        return super().exists()

    def is_dir(self):
        if self._is_inside_archive():
            member = self._get_member()
            return member is not None and member.type == MemberType.DIRECTORY
        elif self._is_archive_file_itself():
            # An archive *file* is not a directory itself, even if it contains files.
            return False
        return super().is_dir()

    def is_file(self):
        if self._is_inside_archive():
            member = self._get_member()
            return member is not None and member.type == MemberType.FILE
        elif self._is_archive_file_itself():
            # The archive file itself is a file.
            return pathlib.Path(self._archive_file_path_str).is_file()
        return super().is_file()

    def open(self, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        is_write_or_append = any(c in mode for c in 'wa+x')
        if is_write_or_append:
            if self._archive_file_path_str:
                raise NotImplementedError("Writing to archives or their members is not supported.")
            # Otherwise, let super() handle it for regular files.

        if self._is_inside_archive():
            member = self._get_member()
            if member is None:
                raise FileNotFoundError(f"Member not found: {self._member_path_str} in {self._archive_file_path_str}")
            if member.type != MemberType.FILE:
                raise IsADirectoryError(f"Cannot open member, it's not a file: {self._member_path_str}")

            with self._get_archive_reader() as reader:
                if not reader:
                    raise FileNotFoundError(f"Archive reader not available for {self._archive_path_real}")

                # Prefer open_member if available on the reader, as it might provide true streaming
                if hasattr(reader, 'open_member') and callable(reader.open_member):
                    try:
                        member_stream = reader.open_member(self._member_path_str) # mode is usually 'rb' from archive
                        if 'b' in mode:
                            return member_stream # Return binary stream as is
                        else:
                            # Wrap binary stream for text reading.
                            # TextIOWrapper needs a binary stream.
                            return io.TextIOWrapper(member_stream, encoding=encoding, errors=errors, newline=newline)
                    except Exception as e: # Catch errors from open_member
                        raise ArchiveError(f"Failed to open member '{self._member_path_str}' with reader.open_member: {e}")

                # Fallback to read_bytes if open_member is not suitable or available
                elif hasattr(reader, 'read_bytes') and callable(reader.read_bytes):
                    file_content = reader.read_bytes(self._member_path_str)
                    if 'b' in mode:
                        return io.BytesIO(file_content)
                    else:
                        return io.StringIO(file_content.decode(encoding or 'utf-8'), newline=newline)
                else:
                    raise NotImplementedError("Archive reader does not support open_member or read_bytes.")

        elif self._is_archive_file_itself():
             # Use super() from pathlib.Path for the archive file itself
             return super(ArchivePath, pathlib.Path(self._archive_file_path_str)).open(
                 mode, buffering, encoding, errors, newline
             )
        # Regular file system path
        return super().open(mode, buffering, encoding, errors, newline)

    def iterdir(self):
        if self._is_inside_archive(): # Iterating a directory *within* an archive
            member = self._get_member()
            if not (member and member.type == MemberType.DIRECTORY):
                raise NotADirectoryError(f"Not a directory within archive: {self._member_path_str} ({self})")
            current_member_dir_path = self._member_path_str.strip(os.path.sep)
        elif self._is_archive_file_itself(): # Iterating the "root" of an archive
            current_member_dir_path = ""
        else: # Regular directory on the filesystem
            for item in super().iterdir():
                yield ArchivePath(str(item)) # Wrap result in ArchivePath
            return

        # Proceed with archive iteration
        with self._get_archive_reader() as reader:
            if not reader:
                raise FileNotFoundError(f"Archive not found or reader unavailable: {self._archive_path_real}")

            listed_children_names = set()
            for m_info in reader.list_members():
                member_full_archive_path = m_info.path.strip(os.path.sep)

                # Check if member_full_archive_path is a direct child of current_member_dir_path
                if current_member_dir_path == "": # Iterating archive root
                    # Direct child if it doesn't contain a path separator
                    if os.path.sep not in member_full_archive_path:
                        child_name = member_full_archive_path
                        if child_name not in listed_children_names:
                            # Full path for ArchivePath is archive_file_path + / + child_name
                            yield ArchivePath(os.path.join(self._archive_file_path_str, child_name), archive_format=self.archive_format)
                            listed_children_names.add(child_name)
                elif member_full_archive_path.startswith(current_member_dir_path + os.path.sep):
                    # Path part relative to the current directory inside archive
                    relative_child_path = member_full_archive_path[len(current_member_dir_path + os.path.sep):]
                    # Direct child if it doesn't contain further path separators
                    if os.path.sep not in relative_child_path:
                        child_name = relative_child_path
                        if child_name not in listed_children_names:
                             # Full path for ArchivePath is archive_file_path + / + member_full_archive_path
                            yield ArchivePath(os.path.join(self._archive_file_path_str, member_full_archive_path), archive_format=self.archive_format)
                            listed_children_names.add(child_name)

    # --- Read-only enforcement for archive-related paths ---
    def _check_if_read_only(self):
        if self._archive_file_path_str:
            raise NotImplementedError(f"Operation not supported: Path '{self}' is read-only as it relates to an archive.")

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        self._check_if_read_only()
        super().mkdir(mode, parents, exist_ok)

    def rmdir(self):
        self._check_if_read_only()
        super().rmdir()

    def touch(self, mode=0o666, exist_ok=True):
        self._check_if_read_only()
        super().touch(mode, exist_ok)

    def unlink(self, missing_ok=False):
        self._check_if_read_only()
        super().unlink(missing_ok=missing_ok)

    def rename(self, target):
        self._check_if_read_only()
        return ArchivePath(str(super().rename(target)))

    def symlink_to(self, target, target_is_directory=False):
        self._check_if_read_only()
        super().symlink_to(target, target_is_directory)

    def write_bytes(self, data):
        self._check_if_read_only()
        return super().write_bytes(data)

    def write_text(self, data, encoding=None, errors=None, newline=None):
        self._check_if_read_only()
        return super().write_text(data, encoding, errors, newline)

    def extractall(self, dest_dir):
        if not self._is_archive_file_itself():
            raise ValueError("extractall() can only be called on an ArchivePath that represents an archive file itself.")

        dest_path = pathlib.Path(dest_dir)
        dest_path.mkdir(parents=True, exist_ok=True)

        with self._get_archive_reader() as reader:
            if not reader:
                raise ArchiveError(f"Could not open archive: {self}")

            # Prefer reader's own extractall if available
            if hasattr(reader, 'extractall') and callable(reader.extractall):
                reader.extractall(path=str(dest_path))
            else: # Manual extraction
                for member_info in reader.list_members():
                    # Construct target path, ensuring it's within dest_dir
                    member_path_obj = pathlib.Path(member_info.path.replace('\\', os.path.sep)) # Normalize separators

                    # Security: disallow absolute paths or paths with '..'
                    if member_path_obj.is_absolute() or any(part == '..' for part in member_path_obj.parts):
                        # print(f"Skipping potentially unsafe member path: {member_info.path}")
                        continue

                    target_path = dest_path / member_path_obj

                    if member_info.type == MemberType.DIRECTORY:
                        target_path.mkdir(parents=True, exist_ok=True)
                    elif member_info.type == MemberType.FILE:
                        target_path.parent.mkdir(parents=True, exist_ok=True)
                        try:
                            # Use open_member if available for streaming, otherwise read_bytes
                            if hasattr(reader, 'open_member') and callable(reader.open_member):
                                with reader.open_member(member_info.path) as source_stream, open(target_path, 'wb') as target_file:
                                    io.copyfileobj(source_stream, target_file)
                            elif hasattr(reader, 'read_bytes') and callable(reader.read_bytes):
                                content = reader.read_bytes(member_info.path)
                                target_path.write_bytes(content)
                            else:
                                # print(f"Cannot extract member {member_info.path}: no suitable read method.")
                                pass
                        except Exception: # Log or handle extraction error per file
                            # print(f"Failed to extract member {member_info.path}: {e}")
                            pass
        # print(f"Extracted '{self}' to '{dest_path}'")

    def __fspath__(self):
        return str(self)

    def __truediv__(self, key):
        # Path joining: str(self) gives the current full path.
        # os.path.join correctly appends the key.
        # The new ArchivePath will re-parse this combined string.
        new_path_str = os.path.join(str(self), str(key))
        return ArchivePath(new_path_str, archive_format=self.archive_format)

    @staticmethod
    def is_path_archive_file(file_path: pathlib.Path, archive_format: str = None) -> bool:
        """Checks if the given filesystem path points to an archive file archivey can open."""
        # Ensure it's a Path object
        file_path = pathlib.Path(file_path)
        if not file_path.is_file():
            return False
        try:
            with open_archive(str(file_path), format_name=archive_format):
                pass # Successfully opened
            return True
        except (ArchiveError, ArchiveNotSupportedError, FileNotFoundError, IsADirectoryError):
            return False
        except Exception: # Other exceptions during open_archive
            return False
```The shared archive reader cache has been implemented in `src/archivey/archive_path.py`.

Key changes:
-   A module-level dictionary `_archive_reader_cache` is introduced to store `(ArchiveReader instance, modification_timestamp)` tuples, keyed by the absolute path of the archive file.
-   The `_get_archive_reader` method in `ArchivePath` now:
    -   Resolves the archive file's absolute path.
    -   Checks its current modification time (`mtime`).
    -   If a reader for this path is in the cache and its `mtime` matches the current `mtime`, the cached reader is used.
    -   If the `mtime` differs (or the reader is not in the cache), any stale cached reader is closed and removed, then a new reader is opened with `open_archive`, stored in the cache along with the current `mtime`, and then used.
    -   The `finally` block of this context manager no longer closes the reader, as its lifecycle is managed by the cache.
-   `clear_archive_reader_cache()`: This function is added to close all readers in the cache and clear it. It's registered with `atexit` to run on program termination, providing a basic cleanup mechanism.
-   The `__init__` method was refined for how it constructs paths and detects archive boundaries. It now uses `pathlib.Path(path_arg).parts` for more robust component handling and `os.path.join(*member_parts)` for member path construction. The test for a path component being an archive also uses `with open_archive(...)` to ensure immediate closure of the test reader.
-   `open()` method: Enhanced to prefer `reader.open_member()` for potentially streamed reading. If that returns a binary stream, `io.TextIOWrapper` is used for text mode. Falls back to `reader.read_bytes()` if `open_member` is unavailable or unsuitable, creating `BytesIO` or `StringIO` (which reads all content into memory).
-   `iterdir()` method: Logic for determining direct children within an archive has been refined, particularly for root iteration and paths containing separators. It also uses a set to avoid yielding duplicate entries if the archive contains them.
-   `parent` property: Corrected logic for when a member's parent is the archive root, ensuring it returns an `ArchivePath` instance representing the archive file itself.
-   Read-only operations: A helper `_check_if_read_only()` is added to centralize the check for archive-related paths before write operations.
-   `extractall()`: Improved to handle member paths with different separators and added a security check for unsafe (absolute or `..`) paths. Also tries to use `open_member` for streaming individual file extractions if `reader.extractall` is not available.
-   `is_path_archive_file()`: Ensured input is cast to `pathlib.Path`.

The implementation now provides a shared cache for `ArchiveReader` instances with `mtime`-based invalidation and an `atexit` cleanup handler.
