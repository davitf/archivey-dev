import pytest
import pathlib
import shutil
import os
import time
import zipfile
import tarfile

from archivey import ArchivePath # Assuming archivey is importable and __init__ is set up
from archivey.archive_path import _archive_reader_cache, clear_archive_reader_cache
from archivey.core import open_archive # For mocking or direct use if needed for tests

# Define the paths to the source test archives I created earlier
# These are relative to the repository root.
SOURCE_TEST_ARCHIVES_DIR = pathlib.Path("tests/test_archives")
SAMPLE_ZIP_SOURCE = SOURCE_TEST_ARCHIVES_DIR / "sample.zip"
SAMPLE_TAR_GZ_SOURCE = SOURCE_TEST_ARCHIVES_DIR / "sample.tar.gz"
EMPTY_ZIP_SOURCE = SOURCE_TEST_ARCHIVES_DIR / "empty.zip"
TEXT_ARCHIVE_SOURCE = SOURCE_TEST_ARCHIVES_DIR / "text_archive.txt" # Not an archive


@pytest.fixture(autouse=True)
def clear_cache_before_after_each_test():
    """Ensure the global archive reader cache is clear before and after each test."""
    clear_archive_reader_cache()
    yield
    clear_archive_reader_cache()

@pytest.fixture
def test_files_dir(tmp_path_factory):
    """Create a temporary directory for test files and archives for a single test."""
    temp_dir = tmp_path_factory.mktemp("test_archive_path_files")

    # Copy the sample archives I created into this temp dir for the test to use
    shutil.copy(SAMPLE_ZIP_SOURCE, temp_dir / SAMPLE_ZIP_SOURCE.name)
    shutil.copy(SAMPLE_TAR_GZ_SOURCE, temp_dir / SAMPLE_TAR_GZ_SOURCE.name)
    shutil.copy(EMPTY_ZIP_SOURCE, temp_dir / EMPTY_ZIP_SOURCE.name)
    shutil.copy(TEXT_ARCHIVE_SOURCE, temp_dir / TEXT_ARCHIVE_SOURCE.name)

    # Create some regular files and directories in the temp_dir for testing non-archive paths
    (temp_dir / "regular_dir").mkdir()
    (temp_dir / "regular_dir" / "file_in_reg_dir.txt").write_text("Regular file in regular dir.")
    (temp_dir / "regular_file.txt").write_text("This is a regular file.")

    return temp_dir

# --- Test Cases Start Here ---

# Group 1: Basic Path Operations (Regular Files/Dirs)
def test_regular_file_properties(test_files_dir):
    reg_file_path_str = str(test_files_dir / "regular_file.txt")
    p = ArchivePath(reg_file_path_str)

    assert p.name == "regular_file.txt"
    assert p.stem == "regular_file"
    assert p.suffix == ".txt"
    assert p.parent.name == test_files_dir.name
    assert p.exists()
    assert p.is_file()
    assert not p.is_dir()

def test_regular_directory_properties(test_files_dir):
    reg_dir_path_str = str(test_files_dir / "regular_dir")
    p = ArchivePath(reg_dir_path_str)

    assert p.name == "regular_dir"
    # Stem/suffix for directories can be a bit ambiguous in pathlib, depends on trailing slash interpretation
    # For a path string without trailing slash, it's usually dir name and empty suffix.
    assert p.stem == "regular_dir"
    assert p.suffix == ""
    assert p.parent.name == test_files_dir.name
    assert p.exists()
    assert p.is_dir()
    assert not p.is_file()

def test_non_existent_regular_path(test_files_dir):
    p = ArchivePath(test_files_dir / "non_existent_file.txt")
    assert not p.exists()
    assert p.name == "non_existent_file.txt"


# Group 2: Archive Handling (Paths to Archives and Members)
def test_archive_file_itself_properties_zip(test_files_dir):
    archive_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name)
    p = ArchivePath(archive_path_str)

    assert p.name == SAMPLE_ZIP_SOURCE.name
    assert p.stem == SAMPLE_ZIP_SOURCE.stem
    assert p.suffix == SAMPLE_ZIP_SOURCE.suffix
    assert p.exists() # The archive file itself exists
    assert p.is_file() # The archive IS a file
    assert not p.is_dir() # It's not a directory
    assert p._is_archive_file_itself()
    assert not p._is_inside_archive()

def test_archive_file_itself_properties_tar_gz(test_files_dir):
    archive_path_str = str(test_files_dir / SAMPLE_TAR_GZ_SOURCE.name)
    p = ArchivePath(archive_path_str)

    assert p.name == SAMPLE_TAR_GZ_SOURCE.name
    # pathlib behavior for ".tar.gz": suffix is ".gz", stem is "sample.tar"
    assert p.stem == "sample.tar"
    assert p.suffix == ".gz"
    assert p.suffixes == ['.tar', '.gz'] # Check multiple suffixes
    assert p.exists()
    assert p.is_file()
    assert not p.is_dir()
    assert p._is_archive_file_itself()
    assert not p._is_inside_archive()

def test_path_to_member_in_zip_properties(test_files_dir):
    # Path like "sample.zip/file1.txt"
    member_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name / "file1.txt")
    p = ArchivePath(member_path_str)

    assert p.name == "file1.txt"
    assert p.stem == "file1"
    assert p.suffix == ".txt"
    assert p.parent.name == SAMPLE_ZIP_SOURCE.name # Parent is the archive file itself
    assert p.exists()
    assert p.is_file()
    assert not p.is_dir()
    assert p._is_inside_archive()
    assert p._archive_file_path_str == str(test_files_dir / SAMPLE_ZIP_SOURCE.name)
    assert p._member_path_str == "file1.txt"

def test_path_to_dir_member_in_zip_properties(test_files_dir):
    # Path like "sample.zip/dir1/"
    # Note: ArchivePath's __init__ might normalize trailing slashes.
    # For a member path, os.path.join is used, which might strip it.
    # Let's test with and without trailing slash if ArchivePath handles it.
    # For now, assume no trailing slash for member_path_str.
    dir_member_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name / "dir1")
    p_dir = ArchivePath(dir_member_path_str)

    assert p_dir.name == "dir1"
    assert p_dir.stem == "dir1"
    assert p_dir.suffix == ""
    assert p_dir.parent.name == SAMPLE_ZIP_SOURCE.name
    assert p_dir.exists()
    assert p_dir.is_dir()
    assert not p_dir.is_file()
    assert p_dir._is_inside_archive()
    assert p_dir._archive_file_path_str == str(test_files_dir / SAMPLE_ZIP_SOURCE.name)
    assert p_dir._member_path_str == "dir1"

def test_path_to_nested_member_in_zip_properties(test_files_dir):
    # Path like "sample.zip/dir1/file2.txt"
    nested_member_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name / "dir1" / "file2.txt")
    p_nested = ArchivePath(nested_member_path_str)

    assert p_nested.name == "file2.txt"
    assert p_nested.stem == "file2"
    assert p_nested.suffix == ".txt"
    assert p_nested.parent.name == "dir1" # Parent is dir1 inside archive
    assert p_nested.parent._member_path_str == "dir1"
    assert p_nested.exists()
    assert p_nested.is_file()
    assert not p_nested.is_dir()
    assert p_nested._is_inside_archive()

def test_non_existent_member_in_zip(test_files_dir):
    p = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "non_existent.txt")
    assert not p.exists()
    assert p.name == "non_existent.txt"
    assert p._is_inside_archive() # It's still an archive path, just points to nothing

def test_path_looks_like_archive_but_is_not(test_files_dir):
    # text_archive.txt is a plain text file, not a zip/tar.
    # So, text_archive.txt/some_file.txt should NOT be treated as an archive member path.
    # It should be treated as a regular path (that likely doesn't exist).
    p = ArchivePath(test_files_dir / TEXT_ARCHIVE_SOURCE.name / "some_file.txt")

    assert p.name == "some_file.txt"
    assert not p.exists() # "/path/to/text_archive.txt/some_file.txt" doesn't exist as regular path
    assert not p._is_inside_archive() # Crucial: it should not think it's inside an archive
    assert not p._is_archive_file_itself()
    assert p._archive_file_path_str is None
    # This tests that the __init__ logic correctly identifies non-archives.
    # For this to work, text_archive.txt must NOT be openable by archivey.core.open_archive
    # (which is true for a plain text file).

# More tests will follow for iterdir, open, traversal, read-only, cache, extractall, edge cases.
# This is a good start for the first block.
# I will also add a test for a path that is just the archive file name, with a specific archive_format hint.

def test_archive_file_with_format_hint(test_files_dir):
    # Test providing archive_format hint for a file that might have ambiguous extension
    # For this test, let's use sample.zip but pretend its extension isn't obvious.
    # We can't easily rename it here without affecting other source paths.
    # Instead, let's ensure the format hint is passed to open_archive.
    # This is more of an integration test for the hint.
    # We can mock open_archive to check if it was called with the format hint.

    # For now, just test that creating ArchivePath with a format doesn't break.
    archive_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name)
    p = ArchivePath(archive_path_str, archive_format="zip") # Provide hint

    assert p.name == SAMPLE_ZIP_SOURCE.name
    assert p.exists()
    assert p.is_file()
    assert p._is_archive_file_itself()
    assert p.archive_format == "zip" # Check the attribute is set

    # A deeper test would involve mocking 'archivey.core.open_archive'
    # from unittest.mock import patch
    # with patch('archivey.archive_path.open_archive') as mock_open_archive:
    #     p_test = ArchivePath(archive_path_str, archive_format="zip")
    #     # Trigger an operation that calls _get_archive_reader -> open_archive
    #     p_test.is_file() # or p_test.exists() if it implies opening
    #     # Need to ensure the mock is called for the cache fill in __init__ or _get_archive_reader
    # This kind of mocking is more advanced, will add if time permits or if specific issues arise.


# Group 3: iterdir() tests
class TestIterdir:
    def test_iterdir_regular_directory(self, test_files_dir):
        reg_dir_path = ArchivePath(test_files_dir / "regular_dir")
        contents = list(reg_dir_path.iterdir())

        assert len(contents) == 1
        child_names = {item.name for item in contents}
        assert "file_in_reg_dir.txt" in child_names
        for item in contents:
            assert isinstance(item, ArchivePath)

    def test_iterdir_archive_root_zip(self, test_files_dir):
        archive_path = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name)
        contents = list(archive_path.iterdir())

        # sample.zip contains: file1.txt, dir1/ (which has file2.txt)
        # iterdir on root should list 'file1.txt' and 'dir1'
        assert len(contents) >= 2 # Could be more if zip adds other hidden files
        child_names = {item.name for item in contents}
        assert "file1.txt" in child_names
        assert "dir1" in child_names
        for item in contents:
            assert isinstance(item, ArchivePath)
            assert item._is_inside_archive() or item._is_archive_file_itself() # children are members
            assert item._archive_file_path_str == str(archive_path)

    def test_iterdir_archive_root_tar_gz(self, test_files_dir):
        archive_path = ArchivePath(test_files_dir / SAMPLE_TAR_GZ_SOURCE.name)
        contents = list(archive_path.iterdir())

        # sample.tar.gz contains: fileA.txt, dirA/ (which has fileB.txt)
        assert len(contents) >= 2
        child_names = {item.name for item in contents}
        assert "fileA.txt" in child_names
        assert "dirA" in child_names
        for item in contents:
            assert isinstance(item, ArchivePath)
            assert item._archive_file_path_str == str(archive_path)

    def test_iterdir_archive_subdirectory_zip(self, test_files_dir):
        # Path to sample.zip/dir1
        subdir_path_str = str(test_files_dir / SAMPLE_ZIP_SOURCE.name / "dir1")
        p_subdir = ArchivePath(subdir_path_str)
        contents = list(p_subdir.iterdir())

        assert len(contents) == 1
        child_names = {item.name for item in contents}
        assert "file2.txt" in child_names
        for item in contents:
            assert isinstance(item, ArchivePath)
            assert item._is_inside_archive()
            assert item._archive_file_path_str == str(test_files_dir / SAMPLE_ZIP_SOURCE.name)
            assert item._member_path_str.startswith("dir1/")

    def test_iterdir_empty_archive_root(self, test_files_dir): # Using empty.zip
        empty_archive_path = ArchivePath(test_files_dir / EMPTY_ZIP_SOURCE.name)
        contents = list(empty_archive_path.iterdir())
        assert len(contents) == 0

    def test_iterdir_regular_empty_directory(self, test_files_dir):
        empty_reg_dir = test_files_dir / "empty_regular_dir"
        empty_reg_dir.mkdir()
        p_empty_reg = ArchivePath(empty_reg_dir)
        contents = list(p_empty_reg.iterdir())
        assert len(contents) == 0

    def test_iterdir_on_regular_file_raises_error(self, test_files_dir):
        reg_file_path = ArchivePath(test_files_dir / "regular_file.txt")
        with pytest.raises(NotADirectoryError):
            list(reg_file_path.iterdir())

    def test_iterdir_on_file_in_archive_raises_error(self, test_files_dir):
        # sample.zip/file1.txt
        member_file_path = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "file1.txt")
        with pytest.raises(NotADirectoryError):
            list(member_file_path.iterdir())


# Group 4: open() tests
class TestOpen:
    def test_open_regular_file_read_modes(self, test_files_dir):
        reg_file_path = ArchivePath(test_files_dir / "regular_file.txt")
        expected_content = "This is a regular file."

        # Binary mode
        with reg_file_path.open('rb') as f:
            content_binary = f.read()
            assert content_binary == expected_content.encode('utf-8')

        # Text mode
        with reg_file_path.open('rt', encoding='utf-8') as f:
            content_text = f.read()
            assert content_text == expected_content

    def test_open_file_in_zip_read_modes(self, test_files_dir):
        # sample.zip/file1.txt
        member_file_path = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "file1.txt")
        expected_content = "This is file1." # Content from archive creation script

        # Binary mode
        with member_file_path.open('rb') as f:
            content_binary = f.read()
            assert content_binary.strip() == expected_content.encode('utf-8').strip() # Strip for potential line ending issues in zip

        # Text mode
        with member_file_path.open('rt', encoding='utf-8') as f:
            content_text = f.read()
            assert content_text.strip() == expected_content.strip()

    def test_open_nested_file_in_zip_read_modes(self, test_files_dir):
        # sample.zip/dir1/file2.txt
        nested_file_path = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "dir1" / "file2.txt")
        expected_content = "This is file2 in dir1."

        with nested_file_path.open('rb') as f:
            assert f.read().strip() == expected_content.encode('utf-8').strip()

        with nested_file_path.open('rt', encoding='utf-8') as f:
            assert f.read().strip() == expected_content.strip()

    def test_open_file_in_tar_gz_read_modes(self, test_files_dir):
        # sample.tar.gz/fileA.txt
        member_file_path = ArchivePath(test_files_dir / SAMPLE_TAR_GZ_SOURCE.name / "fileA.txt")
        expected_content = "File A in tar."

        with member_file_path.open('rb') as f:
            # Tar files might have null padding, be careful with exact binary match if not stripped by reader.
            # .strip() on bytes might remove more than just whitespace.
            # For text files in tar, usually safe to decode then strip.
            binary_content = f.read()
            assert binary_content.decode('utf-8').strip() == expected_content # Decode then strip for comparison

        with member_file_path.open('rt', encoding='utf-8') as f:
            assert f.read().strip() == expected_content


    def test_open_non_existent_regular_file_raises_error(self, test_files_dir):
        p = ArchivePath(test_files_dir / "no_such_file.txt")
        with pytest.raises(FileNotFoundError):
            p.open('rb')

    def test_open_non_existent_file_in_archive_raises_error(self, test_files_dir):
        p = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "no_such_file.txt")
        with pytest.raises(FileNotFoundError): # Or ArchiveMemberNotFoundError, depends on ArchivePath's behavior
            p.open('rb')

    def test_open_directory_regular_raises_error(self, test_files_dir):
        p = ArchivePath(test_files_dir / "regular_dir")
        with pytest.raises(IsADirectoryError): # Or generic OSError on some systems
            p.open('rb')

    def test_open_directory_in_archive_raises_error(self, test_files_dir):
        # sample.zip/dir1
        p = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "dir1")
        with pytest.raises(IsADirectoryError): # Or ArchiveMemberError if it's not a file
             p.open('rb')

    def test_open_file_in_zip_check_std_methods(self, test_files_dir):
        member_file_path = ArchivePath(test_files_dir / SAMPLE_ZIP_SOURCE.name / "file1.txt")
        expected_content = "This is file1."

        with member_file_path.open('rt', encoding='utf-8') as f:
            assert f.readable()
            # seek/tell might be limited depending on how archivey.core.open_member works
            # For now, just check read operations
            line = f.readline()
            assert line.strip() == expected_content.strip() # readline often keeps newline
            f.seek(0) # Should work if it's BytesIO/StringIO or a well-behaved stream
            lines = f.readlines()
            assert len(lines) == 1
            assert lines[0].strip() == expected_content.strip()
            f.seek(0)
            assert f.read().strip() == expected_content.strip()

# Placeholder for future test groups
# Group 5: Path Traversal (/, parent)
# Group 6: Read-Only Operations
# Group 7: Cache Functionality
# Group 8: extractall()
# Group 9: Edge Cases
print("Added iterdir and open tests to test_archive_path.py.")
```

I've created the initial test file `tests/archivey/test_archive_path.py` with:
-   Imports.
-   An `autouse` fixture `clear_cache_before_after_each_test` to ensure the global reader cache is clean for each test.
-   A `test_files_dir` fixture that prepares a temporary directory, copies my sample archives into it, and creates some regular files/directories. This ensures tests run with isolated, fresh data.
-   Test cases for "Basic Path Operations (Regular Files/Dirs)".
-   Test cases for "Archive Handling (Paths to Archives and Members)", covering properties, existence, and type checks for paths pointing to archive files themselves and members within them (top-level, nested, non-existent).
-   A test for `text_archive.txt` to ensure a non-archive file isn't misinterpreted when used in a path that looks like an archive path.
-   A placeholder test idea for `archive_format` hint.

This is a good starting point. The next steps would be to fill in the remaining test categories. Given the turn limit, I'll submit this initial structure and first set of tests.
