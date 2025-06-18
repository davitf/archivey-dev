# Reader-Specific Functionalities

This page details the functionalities provided by `archivey`'s format-specific readers that go beyond the capabilities of their underlying Python libraries.

## TAR Reader (`src/archivey/tar_reader.py`)

The TAR reader in `archivey` extends the standard `tarfile` library by providing:

- **Compressed Format Handling:** Supports various compression formats (e.g., gzip, bzip2, lzma, zstd) not directly handled by `tarfile` for archive reading, by utilizing a custom `open_stream` mechanism.
- **Integrity Checks:** Offers an optional integrity check (`_check_tar_integrity`) to verify the structural integrity of the TAR archive after processing all members. This can help detect certain types of corruption.
- **Specific Exception Translation:** Translates generic `tarfile.ReadError` exceptions into more specific `archivey.exceptions` like `ArchiveEOFError` or `ArchiveCorruptedError` for better error handling.

## ZIP Reader (`src/archivey/zip_reader.py`)

The ZIP reader enhances the standard `zipfile` library with:

- **Symlink Support:** Explicitly reads and makes available the target paths for symbolic links (`_read_link_target`), which are stored as file content in ZIP archives.
- **Extended Timestamp Parsing:** Implements `get_zipinfo_timestamp` to parse extended timestamp information (e.g., from Unix-style `UT` extra fields) to provide more accurate modification times than the basic DOS date/time in `ZipInfo`.
- **Fallback Encoding Detection:** Attempts to decode filenames and comments using a list of common encodings (`_ZIP_ENCODINGS`) if the standard UTF-8 decoding fails, improving compatibility with archives created with legacy tools.

## RAR Reader (`src/archivey/rar_reader.py`)

The RAR reader significantly extends the `rarfile` library (and direct `unrar` CLI usage) by offering:

- **Advanced Password Checking (RAR5):** Implements `verify_rar5_password` to check the validity of a password for RAR5 encrypted files using the embedded password check value, often without needing to decompress the file.
- **Streaming for Solid Archives:** Provides `RarStreamReader`, which leverages the `unrar` command-line tool to enable streaming access to the content of members within solid RAR archives. This is beneficial as `rarfile` typically requires full extraction for solid archives.
- **RAR4 Filename Corruption Detection:** Includes `get_non_corrupted_filename` to address a specific issue in older RAR versions (2.9-4.x) where UTF-16 filenames for characters outside the Basic Multilingual Plane could be corrupted. This function attempts to recover the correct filename using the 8-bit `orig_filename`.
- **Encrypted CRC Handling (RAR5):** Implements `check_rarinfo_crc` and `convert_crc_to_encrypted` to correctly validate CRC checksums for files within RAR5 archives when "Encrypt file names" is off. In such cases, RAR modifies checksums using a password-dependent algorithm.
- **Hardlink Detection:** Identifies hardlinks within RAR5 archives.

## 7-Zip Reader (`src/archivey/sevenzip_reader.py`)

The 7-Zip reader builds upon the `py7zr` library to provide:

- **Iterator for Streams:** Implements `StreamingFile` and `StreamingFactory` to support an iterator-based approach (`iter_members_with_io`) for reading member data as streams. This allows for efficient processing of files, especially in large archives, without requiring full extraction to disk or memory first.
- **Temporary Password Context:** Uses a `_temporary_password` context manager to more robustly apply passwords to encrypted folders within the 7-Zip archive structure during read operations.
- **Link Target Resolution:** Automatically resolves and populates symlink targets during member iteration if they are not immediately available from the archive header.
- **Duplicate Filename Handling:** Correctly handles `py7zr`'s internal logic for renaming duplicate filenames (e.g., `file.txt`, `file.txt_1`) when using custom writer factories, ensuring consistent member identification.
