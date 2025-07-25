# Archivey User Guide

Archivey is a Python library that provides a consistent interface for reading and extracting files from many archive formats, including ZIP, TAR, RAR, 7z, and compressed formats like `.gz`, `.bz2`, `.xz`, `.zst`, and `.lz4`.

This guide covers the most common use cases. For full details, see the [API reference](api.md).

---

## 📦 Opening an Archive

Use [`open_archive`][archivey.open_archive] to open any supported archive:

```python
from archivey import open_archive

with open_archive("data.zip") as archive:
    print("Opened archive with", len(archive.get_members()), "entries")
```

You can pass:

- A file path or binary stream
- `config`: an [`ArchiveyConfig`][archivey.ArchiveyConfig] object
- `streaming_only=True`: enables one-pass streaming mode
- `pwd`: password for encrypted archives

---

## 📤 Streaming-Safe Methods

Some archive formats (like `.tar.gz`, `.tar.xz`) don’t include a central index, so listing or accessing members typically requires decompressing the entire archive. Similarly, **solid archives** (like some RAR and 7z files) store multiple files in a single compressed block — so accessing a file mid-archive may require decompressing everything before it. Underlying libraries often perform this extra decompression silently.

**Streaming-safe methods** let you read or extract relevant members in a single pass, avoiding redundant decompression. They also support non-seekable sources (e.g. pipes or network streams), if the underlying format or library allows.

When opening with `streaming_only=True`, non-streaming methods are disabled to prevent accidental re-decompression. Even outside of streaming mode, these methods may still be more efficient.

---

### [`extractall`][archivey.ArchiveReader.extractall]

Extracts all or selected members to a target directory:

```python
archive.extractall(
    path="output/",
    members=lambda m: m.filename.endswith(".txt"),
)
```

Options:

- `members`: list of names and/or [`ArchiveMember`][archivey.ArchiveMember] objects, or a predicate function to select entries to extract
- `filter`: sanitization policy or callable to adjust, reject, or rename members  
  - Predefined [`ExtractionFilter`][archivey.ExtractionFilter] values: `DATA`, `TAR`, or `FULLY_TRUSTED`
  - Custom: `(member, dest_path) -> member or None`  
    Useful for renaming files, skipping dangerous paths, adjusting permissions, etc.
- `pwd`: optional password for encrypted members  
  If omitted, uses the value passed to `open_archive` (if any). You can override it here or use it to handle archives with multiple passwords.

Returns a mapping of extracted paths to their corresponding [`ArchiveMember`][archivey.ArchiveMember].

---

### [`iter_members_with_streams`][archivey.ArchiveReader.iter_members_with_streams]

Iterates over each member, yielding `(ArchiveMember, BinaryIO | None)`:

```python
for member, stream in archive.iter_members_with_streams():
    print(member.filename)
    if stream:
        data = stream.read()
```

- Accepts the same `members`, `filter`, and `pwd` arguments as [`extractall`](#extractall)
- Streams are lazily opened and closed automatically as iteration advances
- `stream` is `None` for non-file entries (e.g. directories or symlinks)

---

### [`get_members_if_available`][archivey.ArchiveReader.get_members_if_available]

Returns the member list if it’s already known or can be retrieved from a central directory (e.g. ZIP or 7z). Returns `None` if the archive would need to be scanned or decompressed.

Useful for progress reporting or early inspection without triggering a full scan.

---

## 🗂️ Random-Access Methods

These methods are available only if the archive was **not** opened in `streaming_only` mode. You can check with:

```python
if archive.has_random_access():
    ...
```

---

### [`get_members`][archivey.ArchiveReader.get_members]

Returns a complete list of archive entries:

```python
members = archive.get_members()
```

Note: For some formats, this may involve scanning or decompressing large portions of the archive.

---

### [`open`][archivey.ArchiveReader.open]

Opens a specific file in the archive:

```python
with archive.open("docs/readme.txt") as f:
    content = f.read()
```

If the member is a symlink or hardlink, the link will be resolved to its target, and the stream will reflect the target’s contents. Raises an error if the member is a directory, or a link pointing outside the archive or to a missing file.

---

### [`extract`][archivey.ArchiveReader.extract]

Extracts a single member to disk:

```python
archive.extract("README.md", path="docs/")
```

Returns the extracted file path.

---

### [`get_member`][archivey.ArchiveReader.get_member]

Looks up a member by name or validates an existing one:

```python
member = archive.get_member("assets/logo.png")
```

---

## 🧪 Filters and Sanitization

Archivey applies sanitization by default to prevent unsafe extraction:
- Strips absolute paths
- Blocks path traversal (`../`)
- Normalizes symlink targets
- Adjusts unsafe permissions

You can override this with the `filter` argument or set it globally using [`extraction_filter`][archivey.ArchiveyConfig.extraction_filter]:

```python
from archivey import ArchiveyConfig, ExtractionFilter

config = ArchiveyConfig(extraction_filter=ExtractionFilter.FULLY_TRUSTED)
```

### Predefined filters:
- `DATA`: safe defaults (default)
- `TAR`: mimics `tar` behavior
- `FULLY_TRUSTED`: disables filtering (use with caution)

You can also use a custom function:
```python
(member: ArchiveMember, dest_path: str | None) → ArchiveMember | None
```

---

## ⚙️ Configuration Options

You can control Archivey’s behavior using an [`ArchiveyConfig`][archivey.ArchiveyConfig] object.

Pass it to [`open_archive`][archivey.open_archive], or set it globally using [`set_archivey_config`][archivey.set_archivey_config] and [`get_archivey_config`][archivey.get_archivey_config].

```python
from archivey import ArchiveyConfig, OverwriteMode

config = ArchiveyConfig(
    use_rapidgzip=True,
    overwrite_mode=OverwriteMode.SKIP,
)
set_archivey_config(config)
```

Common options:
- `use_rar_stream`: improves streaming performance for solid RAR archives by avoiding repeated decompression; uses `unrar` directly instead of `rarfile`
- `use_rapidgzip`, `use_indexed_bzip2`, etc.: enable faster or more flexible backends
- `overwrite_mode`: controls behavior when extracting over existing files
- `extraction_filter`: global sanitization policy for extracted entries

You can also use the [`archivey_config`][archivey.archivey_config] context manager to temporarily override the global config:

```python
from archivey import archivey_config, get_archivey_config

with archivey_config(
    use_rapidgzip=True, extraction_filter="data", overwrite_mode="skip"
):
    print(get_archivey_config())
    with open_archive(...):
        ...
```

---

## 🧵 Reading Compressed Streams

Use [`open_compressed_stream`][archivey.open_compressed_stream] to read `.gz`, `.bz2`, `.xz`, `.zst`, or `.lz4` files:

```python
from archivey import open_compressed_stream

with open_compressed_stream("file.txt.gz") as f:
    print(f.read().decode())
```

---

## 🛑 Error Handling

All archive-related exceptions derive from [`ArchiveError`][archivey.ArchiveError].

Notable subtypes:
- `ArchiveEncryptedError`
- `ArchiveCorruptedError`
- `ArchiveMemberNotFoundError`

Example:

```python
from archivey import open_archive, ArchiveError

try:
    with open_archive("file.7z") as archive:
        ...
except ArchiveError as e:
    print("Archive error:", e)
```

---

## 📘 See Also

- [API Reference](api.md)
- [Developer Guide](developer_guide.md)
