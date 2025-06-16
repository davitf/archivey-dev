# Archivey API

Archivey's main entrypoint is the `open_archive` function. It inspects a file path and
returns an [`ArchiveReader`](../src/archivey/base_reader.py) instance capable of
iterating over members and extracting files.

```python
from archivey import open_archive

with open_archive("example.zip") as archive:
    for member, stream in archive.iter_members_with_io():
        print(member.filename, member.file_size)
        if stream:
            data = stream.read()
```

The `ArchiveReader` objects implement the following public methods:

- `close()` – close the archive and release resources.
- `get_members_if_available()` – return the list of members if available or `None` for
  streaming-only formats.
- `get_members()` – return the list of members, raising an error if the format does
  not support listing.
- `iter_members_with_io()` – iterate over all members yielding `(ArchiveMember, BinaryIO)`
  tuples.
- `get_archive_info()` – return an `ArchiveInfo` object describing the archive
  format.
- `has_random_access()` – return `True` if random access is available.
- `get_member()` – resolve a filename to an `ArchiveMember` object.
- `open()` – open a member for reading.
- `extract()` – extract a member to disk.
- `extractall()` – extract multiple members at once.

See the docstrings in the code for detailed semantics of each method.
