# Developing new `ArchiveReader` implementations

To support a new archive format create a class that derives from
`BaseArchiveReader` in `src/archivey/base_reader.py`.  The base class provides
member bookkeeping, extraction helpers and the default implementations of most
methods.  A minimal reader typically needs to implement:

1. `__init__(self, archive_path, *, pwd=None, streaming_only=False)` – open the
   underlying file and call `BaseArchiveReader.__init__` with information about
   whether random access and member listing are supported.
2. `close()` – close any open file handles.
3. `iter_members_for_registration()` – yield `ArchiveMember` objects describing
   the members in the archive.  The base class handles registration and exposes
   them via `iter_members()` and `get_members()`.
4. `open(member_or_filename, *, pwd=None)` – return a file-like object for a
   member's contents.
5. `get_archive_info()` – return an `ArchiveInfo` instance describing format
   details.

Optional hooks such as `open_for_iteration()` or `get_members_if_available()` can
be overridden for specialised behaviour.

## Exception handling

All exceptions raised to library users must derive from `ArchiveError`.  When
wrapping another library, use `ExceptionTranslatingIO` from
`archivey.io_helpers` for any streams it returns.  Provide a translator function
that maps the library's exceptions to `ArchiveError` subclasses.  Avoid catching
`Exception`; instead list the specific exceptions the dependency can raise and
cover them with tests so unexpected errors are surfaced during development.
