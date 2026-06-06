# Compressed Stream Backends Specification

## Purpose

Define how Archivey opens a single compression stream format and yields a
decompressed binary stream: which backend library serves each `StreamFormat`,
how optional/alternative backends are selected by configuration, how missing
optional packages are surfaced, and how every returned stream is wrapped for
uniform exception translation.

## Requirements

### Requirement: Each stream format has a default backend

`open_stream()` SHALL decompress the supported stream formats using these
defaults: gzip via stdlib `gzip`, bzip2 via stdlib `bz2`, xz via the native
`XzDecompressorStream` (stdlib `lzma`), lzip via the native
`LzipDecompressorStream`, zlib and brotli via native decompressor streams, lz4
via `lz4.frame`, zstd via `pyzstd`, and Unix compress via `uncompresspy`.

#### Scenario: Default gzip backend
- **WHEN** a gzip stream is opened with default configuration
- **THEN** it is decompressed using the stdlib gzip module

#### Scenario: Default xz backend
- **WHEN** an xz stream is opened with default configuration
- **THEN** it is decompressed using the native `XzDecompressorStream`

### Requirement: Configuration selects alternative backends

`open_stream()` SHALL select alternative backends when configured:
`use_rapidgzip` for gzip, `use_indexed_bzip2` for bzip2, `use_zstandard` to use
the `zstandard` library instead of `pyzstd` for zstd, and `use_python_xz` to use
the `python-xz` library instead of the native xz stream.

#### Scenario: rapidgzip selected
- **WHEN** a gzip stream is opened with `use_rapidgzip=True`
- **THEN** the rapidgzip backend is used

#### Scenario: python-xz selected
- **WHEN** an xz stream is opened with `use_python_xz=True`
- **THEN** the python-xz backend is used instead of `XzDecompressorStream`

### Requirement: Missing optional backends raise PackageNotInstalledError

Opening a stream SHALL raise `PackageNotInstalledError` when the stream format's
selected backend requires an optional package that is not installed.

#### Scenario: Missing lz4 package
- **WHEN** an lz4 stream is opened but the `lz4` package is not installed
- **THEN** `PackageNotInstalledError` is raised

#### Scenario: Missing requested alternative backend
- **WHEN** `use_python_xz=True` but `python-xz` is not installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: Returned streams translate decompression errors

`open_stream()` SHALL wrap each backend stream so that decompression errors are
translated into `ArchiveError` subclasses: corrupt data to
`ArchiveCorruptedError`, unexpected end-of-input to `ArchiveEOFError`, and
backends that require seeking on a non-seekable source to
`ArchiveStreamNotSeekableError`.

#### Scenario: Corrupt compressed data
- **WHEN** reading a corrupted compressed stream
- **THEN** `ArchiveCorruptedError` is raised

#### Scenario: Truncated compressed data
- **WHEN** a compressed stream ends mid-data
- **THEN** `ArchiveEOFError` is raised

#### Scenario: Non-seekable source for a seek-requiring backend
- **WHEN** a backend that requires seeking is given a non-seekable source
- **THEN** `ArchiveStreamNotSeekableError` is raised

### Requirement: Backend dispatch is separable from opening

The backend open function and its exception translator SHALL be resolvable for a
given `StreamFormat` and configuration independently of opening a stream, so that
callers (e.g. format detection, the tar reader) can reuse the correct backend.

#### Scenario: Resolve backend for a format
- **WHEN** the open function for a stream format and configuration is requested
- **THEN** the function and its matching exception translator are returned
