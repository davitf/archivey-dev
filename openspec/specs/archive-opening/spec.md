# Archive Opening Specification

## Purpose

Define the public entry points that turn a path or binary stream into a usable
reader: `open_archive()` (returns an `ArchiveReader`) and
`open_compressed_stream()` (returns an uncompressed binary stream for a single
compressed file). This capability covers input normalization, seekability
requirements, streaming mode selection, format resolution, and configuration
resolution performed before a concrete reader is constructed.

## Requirements

### Requirement: open_archive accepts paths and binary streams

`open_archive()` SHALL accept a filesystem path (`str`, `bytes`, or
`os.PathLike`) or a binary file-like object as its first argument, and return an
`ArchiveReader` instance.

#### Scenario: Open by path string
- **WHEN** `open_archive("example.zip")` is called for an existing ZIP file
- **THEN** an `ArchiveReader` instance is returned

#### Scenario: Open by binary stream
- **WHEN** `open_archive(stream)` is called with an open binary file-like object
- **THEN** the stream is wrapped and an `ArchiveReader` instance is returned

#### Scenario: Invalid argument type
- **WHEN** `open_archive()` is called with an argument that is neither a stream,
  `str`, `bytes`, nor `os.PathLike`
- **THEN** a `TypeError` is raised

#### Scenario: Missing file path
- **WHEN** `open_archive(path)` is called with a path that does not exist
- **THEN** a `FileNotFoundError` is raised

### Requirement: Non-seekable sources require streaming mode

`open_archive()` SHALL raise `ArchiveStreamNotSeekableError` when opening from a
stream that is not seekable while `streaming` is `False` (the default), rather
than attempting random access. It SHALL also raise `ArchiveStreamNotSeekableError`
when `streaming` is `True` but the resolved format cannot operate on a
non-seekable source (e.g. ZIP, which requires the end-of-central-directory record
at the tail of the stream).

#### Scenario: Non-seekable source without streaming
- **WHEN** `open_archive(non_seekable_stream)` is called with `streaming=False`
- **THEN** `ArchiveStreamNotSeekableError` is raised

#### Scenario: Non-seekable source with streaming, format supports sequential reading
- **WHEN** `open_archive(non_seekable_stream, streaming=True)` is called and the
  format supports sequential reading
- **THEN** an `ArchiveReader` is returned in streaming mode

#### Scenario: Non-seekable source with streaming, format cannot stream
- **WHEN** `open_archive(non_seekable_stream, streaming=True)` is called and the
  resolved format cannot operate on a non-seekable source (such as ZIP)
- **THEN** `ArchiveStreamNotSeekableError` is raised

#### Scenario: Seekable source is rewound
- **WHEN** a seekable stream is passed to `open_archive()`
- **THEN** the stream is seeked to position 0 before reading begins

### Requirement: streaming_only is a deprecated alias for streaming

The `streaming_only` parameter SHALL continue to work as an alias for
`streaming`, overriding it when provided, and SHALL emit a `DeprecationWarning`.

#### Scenario: streaming_only emits deprecation warning
- **WHEN** `open_archive(path, streaming_only=True)` is called
- **THEN** a `DeprecationWarning` is emitted and the archive is opened in
  streaming mode

### Requirement: Format is auto-detected unless explicitly provided

When `format` is `None`, `open_archive()` SHALL auto-detect the archive format
from the content (and filename when available). When `format` is provided as an
`ArchiveFormat`, `ContainerFormat`, or `StreamFormat`, that format SHALL be used
and detection SHALL be skipped.

#### Scenario: Auto-detection
- **WHEN** `open_archive(path)` is called without a `format` argument
- **THEN** the format is detected via the format-detection capability

#### Scenario: Explicit container format
- **WHEN** `open_archive(path, format=ContainerFormat.ZIP)` is called
- **THEN** the format is treated as ZIP with an uncompressed stream and no
  detection is performed

#### Scenario: Explicit stream format
- **WHEN** `open_archive(path, format=StreamFormat.GZIP)` is called
- **THEN** the format is treated as a `RAW_STREAM` container with a GZIP stream

### Requirement: Unknown or unsupported formats are rejected

`open_archive()` SHALL raise `ArchiveNotSupportedError` if the resolved format is
`ArchiveFormat.UNKNOWN`, or its container has no registered reader.

#### Scenario: Unknown format
- **WHEN** the format cannot be determined for the input
- **THEN** `ArchiveNotSupportedError` is raised

#### Scenario: Unsupported container
- **WHEN** the detected container format has no registered reader
- **THEN** `ArchiveNotSupportedError` is raised

### Requirement: Container formats are dispatched to the matching reader

`open_archive()` SHALL select the concrete reader from the resolved container
format: ZIP→ZipReader, RAR→RarReader, 7z→SevenZipReader, TAR→TarReader,
ISO→IsoReader, FOLDER→FolderReader, RAW_STREAM→SingleFileReader.

#### Scenario: Single-file compressed archive uses SingleFileReader
- **WHEN** a standalone `.gz` file is opened
- **THEN** a `SingleFileReader` is returned, exposing the file as an archive with
  a single member

### Requirement: Password type is validated

`open_archive()` SHALL accept `pwd` as `str`, `bytes`, or `None`, and SHALL raise
`TypeError` for any other type.

#### Scenario: Invalid password type
- **WHEN** `open_archive(path, pwd=123)` is called with a non-string,
  non-bytes password
- **THEN** a `TypeError` is raised

### Requirement: Configuration is resolved before reader construction

When `config` is `None`, `open_archive()` SHALL use the current default
configuration (see the configuration capability). The resolved configuration
SHALL be active while format detection and reader construction run.

#### Scenario: Default configuration used
- **WHEN** `open_archive(path)` is called without a `config` argument
- **THEN** the current default `ArchiveyConfig` is used for detection and reading

### Requirement: open_compressed_stream returns an uncompressed stream

`open_compressed_stream()` SHALL open a single-file compressed input and return a
binary file-like object yielding the uncompressed bytes. It SHALL only accept
formats whose container is `RAW_STREAM`.

#### Scenario: Open a compressed file
- **WHEN** `open_compressed_stream("file.txt.gz")` is called
- **THEN** a readable binary stream of the decompressed content is returned

#### Scenario: Reading starts at the stream's current position
- **WHEN** a stream positioned past byte 0 is passed to `open_compressed_stream()`
- **THEN** detection may read ahead from the call-time position, but the stream is
  rewound to that same position before decompression begins (the call-time
  position is treated as the start of the compressed data)

#### Scenario: Non-RAW_STREAM format rejected
- **WHEN** the detected (or provided) format is a multi-file container such as ZIP
- **THEN** `ArchiveNotSupportedError` is raised

#### Scenario: Compressed-tar detection disabled
- **WHEN** `open_compressed_stream()` detects the format
- **THEN** the input is never reinterpreted as a TAR archive (the underlying
  decompressed stream is returned as-is)
