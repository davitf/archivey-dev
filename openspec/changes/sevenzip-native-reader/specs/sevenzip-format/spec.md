## REMOVED Requirements

### Requirement: 7z requires the py7zr package

**Reason**: 7z support is now fully native — metadata is parsed by a built-in header
parser and decompression is driven by archivey directly using standard codec packages
(stdlib `lzma`/`bz2`/`zlib`, plus the existing `zstandard`/`brotli`/crypto optionals,
plus `pyppmd` and `inflate64` for PPMd and Deflate64). py7zr is no longer a dependency.

**Migration**: remove `py7zr` from your environment if it was installed only for
archivey. Reading or extracting common 7z archives (LZMA2, optionally with BCJ/Delta
filters, Deflate, BZip2, Copy, AES) needs no extra package. PPMd and Deflate64 require
the `pyppmd` and `inflate64` packages respectively; Zstd/Brotli/AES require their
existing optional packages. The codec-specific behaviour is restated below.

## ADDED Requirements

### Requirement: 7z is read and decompressed natively without py7zr

The 7z reader SHALL parse archive metadata (member list, folders, solidity,
encryption, comment) and decompress member content with a built-in implementation that
does not require the py7zr package. Decompression SHALL be driven directly by the
reader (pull-based), without a background extraction thread.

The reader SHALL support, without py7zr, the codecs that map onto facilities archivey
already has: Copy, LZMA1, LZMA2, Delta, the BCJ branch filters (x86/ARM/ARMT/PPC/
SPARC/IA64), Deflate, and BZip2 using the standard library; and Zstd, Brotli, and
AES-256 using their existing optional packages.

#### Scenario: Metadata read without py7zr
- **WHEN** a 7z archive is opened and py7zr is not installed
- **THEN** the member list and metadata are read by the native parser

#### Scenario: Common archive extracted without py7zr
- **WHEN** a 7z member compressed with LZMA2 (optionally with a BCJ and/or Delta
  filter), Deflate, BZip2, or stored uncompressed is read and py7zr is not installed
- **THEN** its content is decompressed and returned, with the per-member CRC verified

### Requirement: Codecs needing extra packages report a clear dependency error

PPMd and Deflate64 SHALL be decompressed via the `pyppmd` and `inflate64` packages
respectively; Zstd, Brotli, and AES-256 via their existing optional packages. When a
member requires a codec whose backing package is not installed, the reader SHALL raise
`PackageNotInstalledError` naming the missing package, rather than failing obscurely.
The `pyppmd` and `inflate64` decompressors SHALL be exposed as shared stream
decompressors (usable by other format readers, e.g. a future ZIP reader for Deflate64),
not 7z-local.

#### Scenario: PPMd member without pyppmd
- **WHEN** a PPMd-compressed 7z member is read and `pyppmd` is not installed
- **THEN** `PackageNotInstalledError` naming `pyppmd` is raised

#### Scenario: Deflate64 member without inflate64
- **WHEN** a Deflate64-compressed 7z member is read and `inflate64` is not installed
- **THEN** `PackageNotInstalledError` naming `inflate64` is raised

### Requirement: Unsupported 7z coders raise a clean error

The reader SHALL raise a clear `ArchiveError` (an unsupported-compression-method
error) for coders it cannot decode — including the BCJ2 four-stream coder and any
newer branch filter (e.g. ARM64/RISC-V) not available in the installed liblzma —
rather than producing incorrect output. This is at least as capable as the previous
py7zr-based reader, which also cannot decode BCJ2.

#### Scenario: BCJ2 archive
- **WHEN** a 7z member is stored in a folder using the BCJ2 coder
- **THEN** a clear unsupported-compression-method `ArchiveError` is raised (no garbage
  output)

### Requirement: 7z exposes per-member compression method

The reader SHALL populate each member's `compression_method` with the typed primary
codec (for example `LZMA2`, `LZMA`, `PPMD`) and SHALL preserve the full coder chain
(for example `"LZMA2 + BCJ"`) in `compression_method_detail`.

#### Scenario: LZMA2 member
- **WHEN** a 7z member is stored in an LZMA2-coded folder
- **THEN** its `compression_method` reflects LZMA2 (rather than `None`)

#### Scenario: Filtered LZMA2 member
- **WHEN** a member's folder prepends a BCJ or Delta filter before LZMA2
- **THEN** `compression_method` is the primary codec and `compression_method_detail`
  reflects the full filter chain

### Requirement: 7z exposes the archive comment

The reader SHALL surface the archive comment (from `FILES_INFO`) in
`ArchiveInfo.comment` when present, rather than discarding it.

#### Scenario: Archive with a comment
- **WHEN** a 7z archive stores a comment
- **THEN** `get_archive_info().comment` returns it

### Requirement: Unsupported 7z variants raise a clean error

The native parser SHALL raise an `ArchiveError` for multi-volume 7z archives rather
than producing incorrect output, and SHALL handle anti-items without corrupting the
member list.

#### Scenario: Multi-volume 7z
- **WHEN** a multi-volume 7z archive is opened
- **THEN** an `ArchiveError` is raised
