## REMOVED Requirements

### Requirement: RAR requires the rarfile package

**Reason**: Metadata is now parsed by a built-in native RAR parser, so `rarfile`
is no longer needed for reading. Decompression already relies on the `unrar`
binary.

**Migration**: Remove `rarfile` from your environment if you don't need it
elsewhere; ensure the `unrar` binary is installed for decompression. Encrypted
headers still need a cryptography backend.

## ADDED Requirements

### Requirement: RAR metadata is parsed natively; decompression requires unrar

The RAR reader SHALL parse archive metadata (member list, headers, solidity,
encryption records) with a built-in native parser that requires no third-party
Python package. Decompression SHALL use the `unrar` binary, and the reader SHALL
raise `PackageNotInstalledError` when decompression is attempted but `unrar` is
not available. Decrypting encrypted headers SHALL require a cryptography backend
(`cryptography` or `pycryptodome`), raising `PackageNotInstalledError` when one is
needed but unavailable.

#### Scenario: Metadata read without rarfile
- **WHEN** a RAR archive is opened and `rarfile` is not installed
- **THEN** the member list and metadata are still read by the native parser

#### Scenario: Decompression without unrar
- **WHEN** a non-stored RAR member is opened for reading and the `unrar` binary is
  not available
- **THEN** `PackageNotInstalledError` is raised

#### Scenario: Encrypted headers without a cryptography backend
- **WHEN** a header-encrypted RAR archive is opened with a password but no
  cryptography backend is installed
- **THEN** `PackageNotInstalledError` is raised

### Requirement: Unsupported RAR variants raise a clean error

The native parser SHALL raise an `ArchiveError` (an `ArchiveUnsupportedFeatureError`
where appropriate) for RAR variants it does not support — multi-volume archives and
very old RAR2 layouts — rather than producing incorrect output.

#### Scenario: Multi-volume archive
- **WHEN** a multi-volume RAR archive is opened
- **THEN** an `ArchiveError` is raised rather than a partial or garbled member list

#### Scenario: RAR2 archive
- **WHEN** a RAR2-era archive (extract version ≤ 20) is opened and is not supported
- **THEN** an `ArchiveUnsupportedFeatureError` is raised

### Requirement: Blake2sp-only RAR5 members report no CRC32

When a RAR5 member stores a Blake2sp hash instead of a CRC32, the reader SHALL set
`crc32 = None` rather than reporting the Blake2sp value as a CRC32.

#### Scenario: Blake2sp member
- **WHEN** a RAR5 member uses a Blake2sp hash and has no CRC32
- **THEN** the member's `crc32` is `None`
