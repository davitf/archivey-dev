# API Reference

## Core

::: archivey
    options:
      members:
      - open_archive
      - open_compressed_stream
      - ArchiveReader
      - ArchiveInfo
      - ArchiveMember

## Enums

::: archivey
    options:
      members:
      - ArchiveFormat
      - MemberType
      - CreateSystem

## Configuration

::: archivey
    options:
      members:
      - ArchiveyConfig
      - default_config
      - get_default_config
      - set_default_config

## Exceptions

::: archivey
    options:
      members:
      - ArchiveError
      - ArchiveFormatError
      - ArchiveCorruptedError
      - ArchiveEncryptedError
      - ArchiveEOFError
      - ArchiveMemberNotFoundError
      - ArchiveNotSupportedError

## Filters

::: archivey
    options:
      members:
      - create_filter
      - data_filter
      - tar_filter
      - fully_trusted
