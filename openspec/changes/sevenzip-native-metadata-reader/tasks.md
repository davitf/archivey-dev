# Implementation Tasks: 7z native metadata reader

## 1. Native parser module

- [ ] 1.1 Create `src/archivey/formats/sevenzip_parser.py` with `SevenZipMemberInfo`
      (design §4.2)
- [ ] 1.2 Parse signature header + end header; verify CRC32
- [ ] 1.3 Handle `ENCODED_HEADER` (locate, decompress, verify) and header decryption
      when a password is supplied (design §4.6)
- [ ] 1.4 Parse `FILES_INFO` + `MAIN_STREAMS_INFO` (folders, coders, substreams);
      associate files to folders; compute per-file size/CRC
- [ ] 1.5 Detect `is_solid` (`num_unpackstreams_folders`) and per-folder encryption
      (coder-list check) directly
- [ ] 1.6 Derive `compression_method` names from the coder chain; read archive
      `comment`; populate `atime`/`ctime`

## 2. Wire the reader to the native parser

- [ ] 2.1 Replace py7zr metadata usage in `iter_members_for_registration` with the
      native parser; build `SevenZipMemberInfo` objects
- [ ] 2.2 Keep the thread+queue extractor, `_temporary_password`, and duplicate-name
      mapping (still py7zr for decompression)
- [ ] 2.3 Replace `_is_member_encrypted` and the `archiveinfo()` empty-archive guard
      with native equivalents; inline `filetime_to_dt`
- [ ] 2.4 Raise a clean `ArchiveError` for multi-volume; warn on anti-files

## 3. Dependencies

- [ ] 3.1 `internal/dependency_checker.py`: py7zr documented as decompression-only

## 4. Tests & validation

- [ ] 4.1 All existing 7z test archives still pass (metadata + extraction + CRC)
- [ ] 4.2 New parser unit tests (plain/encoded/encrypted headers, solid folders)
- [ ] 4.3 `compression_method` populated; archive `comment` surfaced
- [ ] 4.4 `openspec validate sevenzip-native-metadata-reader --type change --strict`
- [ ] 4.5 `hatch run lint` and `hatch run test`
