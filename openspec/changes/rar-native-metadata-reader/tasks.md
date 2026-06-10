# Implementation Tasks: RAR native metadata reader

## 1. Native parser module

- [ ] 1.1 Create `src/archivey/formats/rar_parser.py` with the `RarMemberInfo`
      dataclass (design §4.4) including the extract-hack offset fields
- [ ] 1.2 Implement `NativeRar5Parser` (vint parsing, block walk, extra-field
      decode, solid/password/header-encryption detection)
- [ ] 1.3 Implement `NativeRar3Parser` (block headers, flags, Unicode filename
      decompression, solid/password detection)
- [ ] 1.4 Implement SFX-prefix skipping (scan up to 2 MB for `RAR_ID`/`RAR5_ID`)
- [ ] 1.5 Implement header decryption (RAR3 SHA-1 key derivation; RAR5
      PBKDF2-HMAC-SHA256), reusing the existing cryptography backend. **Implement
      from the design doc's Corrections section, not §4.3 as originally written**:
      RAR3 is AES-128 with 262,144 rounds of WinRAR's buggy SHA-1 (port rarfile's
      `Rar3Sha1`; plain `hashlib.sha1` fails for long passwords); RAR5 is plain
      PBKDF2 with `1 << kdf_count` iterations (no `+32`) and a plaintext IV stored
      before each encrypted header block. Test with a ≥28-char password
- [ ] 1.6 Handle service blocks during the block walk — at minimum skip RAR5
      service headers (comment `CMT`, quick-open `QO`, recovery `RR`) without
      misparsing. Record whether a quick-open (`QO`) block is present: the
      `base-reader-architecture-extensions` cost surface defines RAR
      `member_listing_cost` as `INDEXED` iff the quick-open index exists (actually
      *reading* member headers from the QO block instead of scanning is optional
      and can be a follow-up)

## 2. Wire the reader to the native parser

- [ ] 2.1 Replace `rarfile.RarFile`/`Rar*Info` usage in
      `iter_members_for_registration` with the native parser
- [ ] 2.2 Keep `RarStreamReader`, `RarStreamMemberFile`, CRC/encryption helpers; make
      them consume `RarMemberInfo`
- [ ] 2.3 Re-implement the extract-hack (RAR3/RAR5 minimal temp archive) natively
- [ ] 2.4 Raise clean `ArchiveError`/`ArchiveUnsupportedFeatureError` for
      multi-volume and RAR2 archives
- [ ] 2.5 While in this reader, adopt the base co-iteration hook for the
      `use_rar_stream` solid path: override `_iter_members_and_streams_internal`
      instead of the public `iter_members_with_streams`, dropping the bespoke
      filter/iteration duplication (this is §8.A from
      `base-reader-architecture-extensions`, folded here since this change already
      rewrites the reader). Verify filtered-out members still incur no decompression.

## 3. Dependencies

- [ ] 3.1 `internal/dependency_checker.py`: rarfile no longer required; surface the
      `unrar` binary as the decompression requirement
- [ ] 3.2 `pyproject.toml`: remove `rarfile` from the `optional` extra

## 4. Tests & validation

- [ ] 4.1 All existing RAR test archives still pass (metadata + extraction + CRC)
- [ ] 4.2 New parser unit tests (RAR3 and RAR5 headers, encrypted headers, Unicode
      names, solid detection)
- [ ] 4.3 Blake2sp-only member reports `crc32 = None`
- [ ] 4.4 Multi-volume / RAR2 raise the expected errors
- [ ] 4.5 `openspec validate rar-native-metadata-reader --type change --strict`
- [ ] 4.6 `hatch run lint` and `hatch run test`
