# Implementation Tasks: 7z native reader (metadata + decompression)

## 1. Native header parser

- [ ] 1.1 Create `src/archivey/formats/sevenzip_parser.py` with `SevenZipMemberInfo`
      (design §4.2)
- [ ] 1.2 Parse signature header + end header; verify CRC32
- [ ] 1.3 Handle `ENCODED_HEADER` (locate, decompress via the native folder pipeline,
      verify) and header decryption when a password is supplied (design §4.6)
- [ ] 1.4 Parse `FILES_INFO`, `PACK_INFO` (pack-stream offsets/sizes), `UNPACK_INFO`
      (folders, coders, bind pairs), and `SUBSTREAMS_INFO`; associate files to folders;
      compute per-file size/CRC and `(folder_index, file_in_folder)`
- [ ] 1.5 Detect `is_solid` (`num_unpackstreams_folders`) and per-folder encryption
      (coder-list check) directly
- [ ] 1.6 Derive `compression_method` (typed primary codec) + `compression_method_detail`
      (full chain) from the coder chain; read archive `comment`; populate `atime`/`ctime`

## 2. Native decompression

- [ ] 2.1 Coder-chain → pipeline builder: linear chains map onto a single `lzma`
      FORMAT_RAW filter list for LZMA1/LZMA2 + BCJ (x86/ARM/ARMT/PPC/SPARC/IA64) +
      Delta; non-lzma stages (Deflate via `zlib(-15)`, BZip2, Zstd, Brotli) chain as
      separate `decompress()` steps; Copy is passthrough
- [ ] 2.2 Add shared `pyppmd` (PPMd var.H / `Ppmd7Decoder`) and `inflate64` (Deflate64)
      stream openers in `compressed_streams.py`, alongside the existing openers, so
      they are reusable by other readers (e.g. native ZIP, method 9)
- [ ] 2.3 AES-256 stage: key derivation (§3.5) + CBC decrypt, built per folder with the
      supplied password (no global lock, no folder mutation)
- [ ] 2.4 Expose each member as a pull-based `BinaryIO` via `DecompressorStream` over
      the folder's packed byte range; for solid folders, decompress once and slice
      substreams by unpack size (O(N); design §4.3–§4.4)
- [ ] 2.5 Detect BCJ2 (`numinstreams == 4`) and unsupported/newer BCJ filters before
      building a pipeline; raise a clear unsupported-compression-method `ArchiveError`
      (parity with py7zr). Decide the LZMA1+BCJ handling (separate single-filter lzma
      step, or pull in `pybcj` only for that path) and cover it with a test archive
- [ ] 2.6 Raise `PackageNotInstalledError` naming the missing package when a required
      codec backend (pyppmd/inflate64/zstandard/brotli/crypto) is absent

## 3. Wire the reader; delete the py7zr scaffolding

- [ ] 3.1 Build the member list in `iter_members_for_registration` from the native
      parser; `raw_info` becomes `SevenZipMemberInfo`
- [ ] 3.2 Replace extraction with native decompression; **delete** the thread+queue
      extractor, `WriterFactory`/`StreamingFile`/`NullIO`, `_temporary_password` +
      class `_password_lock`, `reset()`, and the duplicate-name round-trip map (§5)
- [ ] 3.3 Replace `_is_member_encrypted` and the `archiveinfo()` empty-archive guard
      with native equivalents; inline `filetime_to_dt`
- [ ] 3.4 Raise a clean `ArchiveError` for multi-volume; warn on anti-files
- [ ] 3.5 Adopt the base co-iteration hook: override `_iter_members_and_streams_internal`
      instead of the public `iter_members_with_streams`, dropping the bespoke
      member-selection/filter duplication (§8.A from `base-reader-architecture-extensions`).
      Verify filtered-out members still incur no decompression

## 4. Dependencies

- [ ] 4.1 `pyproject.toml`: remove `py7zr>=1.0.0`; add `pyppmd` and `inflate64` to the
      `optional` and `optional-freethreaded` extras
- [ ] 4.2 `internal/dependency_checker.py`: drop py7zr; gate PPMd/Deflate64/Zstd/
      Brotli/AES on their own packages with clear messages

## 5. Tests & validation

- [ ] 5.1 All existing 7z test archives still pass (metadata + extraction + CRC) with
      py7zr uninstalled
- [ ] 5.2 Per-codec decompression tests (LZMA2, LZMA2+BCJ, LZMA2+Delta, LZMA1+BCJ,
      Deflate, BZip2, Zstd, Brotli, PPMd, Deflate64, AES-256, Copy); solid folders
- [ ] 5.3 BCJ2 (and an ARM64/RISC-V BCJ if a sample exists) raise a clean
      unsupported-method error; missing-package paths raise `PackageNotInstalledError`
- [ ] 5.3a Per-member passwords: un-skip `encryption_several_passwords__7zcmd.7z` —
      opening members with their matching per-call `pwd` each decrypt correctly; a
      wrong password surfaces as an encrypted/corrupted error
- [ ] 5.4 `compression_method` + `compression_method_detail` populated; archive
      `comment` surfaced; `atime`/`ctime` when present
- [ ] 5.5 `openspec validate sevenzip-native-reader --type change --strict`
- [ ] 5.6 `hatch run lint` and `hatch run test`
