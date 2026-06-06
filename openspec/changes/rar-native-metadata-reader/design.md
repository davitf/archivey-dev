# Design

The full design lives in **`docs/rar-native-reader-design.md`** (block formats,
extract-hack byte layouts, AES key derivation, the `RarMemberInfo` dataclass, parser
class shapes, and the risk list). This file records only the decisions that affect
the live specs and how this change fits the existing capability.

## Scope decision: metadata-only

Replace `rarfile` for parsing; keep `unrar` for decompression (design §4.1–4.2).
This is the smallest change that removes the `rarfile` dependency, and it preserves
the entire decompression path — including `RarStreamReader` (`use_rar_stream`) and
the stored-member fast path — unchanged.

## Spec cross-check (docs vs live `rar-format` spec)

Because the live spec is behavior-focused, almost every requirement is **unchanged**
by a native parser:

| Live requirement | Effect |
|---|---|
| RAR requires a seekable source | unchanged |
| reports version, solidity, header encryption | unchanged (parser computes these directly) |
| member metadata mapped with format workarounds | unchanged (same fields, same Unicode-name fix) |
| encrypted RAR5 CRCs not reported as plain CRCs | unchanged (uses the same encryption tuple) |
| link targets resolved, using unrar when needed | unchanged |
| encrypted members verify password on open | unchanged |
| `use_rar_stream` single-pass solid extraction | unchanged |
| RAR errors are translated | unchanged (native parser raises `ArchiveError` subclasses directly) |
| **RAR requires the `rarfile` package** | **replaced** — see delta |

So the delta is narrow: flip the dependency requirement, and add the edge-case
behaviors the native parser now owns (multi-volume, RAR2, Blake2sp).

## Decisions

- **Multi-volume / RAR2**: out of scope for support, but the parser raises a clean
  `ArchiveError` / `ArchiveUnsupportedFeatureError` rather than producing garbage
  (design §7.1, §7.3).
- **Blake2sp-only RAR5 members**: `crc32 = None` (CRC32 absent); streaming CRC checks
  become no-ops unless Blake2sp verification is added later (design §7.4).
- **Encrypted headers** still require a cryptography backend and a seekable source
  (design §4.3, §7.5); behavior matches today.
- **`raw_info`** becomes `RarMemberInfo` instead of `rarfile`'s `Rar*Info`; the only
  consumers are the CRC/encryption helpers and `RarStreamMemberFile`, which need just
  `filename`, `file_size`, `CRC`, `needs_password()`, and the encryption tuple
  (design §4.4).
