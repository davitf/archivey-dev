# Archivey design principles

This document captures the philosophy behind Archivey's API and behaviour. It is
the north star for design decisions: when a format or library does something
unusual, or a feature could be handled several ways, these principles decide
which behaviour Archivey should present.

These are deliberately high-level. Concrete, testable rules live in the
[OpenSpec live specs](https://github.com/davitf/archivey-dev/tree/main/openspec/specs);
this document explains *why* those rules look the way they do.

## 1. One interface, many formats

A caller should be able to open a ZIP, a `.tar.zst`, a RAR, a 7z, an ISO, a plain
folder, or a single `.gz` and use the **same** [`ArchiveReader`][archivey.ArchiveReader]
methods, member fields, and exceptions. Format-specific quirks are smoothed over
inside the readers, not pushed onto the caller. Where a format genuinely cannot
do something, that surfaces as a documented, consistent limitation (e.g. a
`ValueError` for passwords on formats with no encryption) rather than a
format-specific surprise.

## 2. Least astonishment

Archivey should behave the way a careful person familiar with the underlying OS
and native tools would expect.

- Mirror how the native tool or OS treats an entry. A directory mount point
  (an NTFS junction) is a *link to a directory*, not a directory to recurse
  into — so we don't silently descend into it, just as `os.walk(followlinks=False)`
  and backup tools don't.
- Don't silently do expensive work. Listing members or opening one file should
  not quietly re-decompress the whole archive; when that's unavoidable (solid
  archives, streaming tars), the cost is visible through the streaming-mode API
  and `has_random_access()`.
- Preserve the caller's mental model of the data: normalized but faithful
  filenames, timezone information when the format records it, original metadata
  in `raw_info`.

## 3. Safe by default

Reading and extracting an **untrusted** archive must be safe without the caller
having to think about it.

- The default extraction filter is `DATA`, which blocks absolute paths, path
  traversal (`..`), and unsafe link targets, strips dangerous permission bits,
  and rejects special files.
- Extraction never writes outside the destination directory.
- Behaviour that *could* be dangerous if it were automatic (following links to
  absolute targets, restoring setuid bits, overwriting existing files) requires
  an explicit opt-in (`FULLY_TRUSTED`, `overwrite_mode`, a custom filter).

When safety and convenience conflict, safety wins by default, and the escape
hatch is explicit and discoverable.

## 4. Do the right thing by default, allow power on request

The zero-config path should be the one most people want: automatic format
detection, the most correct available backend, safe extraction. More capable or
specialised behaviour is opt-in through [`ArchiveyConfig`][archivey.ArchiveyConfig]
(alternative backends like `rapidgzip`/`indexed_bzip2`, single-pass streaming,
overwrite modes) — never required for the common case.

## 5. Lose nothing silently

If a format records a piece of metadata, Archivey should surface it rather than
discard it. A field that the format does not provide is `None`/`False`/empty; a
field it *does* provide should reach the caller, even if only through `extra` or
`raw_info`. Silently dropping information the format carried (for example, a
RAR or 7z junction's type and target) is a bug, not a simplification.

When information is *unknown*, represent it as unknown (`None`) rather than
guessing — an honest "I don't know" is less surprising than a confident wrong
answer.

## 6. Compatibility

Where a well-known stdlib API already shapes how Python developers think about
archives, match it so existing code and mental models keep working:

- `ArchiveMember` mirrors `zipfile.ZipInfo` where it can (`date_time`, `CRC`,
  `filename`, `file_size`, `compress_size`, `comment`, `create_system`).
- Extraction filters mirror `tarfile`'s named filters (`fully_trusted`, `tar`,
  `data`).

Compatibility is a convenience, not a straitjacket: when a stdlib behaviour is
itself surprising or unsafe, prefer the safer behaviour and document the
difference.

## 7. Fail loudly and consistently

- Every error caused by an archive problem is an [`ArchiveError`][archivey.ArchiveError]
  subclass, so callers can catch one base type and still discriminate causes.
- Underlying library exceptions are translated, never leaked raw.
- Warn-and-continue is used only where recovery is genuinely safe and the
  alternative would be worse for the caller (e.g. an unreadable link target);
  anything that compromises correctness or safety raises.
