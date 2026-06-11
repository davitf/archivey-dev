# Archivey — High-Level Requirements Specification

This document describes *what* Archivey is and *what behaviour it must provide*,
independent of how it is built. It is meant for someone who wants to understand
the library's purpose, evaluate its design, or re-implement it from first
principles. It deliberately avoids prescribing internal structure (modules,
classes, field names); those are left to the implementer. Where the underlying
formats or third-party libraries have quirks that *force* certain behaviour, the
quirk is described and the requirement it implies is stated — but the mechanism
is not.

The companion [design principles](../design_principles.md) document explains the
*why* behind the rules here; this document is the *what*.

---

## 1. Purpose and scope

Archivey is a library for **reading** archives of many different formats through
one consistent interface. A caller should be able to open a ZIP, a compressed
TAR, a RAR, a 7z, an ISO image, a plain directory on disk, or a single
compressed file, and then list, read, and extract its contents using the same
operations, the same metadata model, and the same error types — without having
to know which format they were handed.

In scope:

- Reading and extracting archives and compressed files.
- A uniform metadata model across formats.
- Safe-by-default extraction of untrusted archives.
- Simple creation (writing) of archives in the formats where writing is
  straightforward, through a model compatible enough with reading that
  converting one archive into another falls out naturally (see §4.9).
- Optional, opt-in performance backends and behaviours.

Out of scope (current):

- **In-place modification** of an existing archive (adding to, or rewriting a
  member inside, a file that already exists). Producing a *new* archive from
  existing contents is supported; editing one in place is not.

### Supported inputs

- **Multi-file container formats:** ZIP, TAR (plain and compressed), RAR, 7z,
  ISO 9660 optical images.
- **Plain directories** on the local filesystem, presented through the same
  interface as an archive.
- **Single-file compressed streams:** gzip, bzip2, xz, zstandard, lz4, lzip,
  zlib, brotli, and Unix `compress`. The requirement is that each is presented
  through the archive interface as an archive containing exactly one member;
  opening one directly as a decompressed binary stream is an additional,
  optional entry point.
- A compressed TAR (e.g. a gzip- or zstd-wrapped TAR) is recognised as a TAR
  container, not as a single compressed file, when its contents warrant it.

Inputs may be supplied as a filesystem path or as an already-open binary stream,
**including non-seekable streams** (pipes, network reads) wherever the format
can be read without random access.

---

## 2. Guiding principles

These shape every behavioural decision and are the tie-breakers when a format
does something unusual.

1. **One interface, many formats.** The same operations, member fields, and
   exceptions work across every supported format. Format-specific quirks are
   absorbed inside the library, never pushed onto the caller. Where a format
   genuinely cannot do something, that surfaces as a documented, consistent
   limitation rather than a format-specific surprise.

2. **Least astonishment.** Behave the way a careful person familiar with the OS
   and the native tools would expect. Mirror how the native tool treats an entry
   (a directory mount point is a *link*, not something to recurse into). Never
   silently do expensive work; when expense is unavoidable, make it visible.

3. **Safe by default.** Reading and extracting an *untrusted* archive must be
   safe without the caller thinking about it. Anything that could be dangerous if
   automatic (escaping the destination directory, following links to absolute
   targets, restoring privileged permission bits, overwriting existing files)
   requires an explicit, discoverable opt-in. When safety and convenience
   conflict, safety wins by default.

4. **Right thing by default, power on request.** The zero-config path —
   automatic format detection, the most correct available backend, safe
   extraction — is the one most people want. More capable or specialised
   behaviour is always opt-in, never required for the common case.

5. **Lose nothing silently.** If a format records a piece of metadata, surface
   it rather than discard it. A field a format does not provide is reported as
   *unknown*, not guessed. An honest "I don't know" is less surprising than a
   confident wrong answer.

6. **Compatibility where it helps.** Where a well-known standard-library API
   already shapes how people think about archives, match it so existing mental
   models and code keep working — but prefer the safer behaviour, and document
   the difference, when the familiar behaviour is itself unsafe.

7. **Fail loudly and consistently.** Every error caused by an archive problem is
   one common error type (with discriminable subtypes). Underlying library
   exceptions are always translated, never leaked raw. Warn-and-continue is used
   only where recovery is genuinely safe.

8. **Stream, don't buffer.** Reading or writing is incremental and bounded in
   memory. The library must not silently decompress a whole file into memory or a
   temporary file before handing back data, nor read an entire input before it
   starts producing output. Operations must remain viable on inputs far larger
   than available memory, and a caller processing one member at a time should pay
   only for what they touch. Where a format genuinely cannot avoid a whole-archive
   pass (a solid archive, a no-index compressed TAR), that cost is made visible
   (principle 2) rather than hidden behind buffering.

---

## 3. The unified data model

The library presents two stable abstractions regardless of source format.

### 3.1 The archive

An opened archive exposes archive-level metadata: its detected format; an
optional format version (e.g. RAR generation, ISO interchange level); whether it
is **solid** (reading one member may require decompressing earlier ones); an
optional archive comment; and a place for format-specific extras that have no
first-class field.

### 3.2 The member

Every entry is a **member** with a normalised, predictable shape:

- A **type**: regular file, directory, symlink, hardlink, or "other" (special
  files such as devices). A junction (a Windows directory mount point) is
  represented as a symlink that is additionally flagged as a junction.
- A **normalised name**: forward-slash separators, directories ending in a
  trailing slash, links carrying their own path (not a trailing slash). The
  original, unmodified name is preserved separately when the format stores one.
- A **stable identity** within the archive (an order-preserving sequence
  position and an archive identifier), so that two members sharing a filename
  remain distinguishable and original order is never lost.
- **Optional metadata**, each present only when the format records it and
  otherwise reported as unknown: uncompressed and compressed sizes; modification,
  access, and creation times; Unix mode, owner/group ids and names; an integrity
  checksum; the compression method; a comment; the originating system; Windows
  attributes; an encryption flag; and, for links, the link target and (when
  known) the type of the target.
- A reference to the underlying library's own object, as an escape hatch for
  callers that need something the unified model does not expose.

**Time handling.** Modification time is exposed in a form that carries timezone
information when the format records times against a global clock, and is naive
when the format records local wall-clock time. A simpler naive form is also
available for compatibility. The library must not invent a timezone the format
did not record.

**Compatibility surface.** The member also offers accessors shaped like the
most familiar standard-library archive type, so existing code keeps working
where it reasonably can.

**Compression method.** The compression method a member uses is exposed in a way
callers can act on programmatically (not only a free-form string they must
parse), without losing detail when the real encoding is richer than a single
codec — for example a 7z filter chain. Exactly how that is represented is left to
the implementer.

---

## 4. Usage patterns the library must support

These are the interaction styles the design is obligated to serve well. The
naming below is conceptual.

### 4.1 Open and inspect

Open by path or stream, with the format auto-detected by default or stated
explicitly. From an open archive the caller can read archive-level metadata and
obtain the member list.

### 4.2 Random access

On a seekable source the caller can list all members, look up a member by name,
open an arbitrary member's content as a binary stream, and extract one member or
many to disk in any order. Opening a link transparently resolves to and reads
its target.

### 4.3 Sequential (streaming) iteration

The caller can iterate members in archive order, receiving each member together
with a lazily-opened content stream (or no stream, for entries that have no
content). This is the efficient path for "do something to every file": it makes a
single forward pass and never silently re-decompresses, and it works on
non-seekable sources (pipes, network streams) for formats that allow it.

When the caller commits to this mode, operations that would require random
access are disabled, so expensive re-reads cannot happen by accident; iteration
in this mode is single-use.

### 4.4 Extraction

The caller can extract all or a selected subset of members to a destination
directory. Extraction:

- applies a **safety filter** (see §5) — by default the strict one;
- honours an **overwrite policy** (error, skip, or replace) when a target
  already exists;
- recreates directories, files, symlinks, and hardlinks, restoring available
  metadata (times, permissions, the Windows read-only attribute) on a best
  effort basis;
- never writes outside the destination directory;
- reports which members were written, skipped, or failed.

### 4.5 Single compressed files

A standalone compressed file can be opened directly as a decompressed binary
stream, or through the archive interface as a one-member archive. The single
member's name is derived from the source name (dropping the recognised
compression suffix); for some formats the original name and time stored inside
the compressed file can optionally be used instead, and the decompressed size is
reported when it can be learned cheaply.

### 4.6 Declaring access intent, and reading the cost

The library separates **how the caller intends to use the archive** from **how
expensive that turns out to be**, and never makes the caller learn backend names
to get good behaviour.

- **Intent (the request).** When opening, the caller may declare an access
  intent: *automatic* (the default — random access on a seekable source, paying
  nothing extra), *sequential* (forward-only, accepts non-seekable sources), or
  *random* (the caller will reach members out of order or seek within a member,
  so the library should proactively make that cheap). Random intent is
  best-effort: where it can be made cheap it is, and where the format or
  available backends cannot, the library falls back rather than failing — the
  only hard failure is genuinely impossible random access (a non-seekable
  source). Intent replaces any need to set low-level performance switches by
  hand.

- **Cost (the receipt).** The library helps callers avoid performance
  footguns rather than letting them discover costs by trial and error. An opened
  archive should give callers enough information — without doing I/O to produce it
  — to tell cheap operations from expensive ones (a one-seek listing versus a full
  scan; a direct member open versus one that re-decompresses everything before it)
  and to know whether a content stream can truly seek or only appears to. The
  exact shape of this information is left to the implementer; the requirement is
  that the honest cost is *available* to a caller who wants to reason about it.

### 4.7 Configuration

Behaviour is tuned through a configuration object that can be passed per call,
set as a process-wide default, or temporarily overridden for a scope. It selects
optional backends, the default extraction filter, the overwrite policy, and
similar behaviours. Optional-backend selection is three-state — always use (and
fail if unavailable), never use, or *use when it helps and is installed* (the
default) — so that declared intent can decide automatically. Convenience string
literals are accepted where an option is really an enumerated choice.

### 4.8 Writing archives

The library can **create** archives, at first in the formats where writing is
straightforward — TAR (plain and compressed), ZIP, and the single-file
compressors — with room to add others later. Writing is intentionally simple
(create a new archive; it is not an editor for existing ones, see §10).

The shape of a writing API, conceptually:

- The caller opens a destination — a path or a binary stream, **including a
  non-seekable stream** for formats that can be written sequentially — choosing
  the target format and any archive-level options (compression method/level,
  comment).
- The caller adds members one at a time. A member can be supplied as a path on
  disk, as bytes, or as a readable stream whose content is **consumed
  incrementally** (never buffered whole, per principle 8); directories, symlinks,
  and hardlinks are added by description rather than content. Per-member metadata
  (name, modification time, permissions, etc.) is taken from the source or set
  explicitly.
- The output is produced as members are added, so writing a huge archive to a
  pipe streams straight through.
- Closing finalises the archive (writing any trailing index the format needs).
  The whole thing works as a context-managed resource, like reading.

The reading and writing models share the **same member abstraction**, which is
what makes conversion fall out for free (see §4.9): a member obtained from a
reader can be handed to a writer.

Formats that cannot reasonably be written incrementally, or at all (RAR; ISO;
7z initially), are simply not offered as write targets rather than emulated
badly.

### 4.9 Converting between formats

Because a member read from one archive can be written into another, converting an
archive from one format to another — or recompressing a single-file stream — is
just *iterate the source, write to the destination*, with no separate conversion
machinery. The requirement is that the read and write surfaces stay compatible
enough (the same member objects, streamed content) that piping a decompressor
into a compressor, or one container into another, is easy and memory-bounded.

### 4.10 Command-line use

A small command-line tool can list and extract archives, primarily for testing
and exploration. It is not the library's main surface but must exercise the same
behaviours.

---

## 5. Safety and extraction filters

Extraction of untrusted input is sanitised by **filters**, modelled on the named
filters of the standard library's TAR extraction so the mental model transfers.

Three built-in policies are provided:

- A **fully-trusted** policy that passes members through unchanged (for input the
  caller already trusts).
- A **tar** policy that sanitises paths and permissions.
- A **data** policy — the strict default — that additionally rejects special
  files, strips ownership, and de-escalates permissions.

The sanitising policies must:

- reject absolute paths and parent-directory traversal, and reject any member
  that would resolve outside the destination;
- verify link targets (symlink targets resolved relative to the link, hardlink
  targets as paths inside the root) and reject those that escape — including
  junctions whose targets point outside the archive;
- sanitise permission bits, with the strict policy going further (dropping
  executable and privileged bits, normalising ownership).

Filters are callable as part of both iteration and extraction, may rename, adjust
or skip a member, and can be configured to either reject unsafe members loudly or
skip them with a warning. Callers may supply a fully custom filter.

---

## 6. Error model

All errors caused by an archive problem derive from a single common error type,
so a caller can catch one base type and still discriminate causes. Concretely:

- Underlying third-party and standard-library exceptions are **always translated**
  into the library's own hierarchy; raw library exceptions never escape.
- The hierarchy distinguishes, at least: read/decode errors (with corruption and
  unexpected-end-of-file as a coherent sub-group, plus unsupported-feature and
  not-seekable cases); member errors (not found, cannot be opened, link target
  missing); extraction errors (such as a refusing-to-overwrite conflict);
  encryption errors (missing or wrong password); filter rejections; unsupported
  or undetectable formats; and a missing optional dependency.
- Errors carry context — which archive, and which member was being processed —
  and include it in their message.
- Errors raised while reading a member's stream are translated the same way as
  errors raised at open time, so streaming callers see the same hierarchy.

The library raises (rather than silently continuing) whenever correctness or
safety is at stake. It only warns-and-continues where recovery is genuinely safe.

---

## 7. Format detection

Format is auto-detected by default. Detection is **content-based first** — it
identifies the format by inspecting the data itself, not by trusting the
filename. A filename extension may be used as a secondary hint, but when content
and name disagree, content wins. Detection must leave the input usable afterwards
(it never consumes a stream destructively and works on non-seekable sources by
reading only what it needs).

How content detection is performed for formats without a clean signature, how
deeply it looks (e.g. recognising a compressed TAR, or an archive embedded in a
self-extracting executable), and how name hints are weighed are left to the
implementer.

---

## 8. Format and library landscape — quirks that shape the design

Each format and its typical backing library carries behaviours that the unified
interface must paper over. These are the constraints a re-implementer must plan
for; they explain why several requirements above exist. Implementation choices
(which library, native parser, or codec) are left open.

### 8.1 ZIP

- **Member catalog lives at the end** (the central directory), so a full listing
  needs the tail of the file. The format *can* be streamed via per-entry local
  headers, but some metadata (Unix permissions, symlink-ness, per-file comments)
  lives only in the central directory and is unavailable when streaming.
- **Timestamps are weak by default:** DOS time is local, two-second granularity,
  no timezone. A higher-precision UTC time exists only in an optional extra field
  that must be parsed explicitly. Some writers emit invalid dates or a
  zero-date sentinel, which must degrade to "unknown" rather than crash.
- **Filename/comment encoding is ambiguous:** a UTF-8 flag exists but legacy
  tools wrote other code pages without setting it; comments are raw bytes. The
  library must decode defensively with fallbacks.
- **No native symlinks or hardlinks:** Unix symlinks are a convention (a mode bit
  plus the target stored as file content), recognisable only for Unix-created
  archives; hardlinks are not representable at all.
- **Encryption:** only legacy ZipCrypto is broadly handled by the common backend;
  strong/AES encryption may be unsupported and must surface as a clear
  unsupported-feature or encryption error.

### 8.2 TAR

- **No central directory:** the member list only exists by scanning the whole
  archive; for a compressed TAR, reaching a member means decompressing everything
  before it. A compressed TAR is therefore treated as solid.
- **Silent truncation:** a TAR cut off mid-archive can look like a clean end. The
  library must verify the expected end-of-archive marker and raise on truncation
  rather than return a short, plausible-looking listing. (This check is on by
  default and can be disabled.)
- **Short reads and lying seekability:** decompressing wrappers may return fewer
  bytes than requested and may claim to be seekable while only supporting seek by
  re-reading from the start; the library must not be fooled into incorrect
  random-access decisions or corrupted reads.
- **Rich native metadata:** TAR has true symlinks *and* hardlinks, Unix
  ownership and permissions, and UTC times (sub-second under the PAX variant).
  Hardlink targets can appear later in the archive, which complicates resolution
  in a single forward pass.
- **No per-member data checksum:** integrity beyond the header cannot be verified.
- **Compressed-TAR random access depends entirely on the decompressor:** a plain
  stdlib decompressor only rewinds-and-replays (expensive), whereas
  index-building backends turn it into bounded or near-direct seeking. This is the
  central reason cost is reported per-stream rather than per-format (§4.6).

### 8.3 RAR

- Member headers precede each file (no front index); a true upfront catalog
  exists only when an optional end-of-file "quick open" index is present — so
  whether listing is cheap is a property of the individual file, not the format.
- Requires an external decompression tool; only metadata parsing is done in
  process. Decompression of a **solid** RAR re-reads all preceding members per
  access unless a single-pass streaming path is used, which is efficient but only
  during in-order iteration.
- **Per-generation quirks:** older RAR truncates non-BMP characters in UTF-16
  filenames (a bug to work around); RAR4 stores local time while RAR5 stores UTC;
  RAR5 can encrypt headers (needing a key derived from the password just to read
  the listing) and can use password-tweaked checksums that must *not* be reported
  as plain CRCs. Symlinks/junctions appear via redirection metadata; link targets
  sometimes require actually extracting the member.
- **Multi-volume** archives (a set split across several files) are a supported
  goal: the volumes are read as one logical archive. Very old RAR generations
  (e.g. RAR2) are out of scope and must fail cleanly rather than mishandle input.

### 8.4 7z

- **Almost always solid:** the default packs everything into one compression
  unit, so reaching any member decompresses the whole unit. Sequential extraction
  is the natural access model; out-of-order access is inherently expensive and
  must be reported as such.
- **Sparse metadata:** the format carries no Unix ownership and limited
  permissions; symlinks are stored as regular files with a special attribute;
  junctions are a distinct concept that must be recognised and preserved.
- **Encryption is per compression unit**, and per-member passwords must be
  supportable (the password belongs to the open/read call, not bound globally to
  the archive).
- **Codec breadth:** beyond the mainstream codecs, 7z uses filter chains and
  less-common codecs; the compression method must be reported losslessly even
  when it is a chain. A few codecs (e.g. certain branch filters) may be
  undecodable and must raise a clean unsupported-feature error.
- Duplicate filenames are possible and must remain individually addressable.
- **Multi-volume** 7z archives are a supported goal, read as one logical archive.

### 8.5 ISO 9660

- **Multiple overlapping namespaces** (plain ISO 9660, Rock Ridge, Joliet, UDF)
  can coexist in one image, each with different names and metadata. The library
  must pick the richest available (Rock Ridge, then Joliet, then plain) so it
  never silently truncates names or drops POSIX metadata that another namespace
  preserved. Plain ISO names carry a version suffix that must be stripped.
- **POSIX metadata, symlinks, and accurate timestamps exist only under Rock
  Ridge;** plain ISO 9660 has none of these. Detection of the extension must be
  robust to images that carry the metadata without the official marker.
- **No compression and no encryption:** stored size equals real size; data access
  is cheap, sector-aligned, and random. Timestamps carry a timezone offset but in
  a non-standard encoding that must be converted.
- Large files may span multiple extents, so a single record's length is not
  necessarily the whole file size.

### 8.6 Folders (directories on disk)

- A real directory is presented through the same interface: traversed
  deterministically, top-down, directories before their contents, without
  following symlinks.
- Repeated inodes encountered during the walk are reported as hardlinks to the
  first occurrence; symlinks and junctions are read as links (and junctions are
  **not** recursed into). Member access is confined to the folder root.

### 8.7 Single-file compressors

- These formats carry essentially no metadata (gzip optionally stores an original
  name and time; xz and lzip can report decompressed size cheaply on a seekable
  source; the rest cannot). They are presented as a one-member archive whose
  member name is inferred from the source.
- Random access within the decompressed stream ranges from cheap (formats with a
  block index) to rewind-only (formats without one), which the per-stream cost
  reporting must reflect honestly.

### 8.8 Cross-cutting consequences

- **Timezones and precision differ per format**; the model exposes
  timezone-aware vs naive accordingly and never fabricates a zone.
- **Symlink/hardlink/junction support is wildly uneven**; the unified model
  always reports a link's type and, when known, its target's type, so callers
  need not resolve a link just to learn it points to a directory.
- **"Solid" is a spectrum, not a flag:** the realised cost depends on format,
  backend, and seekability together — hence intent-as-request and cost-as-receipt
  rather than a single boolean.
- **Backends are replaceable:** any given format may be served by a standard-
  library module, a third-party package, an external tool, or a native parser.
  The requirements here constrain observable behaviour only; which backend
  provides it, and whether optional dependencies are required, is an
  implementation decision — subject to the rule that a missing optional
  dependency produces a clear, specific error.

---

## 9. Testing and verification

The library's central promise — *one interface, many formats, same results* — is
only credible if it is verified that way. The test strategy is therefore a
first-class requirement, not an afterthought, and the following must hold.

- **One catalog of sample archives.** Every sample archive is defined once, in a
  single place, together with how it is built, what it contains, and which
  features it exercises. Tests select from that catalog declaratively (by format,
  by feature, by predicate) rather than re-defining archives inline. Fixtures are
  generated reproducibly from those definitions and built on demand when not
  already present.

- **Cross-format and cross-backend equivalence.** The same logical content,
  packaged in different formats and read through different backends (standard
  library, optional third-party libraries, external tools, native parsers), must
  produce the same members, the same metadata, and the same bytes. This is the
  test that proves the unification is real rather than nominal.

- **The full matrix.** Tests run across the supported language runtimes and across
  dependency sets — at least a minimal install (no optional packages) and a
  full install — so both the zero-dependency path and every optional-backend path
  are covered, on each supported operating system (permissions, symlinks,
  junctions, and path rules differ per platform).

- **Missing optional dependencies skip, not fail.** When a case needs an optional
  package or external tool that is absent, it is skipped centrally — mirroring the
  library's own runtime contract — while genuine failures still fail loudly.

- **Adversarial and malformed input.** Corrupted, truncated, and deliberately
  hostile archives (path traversal, absolute paths, escaping links, decompression
  bombs) are part of the suite, verifying that safe-by-default extraction holds,
  that truncation and corruption are detected rather than silently accepted, and
  that failures surface as the right error type instead of crashing or doing
  something unsafe.

- **Round-trip and conversion identity.** Anything written is read back and
  compared: write-then-read must reproduce the content and the metadata both
  formats can represent, and converting between formats must preserve member data
  and as much metadata as the target format supports.

- **Streaming and memory behaviour.** The streaming principle is tested, not just
  asserted: large inputs are processed without buffering the whole thing,
  non-seekable sources work everywhere they are promised to, and per-member
  streams are released as iteration advances.

- **Real-world archives, not only self-generated ones.** The suite also exercises
  archives produced by the actual native tools and other libraries — including
  old, unusual, and edge-case files — so the library is validated against data it
  will meet in practice, not only against fixtures it created itself.

How this is implemented (test framework, parametrization mechanism, fixture
storage) is left to the implementer; the requirement is the coverage and the
guarantees above.

---

## 10. Non-goals and known boundaries

- **No in-place modification** of an existing archive. Writing produces a new
  archive (§4.8); it does not add to, or rewrite a member inside, a file that
  already exists.
- **No faithful re-creation of every exotic entry** on every OS (for example,
  recreating a native Windows junction during extraction); such entries fall back
  to the nearest safe representation, and unsafe targets are dropped by the
  default filter.
- **No support for the oldest format generations** (e.g. RAR2); these fail
  cleanly rather than silently mishandling input. (Multi-volume archives, by
  contrast, are a supported goal — see §8.3/§8.4.)
- **No measured or predicted performance numbers:** the cost information of §4.6
  distinguishes cheap from expensive operations; it is not a wall-clock estimate.

---

## 11. Summary

Archivey's contract is: *give me any supported archive or compressed file, by
path or by stream, and let me list, read, and safely extract it through one
interface, one metadata model, and one error hierarchy — defaulting to correct
and safe, telling me honestly what each operation costs, and never silently
losing information the format recorded or doing expensive work behind my back.*
Everything in this document serves that sentence.
