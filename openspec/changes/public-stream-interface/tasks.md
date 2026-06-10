# Implementation Tasks: Public stream interface (`ArchiveyStream`)

> **Exploration stage.** Do the analysis tasks first; the implementation surface is not
> finalized. Depends on `base-reader-architecture-extensions` (the `seek_cost` surface).

## 1. Exploration (do first)

- [ ] 1.1 Inventory every stream class archivey constructs and every path that returns a
      stream to a caller (including raw third-party streams), and where wrapping happens
- [ ] 1.2 Decide the public shape: concrete base class vs mixin vs `Protocol` (+ runtime
      type), and the full metadata set beyond `seek_cost` + `name`
- [ ] 1.3 Decide wrap-vs-annotate, with a quick benchmark on a many-small-members archive
      if wrapper overhead is a concern
- [ ] 1.4 Decide read-only vs read+write scope, and whether `ArchiveyStream` is exported

## 2. Implementation (after the surface is settled)

- [ ] 2.1 Add the public `ArchiveyStream` type exposing `seek_cost` (consistent with
      `seekable()`) and `name`
- [ ] 2.2 Have archivey's stream classes provide it; normalize third-party streams into
      it at the `ensure_binaryio()` / `BinaryIOWrapper` seam
- [ ] 2.3 Guarantee `open()` / `iter_members_with_streams()` return only `ArchiveyStream`
      instances; export it publicly

## 3. Validation

- [ ] 3.1 `openspec validate public-stream-interface --type change --strict`
- [ ] 3.2 `hatch run lint` and `hatch run test`
