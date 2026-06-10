# Implementation Tasks: Concurrent member access (exploration)

> **Exploration stage.** Do section 1 first; its findings gate `access-intent`
> task 3.4 and shape the contract before any implementation.

## 1. Exploration (do first)

- [ ] 1.1 Inventory current behavior: for each format reader and decompressor backend,
      open two different members and the same member twice, read them interleaved and
      from separate threads, and record the outcome (correct / wrong bytes / exception
      / crash). Include path-opened and stream-opened archives
- [ ] 1.2 Specifically stress rapidgzip and indexed_bzip2 with multiple
      concurrently-open member streams and multi-threaded reads; record whether one
      decompressor can serve several streams and what duplicate indexes cost in
      memory. Report the verdict to `access-intent` (its default flip depends on it)
- [ ] 1.3 Test on a free-threaded (no-GIL) build given the `optional-freethreaded`
      extra
- [ ] 1.4 Decide the contract: per-stream thread confinement vs shareable streams;
      behavior for solid archives (independent `EXPENSIVE` passes vs restriction);
      behavior for subprocess-backed streams (`unrar p`), which cannot be multiplexed
- [ ] 1.5 Design the shared-source multiplexer (lock-based, seek-to-position-per-read
      over a caller-provided stream; per-stream file handles for path-opened archives;
      prior art: stdlib `zipfile._SharedFile`); coordinate the seam with
      `public-stream-interface`

## 2. Specification & implementation (after the contract is settled)

- [ ] 2.1 Strengthen the `archive-reading` delta with the decided contract (including
      solid-archive and subprocess-stream carve-outs, and the free-threaded statement)
- [ ] 2.2 Implement per-stream file handles for path-opened archives and the
      multiplexer for stream-opened ones
- [ ] 2.3 Add concurrency stress tests to the suite (run across formats × backends ×
      path/stream sources; include same-member-twice and many-streams cases)
- [ ] 2.4 Document the contract in the user guide and on `ArchiveReader.open`

## 3. Validation

- [ ] 3.1 `openspec validate concurrent-member-access --type change --strict`
- [ ] 3.2 `hatch run lint` and `hatch run test`
