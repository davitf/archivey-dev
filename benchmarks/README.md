# Benchmarks

Manual performance benchmarks for archivey decompressor backends.
These are not run in CI — run them locally when evaluating library changes.

## bench_xz.py

Compares three XZ backends across four file variants and a tar.xz scenario:

| Library | Description |
|---|---|
| `lzma.open` (stdlib) | Baseline — no seeking support |
| `python-xz` | Optional; install with `pip install python-xz` |
| `XzDecompressorStream` | Our implementation (default backend) |

**File variants:**
- `single_block` — standard `lzma.compress()` output; one stream, one block
- `multi_block_1mb` — one stream, blocks every 1 MB (requires `xz` binary)
- `multi_stream_100` — 100 concatenated XZ streams of ~1 MB each
- `multi_block_trailing` / `single_block_trailing` — valid XZ + trailing junk bytes (breaks backward index scan, exercises fallback path)

**Operations:**
- **open + size** — time from open to size determination (index scan cost)
- **sequential** — read entire file; throughput in MB/s
- **seek 4×** — seek to 10/30/60/90% offsets and read 1 MB at each

**tar.xz benchmark:**
Creates a tar.xz with N members and measures extracting the first vs last member,
showing the benefit of block-level seeking for non-sequential access.

### Usage

```bash
# Full benchmark (100 MB, 3 runs, 20-member tar.xz)
uv run python benchmarks/bench_xz.py

# Quick smoke-test (20 MB, 1 run, 5-member tar.xz)
uv run python benchmarks/bench_xz.py --quick

# Custom size and repetitions
uv run python benchmarks/bench_xz.py --size-mb 200 --runs 5

# With python-xz for three-way comparison
uv install python-xz
uv run python benchmarks/bench_xz.py
```

### Test data

Seeded synthetic data (reproducible, no external downloads):
- 80% word-repetition text from a small vocabulary
- 20% limited random bytes (printable ASCII range) interleaved every ~10 sentences
- Typically compresses at 5–8× ratio with XZ

Results are saved to `benchmarks/results/YYYY-MM-DD.json`.
