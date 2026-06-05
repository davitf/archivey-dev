#!/usr/bin/env python3
"""
XZ decompressor benchmark: lzma.open vs python-xz vs XzDecompressorStream.

Generates all test files on the fly (nothing stored). Prints a markdown table
and saves JSON results to benchmarks/results/YYYY-MM-DD.json.

Usage:
    uv run python benchmarks/bench_xz.py [--size-mb N] [--runs N] [--quick]

Options:
    --size-mb N   Uncompressed data size in MB (default: 100)
    --runs N      Timing repetitions per measurement (default: 3)
    --quick       Use 20 MB and 1 run for a fast smoke-test
"""

from __future__ import annotations

import argparse
import importlib.metadata
import io
import json
import lzma
import platform
import random
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import IO, Callable

# Ensure the project source is importable when run from the repo root.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from archivey.config import ArchiveyConfig
from archivey.core import open_archive
from archivey.formats.decompressor_stream import XzDecompressorStream

try:
    import xz as _python_xz
except ImportError:
    _python_xz = None

# ── Synthetic data ────────────────────────────────────────────────────────────

_VOCAB = (
    "the quick brown fox jumps over the lazy dog and a sample sentence "
    "to increase vocabulary diversity for better compression ratio testing "
    "archivey benchmarks xz lzma decompressor stream block index seek "
    "python library performance measurement throughput latency random access "
    "file archive format read write open close member extract list"
).split()


def make_data(size_mb: int, seed: int = 42) -> bytes:
    """Seeded synthetic data: 80% word-repetition text, 20% limited random bytes."""
    rng = random.Random(seed)
    target = size_mb * 1024 * 1024
    buf = bytearray()
    sentence_count = 0
    while len(buf) < target:
        n = rng.randint(10, 25)
        buf.extend((" ".join(rng.choices(_VOCAB, k=n)) + "\n").encode())
        sentence_count += 1
        if sentence_count % 10 == 0:
            # interleave a small chunk of low-entropy bytes (printable ASCII range)
            buf.extend(bytes(rng.randint(32, 126) for _ in range(64)))
    return bytes(buf[:target])


# ── XZ file-variant builders ──────────────────────────────────────────────────

def build_single_block(data: bytes) -> bytes:
    return lzma.compress(data, format=lzma.FORMAT_XZ)


def build_multi_stream(data: bytes, n_streams: int = 100) -> bytes:
    chunk = len(data) // n_streams
    parts = [data[i * chunk : (i + 1) * chunk] for i in range(n_streams)]
    parts[-1] += data[n_streams * chunk :]
    return b"".join(lzma.compress(p, format=lzma.FORMAT_XZ) for p in parts)


def build_multi_block(data: bytes, block_size_mb: int = 1) -> bytes | None:
    """Use the xz binary to produce a single-stream multi-block XZ file.

    Returns None if the xz binary is not available.
    """
    if not shutil.which("xz"):
        return None
    with tempfile.NamedTemporaryFile(suffix=".raw", delete=False) as f:
        f.write(data)
        raw = f.name
    out = raw + ".xz"
    try:
        subprocess.run(
            ["xz", f"--block-size={block_size_mb}MiB", "--keep", raw],
            check=True,
            capture_output=True,
        )
        return Path(out).read_bytes()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
    finally:
        Path(raw).unlink(missing_ok=True)
        Path(out).unlink(missing_ok=True)


def build_trailing(base: bytes, trailer_kb: int = 4) -> bytes:
    """Append non-XZ trailing bytes — breaks backward index scan."""
    rng = random.Random(999)
    # Start with a non-null byte so it isn't mistaken for XZ stream padding.
    trailer = bytes(
        [rng.randint(1, 255)] + [rng.randint(0, 255) for _ in range(trailer_kb * 1024 - 1)]
    )
    return base + trailer


# ── Benchmark primitives ──────────────────────────────────────────────────────

CHUNK = 65536
READ_SIZE = 1024 * 1024  # read 1 MB per seek point
SEEK_FRACTIONS = [0.10, 0.30, 0.60, 0.90]


def _median(times: list[float]) -> float:
    s = sorted(times)
    return s[len(s) // 2]


def _measure(fn: Callable[[], None], runs: int) -> float:
    times = [0.0] * runs
    for i in range(runs):
        t0 = time.perf_counter()
        fn()
        times[i] = time.perf_counter() - t0
    return _median(times)


def op_open_size(open_fn: Callable[[str], IO[bytes]], path: str, runs: int) -> float:
    """Open + determine decompressed size."""
    def _run() -> None:
        f = open_fn(path)
        if hasattr(f, "try_get_size"):
            f.try_get_size()
        else:
            try:
                f.seek(0, io.SEEK_END)
            except (io.UnsupportedOperation, OSError):
                pass
        f.close()
    return _measure(_run, runs)


def op_sequential(open_fn: Callable[[str], IO[bytes]], path: str, runs: int) -> float:
    """Read entire file sequentially in 64 KB chunks."""
    def _run() -> None:
        f = open_fn(path)
        while f.read(CHUNK):
            pass
        f.close()
    return _measure(_run, runs)


def op_seek(
    open_fn: Callable[[str], IO[bytes]],
    path: str,
    uncompressed_size: int,
    runs: int,
) -> float | None:
    """Seek to 4 positions and read 1 MB each. Returns None if seeking is unsupported."""
    def _run() -> None:
        f = open_fn(path)
        for frac in SEEK_FRACTIONS:
            f.seek(int(uncompressed_size * frac))
            f.read(READ_SIZE)
        f.close()

    # Probe first to see if seeking is available.
    probe = open_fn(path)
    seekable = probe.seekable()
    probe.close()
    if not seekable:
        return None

    return _measure(_run, runs)


# ── Library definitions ───────────────────────────────────────────────────────

def _open_lzma(path: str) -> IO[bytes]:
    return lzma.open(path, "rb")


def _open_python_xz(path: str) -> IO[bytes]:
    assert _python_xz is not None
    return _python_xz.open(path)


def _open_our_xz(path: str) -> IO[bytes]:
    return XzDecompressorStream(path)


LIBRARIES: list[tuple[str, Callable[[str], IO[bytes]] | None]] = [
    ("lzma.open (stdlib)", _open_lzma),
    ("python-xz", _open_python_xz if _python_xz is not None else None),
    ("XzDecompressorStream", _open_our_xz),
]


# ── Table formatting ──────────────────────────────────────────────────────────

def _fmt_ms(v: float | None) -> str:
    if v is None:
        return "n/a"
    return f"{v * 1000:.1f} ms"


def _fmt_mbs(elapsed: float | None, size_mb: int) -> str:
    if elapsed is None or elapsed == 0:
        return "n/a"
    return f"{size_mb / elapsed:.1f} MB/s"


def _print_table(title: str, headers: list[str], rows: list[list[str]]) -> None:
    col_w = [max(len(h), max((len(r[i]) for r in rows), default=0)) for i, h in enumerate(headers)]
    sep = "─" * (sum(col_w) + 3 * (len(headers) - 1) + 4)
    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)
    hdr = "  " + "   ".join(h.ljust(col_w[i]) for i, h in enumerate(headers))
    print(hdr)
    print("  " + "   ".join("─" * w for w in col_w))
    for row in rows:
        print("  " + "   ".join(c.ljust(col_w[i]) for i, c in enumerate(row)))
    print(sep)


# ── Main benchmark ────────────────────────────────────────────────────────────

def run_benchmarks(size_mb: int, runs: int) -> dict:
    print(f"\nGenerating {size_mb} MB of synthetic data...")
    data = make_data(size_mb)
    uncompressed_size = len(data)

    print("Building XZ file variants...")
    variants: list[tuple[str, bytes | None]] = []

    single = build_single_block(data)
    variants.append(("single_block", single))

    multi_block = build_multi_block(data, block_size_mb=1)
    if multi_block is None:
        print("  (xz binary not found — skipping multi_block_1mb variant)")
    variants.append(("multi_block_1mb", multi_block))

    multi_stream = build_multi_stream(data)
    variants.append(("multi_stream_100", multi_stream))

    if multi_block is not None:
        trailing = build_trailing(multi_block)
        variants.append(("multi_block_trailing", trailing))
    else:
        trailing_single = build_trailing(single)
        variants.append(("single_block_trailing", trailing_single))

    results: dict = {
        "meta": {
            "date": str(date.today()),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "size_mb": size_mb,
            "runs": runs,
            "libraries": {
                "lzma": "stdlib",
                "python_xz": _pkg_version("python-xz"),
                "XzDecompressorStream": "this build",
            },
        },
        "variants": {},
    }

    open_size_rows: list[list[str]] = []
    sequential_rows: list[list[str]] = []
    seek_rows: list[list[str]] = []

    for variant_name, compressed in variants:
        if compressed is None:
            continue

        compressed_mb = len(compressed) / (1024 * 1024)
        ratio = uncompressed_size / len(compressed)
        print(f"\n  {variant_name}: {compressed_mb:.1f} MB compressed ({ratio:.1f}x ratio)")

        results["variants"][variant_name] = {}

        with tempfile.NamedTemporaryFile(suffix=".xz", delete=False) as tf:
            tf.write(compressed)
            tmp_path = tf.name

        try:
            for lib_name, open_fn in LIBRARIES:
                if open_fn is None:
                    open_size_rows.append([variant_name, lib_name, "skipped (not installed)"])
                    sequential_rows.append([variant_name, lib_name, "skipped (not installed)", ""])
                    seek_rows.append([variant_name, lib_name, "skipped (not installed)"])
                    results["variants"][variant_name][lib_name] = {"skipped": True}
                    continue

                t_open = op_open_size(open_fn, tmp_path, runs)
                t_seq = op_sequential(open_fn, tmp_path, runs)
                t_seek = op_seek(open_fn, tmp_path, uncompressed_size, runs)

                open_size_rows.append([variant_name, lib_name, _fmt_ms(t_open)])
                sequential_rows.append([
                    variant_name, lib_name,
                    _fmt_mbs(t_seq, size_mb),
                    _fmt_ms(t_seq),
                ])
                seek_rows.append([variant_name, lib_name, _fmt_ms(t_seek)])

                results["variants"][variant_name][lib_name] = {
                    "open_size_ms": round(t_open * 1000, 2),
                    "sequential_mbs": round(size_mb / t_seq, 2) if t_seq else None,
                    "sequential_ms": round(t_seq * 1000, 2),
                    "seek_4x_ms": round(t_seek * 1000, 2) if t_seek is not None else None,
                }

                print(
                    f"    {lib_name:<30} open={_fmt_ms(t_open):>10}  "
                    f"seq={_fmt_mbs(t_seq, size_mb):>12}  "
                    f"seek={_fmt_ms(t_seek):>10}"
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    _print_table(
        "OPEN + SIZE (index scan, lower is better)",
        ["Variant", "Library", "Time"],
        open_size_rows,
    )
    _print_table(
        "SEQUENTIAL THROUGHPUT (higher is better)",
        ["Variant", "Library", "MB/s", "Total"],
        sequential_rows,
    )
    _print_table(
        "SEEK LATENCY — 4 seeks × 1 MB read (lower is better; n/a = no seeking)",
        ["Variant", "Library", "Time"],
        seek_rows,
    )

    return results


# ── tar.xz benchmark ──────────────────────────────────────────────────────────

def run_tar_benchmark(member_mb: int, n_members: int, runs: int) -> dict:
    total_mb = member_mb * n_members
    print(f"\n{'─'*60}")
    print(f"tar.xz benchmark: {n_members} members × {member_mb} MB = {total_mb} MB")
    print(f"{'─'*60}")

    print("Building tar.xz (multi-block, 1 MB blocks)...")
    member_data = make_data(member_mb, seed=77)

    with tempfile.NamedTemporaryFile(suffix=".tar.xz", delete=False) as tf:
        tar_path = tf.name

    # Write uncompressed tar to a temp file, then xz-compress it with blocks.
    raw_tar_path = tar_path + ".raw.tar"
    try:
        with tarfile.open(raw_tar_path, "w") as tar:
            for i in range(n_members):
                data_i = member_data[:-4] + i.to_bytes(4, "little")  # unique per member
                info = tarfile.TarInfo(name=f"member_{i:04d}.dat")
                info.size = len(data_i)
                tar.addfile(info, io.BytesIO(data_i))

        # Compress with xz blocks if available, else plain lzma
        if shutil.which("xz"):
            subprocess.run(
                ["xz", "--block-size=1MiB", "--keep", raw_tar_path],
                check=True, capture_output=True,
            )
            Path(raw_tar_path + ".xz").rename(tar_path)
        else:
            print("  (xz binary not found — using single-block compression)")
            with open(raw_tar_path, "rb") as src, lzma.open(tar_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
    finally:
        Path(raw_tar_path).unlink(missing_ok=True)

    compressed_mb = Path(tar_path).stat().st_size / (1024 * 1024)
    print(f"  {compressed_mb:.1f} MB compressed ({total_mb / compressed_mb:.1f}x ratio)")

    results = {}
    rows: list[list[str]] = []

    configs: list[tuple[str, ArchiveyConfig]] = [
        ("XzDecompressorStream", ArchiveyConfig()),
    ]
    if _python_xz is not None:
        configs.insert(0, ("python-xz", ArchiveyConfig(use_python_xz=True)))
    else:
        print("  (python-xz not installed — skipping python-xz column)")

    for lib_name, config in configs:
        timings = {}
        for member_idx, label in [(0, "first"), (n_members - 1, "last")]:
            def _extract(cfg=config, idx=member_idx) -> None:
                with open_archive(tar_path, config=cfg) as arc:
                    members = arc.get_members()
                    target = members[idx]
                    with arc.open(target) as f:
                        while f.read(CHUNK):
                            pass

            elapsed = _measure(_extract, runs)
            timings[label] = elapsed
            rows.append([lib_name, label, _fmt_ms(elapsed)])

        results[lib_name] = {
            "first_member_ms": round(timings["first"] * 1000, 2),
            "last_member_ms": round(timings["last"] * 1000, 2),
        }
        speedup = timings["first"] / timings["last"] if timings["last"] > 0 else None
        print(
            f"  {lib_name:<30} first={_fmt_ms(timings['first']):>10}  "
            f"last={_fmt_ms(timings['last']):>10}"
            + (f"  (last is {speedup:.1f}x slower)" if speedup and speedup > 1.1 else "")
            + (f"  (last is {1/speedup:.1f}x faster)" if speedup and speedup < 0.9 else "")
        )

    Path(tar_path).unlink(missing_ok=True)

    _print_table(
        f"tar.xz MEMBER EXTRACTION ({n_members} members × {member_mb} MB)",
        ["Library", "Member", "Time"],
        rows,
    )
    return results


# ── Library version helper ────────────────────────────────────────────────────

def _pkg_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--size-mb", type=int, default=100, metavar="N",
                        help="Uncompressed data size in MB (default: 100)")
    parser.add_argument("--runs", type=int, default=3, metavar="N",
                        help="Timing repetitions per measurement (default: 3)")
    parser.add_argument("--quick", action="store_true",
                        help="Use 20 MB and 1 run for a fast smoke-test")
    parser.add_argument("--tar-members", type=int, default=20, metavar="N",
                        help="Number of members in tar.xz benchmark (default: 20)")
    parser.add_argument("--tar-member-mb", type=int, default=2, metavar="N",
                        help="MB per tar member (default: 2)")
    args = parser.parse_args()

    if args.quick:
        args.size_mb = 20
        args.runs = 1
        args.tar_members = 5
        args.tar_member_mb = 1

    print("=" * 60)
    print("XZ Decompressor Benchmark")
    print("=" * 60)
    print(f"Python {platform.python_version()} | {platform.system()}")
    print(f"python-xz: {_pkg_version('python-xz') or 'not installed'}")
    if _python_xz is None:
        print("  Note: install python-xz to include it in the comparison")

    xz_results = run_benchmarks(args.size_mb, args.runs)
    tar_results = run_tar_benchmark(args.tar_member_mb, args.tar_members, args.runs)

    full_results = {**xz_results, "tar_xz": tar_results}

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{date.today()}.json"
    out_path.write_text(json.dumps(full_results, indent=2))
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
