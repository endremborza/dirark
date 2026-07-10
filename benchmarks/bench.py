"""Performance benchmarks for dirark.

Run:
    uv run python benchmarks/bench.py            # full
    uv run python benchmarks/bench.py --quick    # smaller inputs, fewer reps

The suite exists to settle implementation trade-offs with measurement instead
of assumption. The headline case is checksumming: the production path shells
out to the system ``b2sum``; the suite times it against an in-process
``hashlib`` variant across file-size regimes so the choice is data-backed.
Add a benchmark by writing a ``bench_*`` function that returns ``list[Sample]``
and listing it in ``main``.
"""

import argparse
import hashlib
import os
import statistics
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dirark.core import archive_dir
from dirark.storage import b2sum  # production: shells out to system b2sum

KiB = 1024
MiB = 1024 * 1024


@dataclass
class Sample:
    name: str
    seconds: list[float]

    def report(self) -> str:
        med = statistics.median(self.seconds) * 1e3
        best = min(self.seconds) * 1e3
        return f"  {self.name:<34} median {med:9.2f} ms   min {best:9.2f} ms"


def _hashlib_b2sum(path: Path) -> str:
    h = hashlib.blake2b()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(MiB), b""):
            h.update(chunk)
    return h.hexdigest()


def _measure(name: str, fn: Callable[[], object], reps: int) -> Sample:
    secs: list[float] = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        secs.append(time.perf_counter() - t0)
    return Sample(name, secs)


def _make_files(root: Path, n: int, size: int) -> list[Path]:
    paths = []
    for i in range(n):
        p = root / f"f{i:05d}.bin"
        p.write_bytes(os.urandom(size))
        paths.append(p)
    return paths


def bench_checksum_small(reps: int, n: int, size: int) -> list[Sample]:
    with tempfile.TemporaryDirectory() as tmp:
        paths = _make_files(Path(tmp), n, size)
        tag = f"({n} x {size // KiB or 1} KiB)"
        return [
            _measure(
                f"b2sum subprocess  {tag}", lambda: [b2sum(p) for p in paths], reps
            ),
            _measure(
                f"hashlib in-proc   {tag}",
                lambda: [_hashlib_b2sum(p) for p in paths],
                reps,
            ),
        ]


def bench_checksum_large(reps: int, size: int) -> list[Sample]:
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "big.bin"
        p.write_bytes(os.urandom(size))
        tag = f"(1 x {size // MiB} MiB)"
        return [
            _measure(f"b2sum subprocess  {tag}", lambda: b2sum(p), reps),
            _measure(f"hashlib in-proc   {tag}", lambda: _hashlib_b2sum(p), reps),
        ]


def bench_archive(reps: int, n: int, size: int) -> list[Sample]:
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "src"
        src.mkdir()
        _make_files(src, n, size)
        secs: list[float] = []
        for i in range(reps):
            ark = Path(tmp) / f"ark{i}"
            t0 = time.perf_counter()
            archive_dir(src, ark_out=ark)
            secs.append(time.perf_counter() - t0)
        return [Sample(f"archive_dir  ({n} x {size // KiB} KiB)", secs)]


def main() -> None:
    ap = argparse.ArgumentParser(description="dirark performance benchmarks")
    ap.add_argument("--quick", action="store_true", help="smaller inputs, fewer reps")
    args = ap.parse_args()

    if args.quick:
        reps, small_n, small_sz, big_sz = 3, 200, KiB, 16 * MiB
    else:
        reps, small_n, small_sz, big_sz = 5, 2000, KiB, 128 * MiB

    groups = {
        "checksum — many small files": bench_checksum_small(reps, small_n, small_sz),
        "checksum — one large file": bench_checksum_large(reps, big_sz),
        "archive_dir — many small files": bench_archive(reps, small_n, small_sz),
    }
    for title, samples in groups.items():
        print(f"\n{title}")
        for s in samples:
            print(s.report())
    print()


if __name__ == "__main__":
    main()
