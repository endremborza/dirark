"""dirark's benchmark matrix: corpora × size tiers × operations, tracked per commit.

Run `uv run python benchmarks/bench.py <command>`:

    status             corpora, their tiers, what is fetched
    fetch [NAME...]    download and pin public corpora
    run [REF]          measure a commit, or the working tree
    report             write benchmarks/report.html
    sweep CORPUS TIER  frame size study on the working tree

Data lives under $DIRARK_BENCH_DATA; see docs/benchmarks.md.
"""

import random
import shutil
import statistics
import time

from corpora import PUBLIC, corpus_names, data_dir, fetch, manifest, tier_dir, tiers
from measure import du, evict
from protocli import Dispatcher
from report import report
from runner import READS, run

from dirark import ArkReader, archive_dir
from dirark.blob import FRAME_MB
from dirark.storage import MAX_FRAME_MB, MAX_TAR_MB


def status() -> None:
    """List every corpus with its tiers, or say it is not fetched yet."""
    for name in corpus_names():
        if not (data_dir() / "corpora" / name).is_dir():
            print(f"{name}: not fetched")
            continue
        files = manifest(name)
        parts = [f"{t.name} {t.bytes / 1e9:.2f} GB/{len(t.files)}" for t in tiers(name)]
        size = sum(e.size for e in files) / 1e9
        print(f"{name}: {size:.2f} GB in {len(files)} files; {', '.join(parts)}")


def fetch_corpora(*names: str) -> None:
    """Fetch the named public corpora, or every one not fetched yet."""
    pending = names or [n for n in PUBLIC if not (data_dir() / "corpora" / n).exists()]
    for name in pending:
        t0 = time.perf_counter()
        fetch(name)
        print(f"{name}: {time.perf_counter() - t0:.0f} s")


def sweep(corpus: str, tier: str) -> None:
    """Archive a tier at each frame size in use; packed MiB, archive s, cold read ms."""
    case = next(t for t in tiers(corpus) if t.name == tier)
    src, work = tier_dir(case), data_dir() / "work"
    sample = random.Random(case.key).sample(case.files, min(READS, len(case.files)))
    print(f"{'frame MB':>8} {'packed MiB':>11} {'archive s':>10} {'cold read ms':>13}")
    for frame_mb in sorted({FRAME_MB, MAX_FRAME_MB, MAX_TAR_MB}):
        ark = work / f"sweep-{frame_mb}"
        shutil.rmtree(ark, ignore_errors=True)
        evict(src)
        t0 = time.perf_counter()
        archive_dir(src, ark_out=ark, max_frame_mb=frame_mb)
        secs = time.perf_counter() - t0
        cold = []
        for e in sample:
            evict(ark)
            t0 = time.perf_counter()
            with ArkReader(ark) as reader:
                reader.read_file(e.path)
            cold.append((time.perf_counter() - t0) * 1e3)
        mib = du(ark) / 2**20
        med = statistics.median(cold)
        print(f"{frame_mb:>8} {mib:>11.1f} {secs:>10.2f} {med:>13.1f}")
        shutil.rmtree(ark)


main = Dispatcher(
    "bench",
    {
        "status": status,
        "fetch": fetch_corpora,
        "run": run,
        "report": report,
        "sweep": sweep,
    },
).run

if __name__ == "__main__":
    raise SystemExit(main())
