"""Run the benchmark matrix against a commit and record the results.

Each case is a tier of a corpus. A repetition archives it with `archive_dir`
and again unchanged, reads a fixed sample of its files cold, restores it, pulls
it into an empty ark and pushes it to an empty remote, then archives it again
with its probe added and pushes that; corpora whose names start with one of
BLOB_PREFIXES also go through `BlobStore` (write all and commit, read the
sample, add the probe).
Every op runs in its own `measure.py` process importing the measured commit,
while this process samples the anonymous memory of that process tree.
"""

import hashlib
import json
import os
import platform
import random
import shutil
import socket
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from corpora import WHOLE, Tier, corpus_names, data_dir, tier_dir
from corpora import tiers as corpus_tiers
from measure import du

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "benchmarks" / "results"
MEASURE = Path(__file__).with_name("measure.py")
HARNESS = hashlib.blake2b(
    MEASURE.read_bytes() + Path(__file__).read_bytes(), digest_size=6
).hexdigest()
REPS = {"100M": 3, "1G": 3, "10G": 1, WHOLE: 3}
READS = 20
BLOB_PREFIXES = ("json-", "private-depot-")
SAMPLE_S = 0.02
MAX_BUSY_CORES = 1.0
IDLE_WINDOW_S = 2.0
SPACE_FACTOR = 4
FREE_MARGIN = 10**10
MIB = 1 << 20


@dataclass(frozen=True)
class Metric:
    label: str
    unit: str
    higher_is_better: bool
    group: str


METRICS = {
    "ratio": Metric("ratio", "×", True, "size"),
    "index": Metric("index", "MiB", False, "size"),
    "blob.ratio": Metric("blob ratio", "×", True, "size"),
    "archive.wall": Metric("archive", "s", False, "write"),
    "archive.cpu": Metric("archive CPU", "s", False, "write"),
    "noop.wall": Metric("no-op re-run", "s", False, "write"),
    "noop.cpu": Metric("no-op re-run CPU", "s", False, "write"),
    "add.wall": Metric("add one", "s", False, "write"),
    "add.cpu": Metric("add one CPU", "s", False, "write"),
    "blob.write.wall": Metric("blob write", "s", False, "write"),
    "blob.write.cpu": Metric("blob write CPU", "s", False, "write"),
    "blob.add.wall": Metric("blob add one", "s", False, "write"),
    "read.p50": Metric("read p50", "ms", False, "read"),
    "read.p95": Metric("read p95", "ms", False, "read"),
    "restore.wall": Metric("restore", "s", False, "read"),
    "restore.cpu": Metric("restore CPU", "s", False, "read"),
    "blob.read.p50": Metric("blob read p50", "ms", False, "read"),
    "blob.read.p95": Metric("blob read p95", "ms", False, "read"),
    "push.wall": Metric("push", "s", False, "sync"),
    "push.cpu": Metric("push CPU", "s", False, "sync"),
    "push.add.wall": Metric("push one", "s", False, "sync"),
    "pull.wall": Metric("pull", "s", False, "sync"),
    "pull.cpu": Metric("pull CPU", "s", False, "sync"),
    "archive.mem": Metric("archive", "MiB", False, "memory"),
    "noop.mem": Metric("no-op re-run", "MiB", False, "memory"),
    "add.mem": Metric("add one", "MiB", False, "memory"),
    "read.mem": Metric("read", "MiB", False, "memory"),
    "restore.mem": Metric("restore", "MiB", False, "memory"),
    "push.mem": Metric("push", "MiB", False, "memory"),
    "push.add.mem": Metric("push one", "MiB", False, "memory"),
    "pull.mem": Metric("pull", "MiB", False, "memory"),
    "blob.write.mem": Metric("blob write", "MiB", False, "memory"),
    "blob.add.mem": Metric("blob add one", "MiB", False, "memory"),
    "blob.read.mem": Metric("blob read", "MiB", False, "memory"),
}


def run(
    ref: str | None = None,
    *,
    corpora: list[str] = [],
    tiers: list[str] = [],
    reps: int = 0,
    force: bool = False,
) -> None:
    """Measure the matrix against a commit and record it.

    A clean commit's results go to benchmarks/results/<host>/<sha>.json, merged
    case by case with earlier runs of it; a dirty working tree's replace
    worktree.json under the data dir.

    Args:
        ref: commit to measure in a worktree of its own; the working tree when omitted
        corpora: corpus names to run, every fetched one when empty
        tiers: tier names to run, every one when empty
        reps: repetitions per case, REPS by tier when 0
        force: measure on a busy machine instead of waiting for it to idle
    """
    sha = git("rev-parse", f"{ref or 'HEAD'}^{{commit}}")
    dirty = ref is None and bool(git("status", "--porcelain"))
    host = socket.gethostname()
    out = (
        data_dir() / "results" / host / "worktree.json"
        if dirty
        else RESULTS / host / f"{sha}.json"
    )
    doc = {} if dirty or not out.exists() else json.loads(out.read_text())
    doc |= {
        "commit": sha,
        "dirty": dirty,
        "subject": git("log", "-1", "--format=%s", sha),
        "date": git("log", "-1", "--format=%cI", sha),
        "machine": _fingerprint(),
    }
    cases = doc.setdefault("cases", {})
    selected = [
        t
        for name in corpora or _fetched()
        for t in corpus_tiers(name)
        if not tiers or t.name in tiers
    ]
    with _checkout(ref, sha) as root:
        for tier in selected:
            busy = _wait_idle(force)
            _check_space(tier)
            t0 = time.perf_counter()
            metrics = _case(root, tier, reps or REPS[tier.name])
            cases[tier.key] = {
                "corpus": {
                    "digest": tier.digest,
                    "bytes": tier.bytes,
                    "files": len(tier.files),
                    "probe": tier.probe and tier.probe.size,
                },
                "harness": HARNESS,
                "busy_cores": busy,
                "measured": datetime.now().isoformat(timespec="seconds"),
                "metrics": metrics,
            }
            out.parent.mkdir(parents=True, exist_ok=True)
            tmp = out.with_suffix(".tmp")
            tmp.write_text(json.dumps(doc, indent=1))
            tmp.replace(out)
            print(f"{tier.key}: {time.perf_counter() - t0:.0f} s", flush=True)


def _fetched() -> list[str]:
    return [n for n in corpus_names() if (data_dir() / "corpora" / n).is_dir()]


def _case(root: Path, tier: Tier, reps: int) -> dict[str, list[float]]:
    src, work = tier_dir(tier), data_dir() / "work"
    ark, restored, blob = work / "ark", work / "restore", work / "blob"
    pulled, remote = work / "pulled", work / "remote"
    sample = random.Random(tier.key).sample(tier.files, min(READS, len(tier.files)))
    n = len(tier.files)
    m: dict[str, list[float]] = defaultdict(list)
    for _ in range(reps):
        shutil.rmtree(work, ignore_errors=True)
        work.mkdir(parents=True)
        _record(m, "archive", _op(root, "archive", src=src, ark=ark))
        tars = sum(p.stat().st_size for p in ark.glob("*.tar.zst"))
        m["ratio"].append(tier.bytes / du(ark))
        m["index"].append((du(ark) - tars) / MIB)
        _record(m, "noop", _op(root, "archive", src=src, ark=ark))
        reads = [(e.path, e.size) for e in sample]
        _record_reads(m, "read", _op(root, "read", ark=ark, files=reads))
        _record(m, "restore", _op(root, "restore", ark=ark, out=restored))
        _check_restore(restored, tier)
        shutil.rmtree(restored)
        _record(m, "pull", _synced(tier, n, _op(root, "pull", remote=ark, ark=pulled)))
        shutil.rmtree(pulled)
        _record(m, "push", _synced(tier, n, _op(root, "push", ark=ark, remote=remote)))
        if tier.probe:
            with _linked(src, tier):
                _record(m, "add", _op(root, "archive", src=src, ark=ark))
            r = _op(root, "push", ark=ark, remote=remote)
            _record(m, "push.add", _synced(tier, n + 1, r))
        shutil.rmtree(ark)
        shutil.rmtree(remote)
        if not tier.corpus.startswith(BLOB_PREFIXES):
            continue
        files = [str(src / e.path) for e in tier.files]
        r = _op(root, "blob_write", store=blob, files=files)
        _record(m, "blob.write", r)
        m["blob.ratio"].append(tier.bytes / r["packed"])
        reads = [(str(src / e.path), e.size) for e in sample]
        _record_reads(m, "blob.read", _op(root, "blob_read", store=blob, files=reads))
        if tier.probe:
            probe = data_dir() / "corpora" / tier.corpus / tier.probe.path
            _record(m, "blob.add", _op(root, "blob_write", store=blob, files=[probe]))
    shutil.rmtree(work)
    return dict(m)


def _record(m: dict[str, list[float]], op: str, r: dict) -> None:
    for key in ("wall", "cpu", "mem"):
        m[f"{op}.{key}"].append(r[key])


def _record_reads(m: dict[str, list[float]], op: str, r: dict) -> None:
    m[f"{op}.p50"].append(statistics.median(r["ms"]))
    m[f"{op}.p95"].append(statistics.quantiles(r["ms"], n=20, method="inclusive")[-1])
    m[f"{op}.mem"].append(r["mem"])


def _op(checkout: Path, op: str, **args: object) -> dict:
    """Run one op in a fresh interpreter; its result plus peak tree memory in MiB."""
    proc = subprocess.Popen(
        [sys.executable, "-I", "-S", str(MEASURE)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None
    tmp = data_dir() / "work" / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    request = {"checkout": str(checkout), "tmp": str(tmp), "op": op, **args}
    proc.stdin.write(json.dumps(request, default=str))
    proc.stdin.close()
    peak = 0
    while proc.poll() is None:
        peak = max(peak, _tree_anon_kib(proc.pid))
        time.sleep(SAMPLE_S)
    result = proc.stdout.read()
    if proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode, f"measure {op}")
    os.sync()
    return {**json.loads(result), "mem": peak / 1024}


def _tree_anon_kib(pid: int) -> int:
    total, stack = 0, [pid]
    while stack:
        p = stack.pop()
        try:
            for line in Path(f"/proc/{p}/status").read_text().splitlines():
                if line.startswith("RssAnon:"):
                    total += int(line.split()[1])
            for task in os.listdir(f"/proc/{p}/task"):
                stack += map(
                    int, Path(f"/proc/{p}/task/{task}/children").read_text().split()
                )
        except FileNotFoundError:
            continue
    return total


@contextmanager
def _linked(src: Path, tier: Tier) -> Iterator[None]:
    """The tier's probe hard-linked into its directory for the duration."""
    assert tier.probe is not None
    dst = src / tier.probe.path
    made = [p for p in dst.parents if p.is_relative_to(src) and not p.exists()]
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.link(data_dir() / "corpora" / tier.corpus / tier.probe.path, dst)
    try:
        yield
    finally:
        dst.unlink()
        for p in made:
            p.rmdir()


def _synced(tier: Tier, files: int, r: dict) -> dict:
    if r["files"] != files:
        raise ValueError(f"{tier.key}: {r['files']} paths synced, expected {files}")
    return r


def _check_restore(restored: Path, tier: Tier) -> None:
    files = [p for p in restored.rglob("*") if p.is_file()]
    got = (len(files), sum(p.stat().st_size for p in files))
    if got != (len(tier.files), tier.bytes):
        raise ValueError(
            f"{tier.key}: restored {got}, expected {len(tier.files), tier.bytes}"
        )


def _check_space(tier: Tier) -> None:
    need = SPACE_FACTOR * tier.bytes + FREE_MARGIN
    free = shutil.disk_usage(data_dir()).free
    if free < need:
        raise OSError(
            f"{tier.key} needs {need / 1e9:.1f} GB free, {free / 1e9:.1f} GB left"
        )


def _wait_idle(force: bool) -> float:
    """Busy cores outside this run; waits until below MAX_BUSY_CORES unless forced."""
    while (busy := _busy_cores()) > MAX_BUSY_CORES and not force:
        print(f"waiting: {busy:.1f} cores busy", flush=True)
    return busy


def _busy_cores() -> float:
    def sample() -> tuple[int, int]:
        fields = [int(x) for x in Path("/proc/stat").read_text().split()[1:9]]
        idle = fields[3] + fields[4]
        return sum(fields) - idle, sum(fields)

    busy0, total0 = sample()
    time.sleep(IDLE_WINDOW_S)
    busy1, total1 = sample()
    return (busy1 - busy0) / (total1 - total0) * (os.cpu_count() or 1)


@contextmanager
def _checkout(ref: str | None, sha: str) -> Iterator[Path]:
    if ref is None:
        yield REPO
        return
    path = data_dir() / "worktrees" / sha
    shutil.rmtree(path, ignore_errors=True)
    git("worktree", "prune")
    git("worktree", "add", "--detach", str(path), sha)
    try:
        yield path
    finally:
        git("worktree", "remove", "--force", str(path))


def _fingerprint() -> dict:
    cpu = next(
        line.split(":", 1)[1].strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines()
        if line.startswith("model name")
    )
    mem_kib = int(Path("/proc/meminfo").read_text().split()[1])
    zstd = subprocess.run(["zstd", "--version"], capture_output=True, text=True)
    return {
        "host": socket.gethostname(),
        "cpu": cpu,
        "cores": os.cpu_count(),
        "ram_gib": round(mem_kib / MIB),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "zstd": zstd.stdout.strip(),
    }


def git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO), *args], check=True, capture_output=True, text=True
    ).stdout.strip()
