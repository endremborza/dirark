"""One measured operation, run in a fresh interpreter against a dirark checkout.

Run as `python -I -S measure.py` with a JSON object on stdin naming the
checkout, a temp dir, the op and its paths. The checkout is imported ahead of
anything installed, its temporary files go to the temp dir, the page cache the
op must not benefit from is dropped first (`posix_fadvise` needs no
privileges), and one JSON object goes to stdout: the wall and CPU seconds of
the op itself, plus op-specific numbers. Only the stdlib and the checkout are
importable, so any commit can be measured.
"""

import json
import os
import resource
import sqlite3
import sys
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from types import ModuleType


def evict(*roots: Path) -> None:
    os.sync()
    for root in roots:
        for path in [root] if root.is_file() else root.rglob("*"):
            if path.is_file():
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(fd)


def du(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def _cpu() -> float:
    own = resource.getrusage(resource.RUSAGE_SELF)
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + kids.ru_utime + kids.ru_stime


def _timed(fn: Callable[[], object]) -> dict:
    cpu, t0 = _cpu(), time.perf_counter()
    fn()
    return {"wall": time.perf_counter() - t0, "cpu": _cpu() - cpu}


def archive(d: ModuleType, a: dict) -> dict:
    src, ark = Path(a["src"]), Path(a["ark"])
    evict(src)
    return _timed(lambda: d.archive_dir(src, ark_out=ark))


def restore(d: ModuleType, a: dict) -> dict:
    ark = Path(a["ark"])
    evict(ark)
    return _timed(lambda: d.restore_ark(ark, Path(a["out"])))


def read(d: ModuleType, a: dict) -> dict:
    ark, ms = Path(a["ark"]), []
    for path, size in a["files"]:
        evict(ark)
        t0 = time.perf_counter()
        with d.ArkReader(ark) as reader:
            got = len(reader.read_file(path))
        ms.append((time.perf_counter() - t0) * 1e3)
        if got != size:
            raise ValueError(f"{path}: read {got} bytes, expected {size}")
    return {"ms": ms}


def push(d: ModuleType, a: dict) -> dict:
    ark, remote = Path(a["ark"]), Path(a["remote"])
    evict(ark)
    return _timed(lambda: d.push_ark(ark, str(remote))) | _indexed(d, remote)


def pull(d: ModuleType, a: dict) -> dict:
    remote, ark = Path(a["remote"]), Path(a["ark"])
    evict(remote)
    return _timed(lambda: d.pull_ark(str(remote), ark)) | _indexed(d, ark)


def _indexed(d: ModuleType, ark: Path) -> dict:
    with closing(sqlite3.connect(ark / d.storage.DB_NAME)) as db:
        return {"files": db.execute("SELECT count(*) FROM files").fetchone()[0]}


def blob_write(d: ModuleType, a: dict) -> dict:
    files = [Path(f) for f in a["files"]]
    evict(*files)

    def write() -> None:
        store = d.BlobStore(a["store"])
        for f in files:
            store.write(f.read_bytes())
        store.commit()

    out = _timed(write)
    out["packed"] = du(d.BlobStore(a["store"]).ark)
    return out


def blob_read(d: ModuleType, a: dict) -> dict:
    store, ms = Path(a["store"]), []
    for path, size in a["files"]:
        checksum = d.blob_checksum(Path(path).read_bytes())
        evict(store)
        t0 = time.perf_counter()
        got = len(d.BlobStore(store).read(checksum))
        ms.append((time.perf_counter() - t0) * 1e3)
        if got != size:
            raise ValueError(f"{path}: read {got} bytes, expected {size}")
    return {"ms": ms}


OPS = {
    "archive": archive,
    "restore": restore,
    "read": read,
    "push": push,
    "pull": pull,
    "blob_write": blob_write,
    "blob_read": blob_read,
}


def main() -> None:
    args = json.load(sys.stdin)
    tempfile.tempdir = args["tmp"]
    sys.path.insert(0, args["checkout"])
    import dirark

    if not Path(dirark.__file__).is_relative_to(args["checkout"]):
        raise ImportError(f"dirark from {dirark.__file__}, not {args['checkout']}")
    json.dump(OPS[args["op"]](dirark, args), sys.stdout)


if __name__ == "__main__":
    main()
