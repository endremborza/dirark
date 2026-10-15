"""Low-level storage primitives: checksums, tar I/O, and database access."""

import hashlib
import shutil
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterable
from pathlib import Path

_HASH_CHUNK = 1 << 20

MAX_TAR_MB = 256
DB_NAME = "index.sqlite"
SEP = "-"
TAR_PREFIX = "data" + SEP
TAR_EXT = ".tar.zst"
ARK_DIR_EXT = ".ark.d"


def b2sum(path: Path) -> str:
    """Compute the BLAKE2b-512 checksum of a file.

    Matches the digest of the `b2sum` coreutil (the default BLAKE2b-512), so
    arks built by older versions stay valid, while avoiding a subprocess per
    file.
    """
    h = hashlib.blake2b()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def b2sum_bytes(data: bytes) -> str:
    """Compute the BLAKE2b-512 checksum of in-memory bytes.

    Identical to `b2sum` over a file with the same content, keeping
    content-addressing consistent across the codebase.
    """
    return hashlib.blake2b(data).hexdigest()


def open_db(path: Path) -> sqlite3.Connection:
    """Open or create an ark SQLite database, ensuring the schema exists.

    Tables:
        files(path TEXT PK, checksum TEXT)       -- relative path → checksum
        objects(checksum TEXT PK, tar_name TEXT) -- checksum → archive name
    """
    db = sqlite3.connect(path)
    db.execute(
        "CREATE TABLE IF NOT EXISTS files("
        "path TEXT PRIMARY KEY, checksum TEXT NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS objects("
        "checksum TEXT PRIMARY KEY, tar_name TEXT NOT NULL)"
    )
    return db


def list_tars(ark_dir: Path) -> list[Path]:
    """Return sorted list of tar archives in an ark directory."""
    return sorted(ark_dir.glob(f"{TAR_PREFIX}*{TAR_EXT}"))


def tar_size_mb(path: Path) -> float:
    """Return file size in megabytes."""
    return path.stat().st_size / (1024 * 1024)


def next_tar_path(ark_dir: Path, min_idx: int = 0) -> Path:
    """Return path for the next tar archive in ark_dir.

    Index is max(last_existing + 1, min_idx), guaranteeing no name collision.
    """
    tars = list_tars(ark_dir)
    last_idx = tar_index(tars[-1].name) if tars else 0
    idx = max(last_idx + 1, min_idx)
    return ark_dir / f"{TAR_PREFIX}{idx:05d}{TAR_EXT}"


def tar_index(tar_name: str) -> int:
    """Return the sequence number encoded in a tar archive's file name."""
    return int(tar_name.removesuffix(TAR_EXT).split(SEP)[-1])


def extract_tar_zst(src: Path, dest: Path) -> None:
    """Extract a zstd-compressed tar archive into dest."""
    subprocess.run(
        ["tar", "-I", "zstd", "-xf", str(src), "-C", str(dest)],
        check=True,
    )


def read_object_from_tar(tar: Path, checksum: str) -> bytes:
    """Extract a single object by checksum from a tar and return its bytes.

    Streams the member to stdout (-O) to avoid a temp-dir round trip. The zstd
    stream is still fully decompressed to locate the member, which is inherent
    to the on-disk format.
    """
    res = subprocess.run(
        ["tar", "-I", "zstd", "-xOf", str(tar), f"./objects/{checksum}"],
        check=True,
        stdout=subprocess.PIPE,
    )
    return res.stdout


def extract_objects(
    tar_path: Path, checksums: Iterable[str], dest_dir: Path
) -> set[str]:
    """Extract the named objects from a tar.zst into dest_dir, keyed by checksum.

    Each requested checksum present in the tar is copied to
    ``dest_dir/<checksum>``. Returns the set of checksums actually found; missing
    objects are skipped, leaving the caller to decide how to report them.
    """
    found: set[str] = set()
    with tempfile.TemporaryDirectory() as tmp:
        extract_tar_zst(tar_path, Path(tmp))
        obj_dir = Path(tmp) / "objects"
        for cs in checksums:
            obj = obj_dir / cs
            if obj.exists():
                shutil.copy2(obj, dest_dir / cs)
                found.add(cs)
    return found


def create_tar_zst(src_dir: Path, out: Path) -> None:
    """Create a zstd-compressed tar of src_dir contents at out."""
    subprocess.run(
        ["tar", "-C", str(src_dir), "-I", "zstd", "-cf", str(out), "."],
        check=True,
    )


def ensure_clean_outdir(ark_dir: Path) -> None:
    """Create ark_dir if needed and raise RuntimeError on unexpected files."""
    ark_dir.mkdir(parents=True, exist_ok=True)
    allowed = {DB_NAME} | {p.name for p in ark_dir.glob(f"{TAR_PREFIX}*{TAR_EXT}")}
    for p in ark_dir.iterdir():
        if p.name not in allowed:
            raise RuntimeError(f"Unexpected file in archive dir: {p}")


def write_objects_to_tars(
    ark_dir: Path,
    objects: dict[str, Path],
    min_tar_idx: int = 0,
    max_tar_mb: int = MAX_TAR_MB,
) -> dict[str, str]:
    """Append objects (checksum → source path) to tar archives in ark_dir.

    Objects are packed in batches of at most max_tar_mb of source bytes, one
    tar per batch, so tars stay bounded no matter how many objects one call
    carries (a single object over the cap still gets its own tar). The first
    batch coalesces into the last existing tar only when the tar plus the
    batch's source bytes stay under the cap — small commits merge, full
    batches always roll a fresh tar. min_tar_idx can be used to avoid index
    collisions when merging arks.

    Staging lives beside ark_dir — same filesystem as the output, never the
    OS temp dir, which large batches can exceed. Leftover staging dirs from
    a crashed run are swept on entry.

    Returns checksum → tar filename for every object written.
    """
    staging_prefix = f".{ark_dir.name}-staging-"
    for leftover in ark_dir.parent.glob(f"{staging_prefix}*"):
        shutil.rmtree(leftover)

    max_bytes = max_tar_mb * 1024 * 1024
    batches: list[dict[str, Path]] = [{}]
    acc = 0
    for checksum, src in objects.items():
        size = src.stat().st_size
        if batches[-1] and acc + size > max_bytes:
            batches.append({})
            acc = 0
        batches[-1][checksum] = src
        acc += size

    tars = list_tars(ark_dir)
    last_tar = tars[-1] if tars else None

    written: dict[str, str] = {}
    for i, batch in enumerate(batches):
        if not batch:
            continue
        batch_mb = sum(src.stat().st_size for src in batch.values()) / (1024 * 1024)
        if (
            i == 0
            and last_tar is not None
            and tar_size_mb(last_tar) + batch_mb <= max_tar_mb
        ):
            tar_path = last_tar
        else:
            tar_path = next_tar_path(ark_dir, min_idx=min_tar_idx)
        with tempfile.TemporaryDirectory(
            dir=ark_dir.parent, prefix=staging_prefix
        ) as tmp:
            tmp_dir = Path(tmp)
            obj_dir = tmp_dir / "objects"
            obj_dir.mkdir(parents=True)

            if tar_path.exists():
                extract_tar_zst(tar_path, tmp_dir)

            for checksum, src in batch.items():
                dest = obj_dir / checksum
                if not dest.exists():
                    shutil.copy2(src, dest)

            create_tar_zst(tmp_dir, tar_path)
        for checksum in batch:
            written[checksum] = tar_path.name

    return written
