"""Low-level storage primitives: checksums, tar I/O, and database access."""

import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

MAX_TAR_MB = 256
DB_NAME = "index.sqlite"
SEP = "-"
TAR_PREFIX = "data" + SEP
TAR_EXT = ".tar.zst"
ARK_DIR_EXT = ".ark.d"


def b2sum(path: Path) -> str:
    """Compute BLAKE2b checksum of a file using the system b2sum utility."""
    res = subprocess.run(
        ["b2sum", str(path)],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return res.stdout.split()[0]


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
    last_idx = int(tars[-1].name.removesuffix(TAR_EXT).split(SEP)[-1]) if tars else 0
    idx = max(last_idx + 1, min_idx)
    return ark_dir / f"{TAR_PREFIX}{idx:05d}{TAR_EXT}"


def extract_tar_zst(src: Path, dest: Path) -> None:
    """Extract a zstd-compressed tar archive into dest."""
    subprocess.run(
        ["tar", "-I", "zstd", "-xf", str(src), "-C", str(dest)],
        check=True,
    )


def extract_object_from_tar(tar: Path, checksum: str, dest: Path) -> None:
    """Extract a single object by checksum from a tar archive into dest."""
    subprocess.run(
        [
            "tar",
            "-I",
            "zstd",
            "-xf",
            str(tar),
            "-C",
            str(dest),
            f"./objects/{checksum}",
        ],
        check=True,
    )


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


def write_objects_to_tar(
    ark_dir: Path,
    objects: dict[str, Path],
    min_tar_idx: int = 0,
) -> str:
    """Append objects (checksum → source path) to a tar archive in ark_dir.

    Reuses the last tar if it is under MAX_TAR_MB, otherwise creates a new
    one. min_tar_idx can be used to avoid index collisions when merging arks.

    Returns the tar filename written to.
    """
    tars = list_tars(ark_dir)
    if tars and tar_size_mb(tars[-1]) < MAX_TAR_MB:
        tar_path = tars[-1]
    else:
        tar_path = next_tar_path(ark_dir, min_idx=min_tar_idx)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        obj_dir = tmp_dir / "objects"
        obj_dir.mkdir(parents=True)

        if tar_path.exists():
            extract_tar_zst(tar_path, tmp_dir)

        for checksum, src in objects.items():
            dest = obj_dir / checksum
            if not dest.exists():
                shutil.copy2(src, dest)

        create_tar_zst(tmp_dir, tar_path)

    return tar_path.name
