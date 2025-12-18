import shutil
import sqlite3
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

MAX_TAR_MB = 256
DB_NAME = "index.sqlite"
SEP = "-"
TAR_PREFIX = "data" + SEP
TAR_EXT = ".tar.zst"


def b2sum(path: Path) -> str:
    res = subprocess.run(
        ["b2sum", str(path)],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return res.stdout.split()[0]


def ensure_clean_outdir(out: Path):
    out.mkdir(parents=True, exist_ok=True)
    allowed = {DB_NAME}
    allowed |= {p.name for p in out.glob(f"{TAR_PREFIX}*{TAR_EXT}")}
    for p in out.iterdir():
        if p.name not in allowed:
            raise RuntimeError(f"Unexpected file in archive dir: {p}")


def open_db(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS files(
            path TEXT PRIMARY KEY,
            checksum TEXT NOT NULL
        )
    """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS objects(
            checksum TEXT PRIMARY KEY,
            tar_name TEXT NOT NULL
        )
    """
    )
    return db


def list_tars(out: Path):
    return sorted(out.glob(f"{TAR_PREFIX}*{TAR_EXT}"))


def tar_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def next_tar_path(out: Path) -> Path:
    tars = list_tars(out)
    if not tars:
        return out / f"{TAR_PREFIX}00001{TAR_EXT}"
    idx = int(tars[-1].stem.split(SEP)[-1]) + 1
    return out / f"{TAR_PREFIX}{idx:05d}{TAR_EXT}"


def extract_tar_zst(src: Path, dest: Path):
    subprocess.run(
        ["tar", "-I", "zstd", "-xf", str(src), "-C", str(dest)],
        check=True,
    )


def create_tar_zst(src_dir: Path, out: Path):
    subprocess.run(
        ["tar", "-C", str(src_dir), "-I", "zstd", "-cf", str(out), "."],
        check=True,
    )


def merge_dir(ark_dir: Path, ark_dir_out: Path):
    ensure_clean_outdir(ark_dir_out)
    db = open_db(ark_dir_out / DB_NAME)
    cur = db.cursor()

    new_objects = {}
    new_files = []

    for path in ark_dir.rglob("*"):
        if not path.is_file():
            continue

        rel = path.relative_to(ark_dir).as_posix()
        checksum = b2sum(path)

        cur.execute("SELECT 1 FROM files WHERE path=?", (rel,))
        if cur.fetchone():
            continue

        cur.execute(
            "SELECT tar_name FROM objects WHERE checksum=?",
            (checksum,),
        )
        row = cur.fetchone()

        if row is None:
            new_objects.setdefault(checksum, path)

        new_files.append((rel, checksum))

    if not new_objects:
        return

    tars = list_tars(ark_dir_out)
    if tars and tar_size_mb(tars[-1]) < MAX_TAR_MB:
        target_tar = tars[-1]
    else:
        target_tar = next_tar_path(ark_dir_out)

    tar_batches = defaultdict(list)
    for checksum, src in new_objects.items():
        tar_batches[target_tar].append((checksum, src))

    for tar_path, objects in tar_batches.items():
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            obj_dir = tmp / "objects"
            obj_dir.mkdir(parents=True)

            if tar_path.exists():
                extract_tar_zst(tar_path, tmp)

            for checksum, src in objects:
                target = obj_dir / checksum
                if not target.exists():
                    shutil.copy2(src, target)

            create_tar_zst(tmp, tar_path)

            for checksum, _ in objects:
                cur.execute(
                    "INSERT INTO objects VALUES (?, ?)",
                    (checksum, tar_path.name),
                )

    for rel, checksum in new_files:
        cur.execute(
            "INSERT INTO files VALUES (?, ?)",
            (rel, checksum),
        )

    db.commit()
    db.close()
