"""One-way migration of format-1 arks to the current index and frame format.

Format 1 indexed hex checksums against tar names and wrote each tar as one zstd
stream. Migration re-packs every indexed object into tars numbered past every
format-1 tar, then swaps the new index in and deletes the old tars, so an
interruption before the swap leaves a working format-1 ark.
"""

import logging
import os
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

from .storage import (
    DB_NAME,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    SCHEMA_VERSION,
    Packer,
    list_tars,
    open_db,
    tar_index,
)
from .sync import rsync

logger = logging.getLogger(__name__)


def migrate_ark(
    ark: str, *, max_tar_mb: int = MAX_TAR_MB, max_frame_mb: int = MAX_FRAME_MB
) -> None:
    """Re-pack a format-1 ark in place; a current-format ark is left alone.

    ark is a local path or an rsync remote (user@host:/path). A remote is fetched
    into a temporary dir under the working directory, migrated there, and
    uploaded back. A BlobStore's ark takes max_frame_mb=dirark.blob.FRAME_MB.
    """
    host, colon, _ = ark.partition(":")
    if colon and "/" not in host:
        _migrate_remote(ark, max_tar_mb, max_frame_mb)
    else:
        _migrate_local(Path(ark), max_tar_mb, max_frame_mb)


def _migrate_local(ark: Path, max_tar_mb: int, max_frame_mb: int) -> None:
    with closing(sqlite3.connect(ark / DB_NAME)) as old:
        if _version(old) == SCHEMA_VERSION:
            return
        objects = dict(old.execute("SELECT checksum, tar_name FROM objects"))
        files = old.execute("SELECT path, checksum FROM files").fetchall()
    old_tars = list_tars(ark)
    first = 1 + max((tar_index(t.name) for t in old_tars), default=0)
    stamps: dict[str, tuple[int, int]] = {}
    with tempfile.TemporaryDirectory(
        dir=ark.parent, prefix=f".{ark.name}-migrate-"
    ) as tmp:
        new, unpacked = Path(tmp) / "ark", Path(tmp) / "unpacked"
        new.mkdir()
        with (
            closing(open_db(new / DB_NAME)) as db,
            Packer(
                new,
                db,
                max_tar_mb=max_tar_mb,
                max_frame_mb=max_frame_mb,
                first_tar=first,
            ) as pack,
        ):
            for name in sorted(set(objects.values())):
                if not (ark / name).exists():
                    logger.warning("%s not found; its objects are dropped.", name)
                    continue
                unpacked.mkdir()
                subprocess.run(
                    ["tar", "-I", "zstd", "-xf", str(ark / name), "-C", str(unpacked)],
                    check=True,
                )
                for p in sorted((unpacked / "objects").iterdir()):
                    if objects.get(p.name) == name and p.name not in stamps:
                        st = p.stat()
                        stamps[p.name] = (st.st_mtime_ns, stat.S_IMODE(st.st_mode))
                        pack.add(bytes.fromhex(p.name), p, st.st_size)
                pack.commit()
                shutil.rmtree(unpacked)
            kept = [(path, cs) for path, cs in files if cs in stamps]
            if len(kept) < len(files):
                logger.warning(
                    "%d paths without content dropped.", len(files) - len(kept)
                )
            db.executemany(
                "INSERT INTO files VALUES (?, ?, ?, ?)",
                ((path, bytes.fromhex(cs), *stamps[cs]) for path, cs in kept),
            )
            db.commit()
        for t in list_tars(new):
            os.replace(t, ark / t.name)
        os.replace(new / DB_NAME, ark / DB_NAME)
    for t in old_tars:
        t.unlink()


def _migrate_remote(remote: str, max_tar_mb: int, max_frame_mb: int) -> None:
    with tempfile.TemporaryDirectory(dir=Path.cwd(), prefix=".dirark-migrate-") as tmp:
        local = Path(tmp) / "ark"
        rsync("--mkpath", f"{remote}/{DB_NAME}", f"{local}/")
        with closing(sqlite3.connect(local / DB_NAME)) as db:
            if _version(db) == SCHEMA_VERSION:
                return
        rsync(f"{remote}/", f"{local}/")
        _migrate_local(local, max_tar_mb, max_frame_mb)
        rsync("--exclude", DB_NAME, f"{local}/", f"{remote}/")
        # -c: the new index can match the old one's size and mtime second
        rsync("-c", f"{local}/{DB_NAME}", f"{remote}/")
        rsync("--delete", f"{local}/", f"{remote}/")


def _version(db: sqlite3.Connection) -> int:
    return db.execute("PRAGMA user_version").fetchone()[0]
