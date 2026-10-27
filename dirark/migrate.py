"""One-way migration of format-1 arks to the current index and frame format.

Format 1 indexed hex checksums against tar names and wrote each tar as one zstd
stream. Migration re-packs every indexed object into tars numbered past every
format-1 tar, then swaps the new index in and deletes the old tars, so an
interruption before the swap leaves a working format-1 ark.
"""

import os
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from collections import defaultdict
from contextlib import closing
from pathlib import Path

from .storage import (
    DB_NAME,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    SCHEMA_VERSION,
    Packer,
    digest_file,
    list_tars,
    open_db,
    tar_index,
)
from .sync import rsync


class MigrationError(RuntimeError):
    """An ark whose index names content its tars do not hold intact."""


def migrate_ark(
    ark: str,
    *,
    max_tar_mb: int = MAX_TAR_MB,
    max_frame_mb: int = MAX_FRAME_MB,
    work: Path | None = None,
) -> None:
    """Re-pack a format-1 ark in place; a current-format ark is left alone.

    ark is a local path or an rsync remote (user@host:/path). Each tar is
    unpacked, and the new index built, in a temporary dir under work (beside the
    ark by default: point it at a local disk for an ark on a network mount); the
    new tars are written beside the ark. A remote is fetched into a temporary
    dir under work (the working directory by default), migrated there, and
    uploaded back. Every object is re-hashed against its checksum: a missing
    tar, a missing or corrupt object, or a path without content raises
    MigrationError and leaves the ark as it was. A BlobStore's ark takes
    max_frame_mb=dirark.blob.FRAME_MB.
    """
    host, colon, _ = ark.partition(":")
    if colon and "/" not in host:
        _migrate_remote(ark, max_tar_mb, max_frame_mb, work or Path.cwd())
    else:
        _migrate_local(Path(ark), max_tar_mb, max_frame_mb, work)


def _migrate_local(
    ark: Path, max_tar_mb: int, max_frame_mb: int, work: Path | None
) -> None:
    with closing(sqlite3.connect(ark / DB_NAME)) as old:
        if _version(old) == SCHEMA_VERSION:
            return
        objects = dict(old.execute("SELECT checksum, tar_name FROM objects"))
        files = old.execute("SELECT path, checksum FROM files").fetchall()
    by_tar = _check_index(ark, objects, files)
    old_tars = list_tars(ark)
    first = 1 + max((tar_index(t.name) for t in old_tars), default=0)
    stamps: dict[str, tuple[int, int]] = {}
    prefix = f".{ark.name}-migrate-"
    with (
        tempfile.TemporaryDirectory(dir=ark.parent, prefix=prefix) as tars,
        tempfile.TemporaryDirectory(dir=work or ark.parent, prefix=prefix) as tmp,
    ):
        new, unpacked, index = Path(tars), Path(tmp) / "unpacked", Path(tmp) / DB_NAME
        with (
            closing(open_db(index)) as db,
            Packer(
                new,
                db,
                max_tar_mb=max_tar_mb,
                max_frame_mb=max_frame_mb,
                first_tar=first,
            ) as pack,
        ):
            for name, wanted in sorted(by_tar.items()):
                unpacked.mkdir()
                subprocess.run(
                    ["tar", "-I", "zstd", "-xf", str(ark / name), "-C", str(unpacked)],
                    check=True,
                )
                for p in sorted((unpacked / "objects").iterdir()):
                    if p.name not in wanted:
                        continue
                    if digest_file(p).hex() != p.name:
                        raise MigrationError(f"{name}: {p.name} is corrupt")
                    st = p.stat()
                    stamps[p.name] = (st.st_mtime_ns, stat.S_IMODE(st.st_mode))
                    pack.add(bytes.fromhex(p.name), p, st.st_size)
                if lost := len(wanted - stamps.keys()):
                    raise MigrationError(f"{name} lacks {lost} of its indexed objects")
                pack.commit()
                shutil.rmtree(unpacked)
            db.executemany(
                "INSERT INTO files VALUES (?, ?, ?, ?)",
                ((path, bytes.fromhex(cs), *stamps[cs]) for path, cs in files),
            )
            db.commit()
        for t in list_tars(new):
            os.replace(t, ark / t.name)
        staged = ark / f".{DB_NAME}.migrated"
        shutil.move(index, staged)
        os.replace(staged, ark / DB_NAME)
    for t in old_tars:
        t.unlink()


def _check_index(
    ark: Path, objects: dict[str, str], files: list[tuple[str, str]]
) -> dict[str, set[str]]:
    """The indexed objects of each tar, once every named tar exists and every
    path's content is indexed."""
    by_tar: dict[str, set[str]] = defaultdict(set)
    for checksum, name in objects.items():
        by_tar[name].add(checksum)
    if absent := sorted(n for n in by_tar if not (ark / n).exists()):
        raise MigrationError(f"{ark}: indexed tars missing: {', '.join(absent)}")
    if orphans := sum(cs not in objects for _, cs in files):
        raise MigrationError(f"{ark}: {orphans} paths point at no indexed object")
    return by_tar


def _migrate_remote(
    remote: str, max_tar_mb: int, max_frame_mb: int, work: Path
) -> None:
    with tempfile.TemporaryDirectory(dir=work, prefix=".dirark-migrate-") as tmp:
        local = Path(tmp) / "ark"
        rsync("--mkpath", f"{remote}/{DB_NAME}", f"{local}/")
        with closing(sqlite3.connect(local / DB_NAME)) as db:
            if _version(db) == SCHEMA_VERSION:
                return
        rsync(f"{remote}/", f"{local}/")
        _migrate_local(local, max_tar_mb, max_frame_mb, None)
        rsync("--exclude", DB_NAME, f"{local}/", f"{remote}/")
        # -c: the new index can match the old one's size and mtime second
        rsync("-c", f"{local}/{DB_NAME}", f"{remote}/")
        rsync("--delete", f"{local}/", f"{remote}/")


def _version(db: sqlite3.Connection) -> int:
    return db.execute("PRAGMA user_version").fetchone()[0]
