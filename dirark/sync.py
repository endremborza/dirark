"""Sync operations between local and remote dirark archives."""

import sqlite3
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

from .core import archive_dir
from .storage import (
    DB_NAME,
    MAX_TAR_MB,
    extract_objects,
    open_db,
    tar_index,
    write_objects_to_tars,
)


def push_ark(local_ark: Path, remote: str, *, max_tar_mb: int = MAX_TAR_MB) -> None:
    """Merge local_ark into the ark at remote via rsync, creating it if absent.

    Index-first: only the remote index is fetched; objects it lacks are packed
    into fresh tars numbered past every tar it references, and those tars plus
    the merged index are uploaded. Remote tars are never rewritten, so arks that
    diverged (e.g. one per machine) can all push to one remote. Concurrent
    pushes are not safe: both can claim the same fresh tar name.
    remote may be a local path string or an SSH target (user@host:/path).
    """
    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / "ark"
        staged.mkdir()
        _rsync("--mkpath", f"{staged}/", f"{remote}/")
        _rsync("--ignore-missing-args", f"{remote}/{DB_NAME}", f"{staged}/")
        merge_arks(local_ark, staged, max_tar_mb=max_tar_mb)
        # -c: the merged index can match the remote's size and mtime second
        _rsync("-c", f"{staged}/", f"{remote}/")


def pull_ark(remote: str, local_ark: Path, *, max_tar_mb: int = MAX_TAR_MB) -> None:
    """Merge the ark at remote into local_ark via rsync; local-only objects stay.

    Index-first: fetches the remote index, then only the tars holding objects
    local_ark lacks. A remote without an index is a no-op.
    remote may be a local path string or an SSH target (user@host:/path).
    """
    with tempfile.TemporaryDirectory() as tmp:
        fetched = Path(tmp) / "ark"
        fetched.mkdir()
        _rsync("--ignore-missing-args", f"{remote}/{DB_NAME}", f"{fetched}/")
        if not (fetched / DB_NAME).exists():
            return
        local_ark.mkdir(parents=True, exist_ok=True)
        with (
            closing(open_db(fetched / DB_NAME)) as src_db,
            closing(open_db(local_ark / DB_NAME)) as dst_db,
        ):
            tars = sorted(set(_missing_objects(src_db, dst_db).values()))
        if tars:
            _rsync("--files-from=-", f"{remote}/", f"{fetched}/", stdin="\n".join(tars))
        merge_arks(fetched, local_ark, max_tar_mb=max_tar_mb)


def merge_arks(src_ark: Path, dst_ark: Path, *, max_tar_mb: int = MAX_TAR_MB) -> None:
    """Merge all objects and file mappings from src_ark into dst_ark.

    Objects already present in dst by checksum are skipped (deduplication).
    File path mappings are added with INSERT OR IGNORE, so existing paths
    in dst take precedence. New tars are numbered past every tar dst's index
    references, so dst may be an index-only copy of a remote ark, and hold at
    most max_tar_mb of source bytes each.
    """
    dst_ark.mkdir(parents=True, exist_ok=True)
    src_db = open_db(src_ark / DB_NAME)
    dst_db = open_db(dst_ark / DB_NAME)
    missing = _missing_objects(src_db, dst_db)

    if missing:
        by_tar: dict[str, list[str]] = {}
        for cs, tar_name in missing.items():
            by_tar.setdefault(tar_name, []).append(cs)

        with tempfile.TemporaryDirectory() as tmp:
            staging_obj = Path(tmp)

            staged_cs: set[str] = set()
            for tar_name, checksums in by_tar.items():
                src_tar = src_ark / tar_name
                if src_tar.exists():
                    staged_cs |= extract_objects(src_tar, checksums, staging_obj)

            if staged_cs:
                staged = {cs: staging_obj / cs for cs in staged_cs}
                indexed = dst_db.execute("SELECT DISTINCT tar_name FROM objects")
                min_idx = 1 + max((tar_index(n) for (n,) in indexed), default=0)
                written = write_objects_to_tars(
                    dst_ark, staged, min_tar_idx=min_idx, max_tar_mb=max_tar_mb
                )
                dst_db.executemany(
                    "INSERT OR IGNORE INTO objects VALUES (?, ?)", written.items()
                )

    files = src_db.execute("SELECT path, checksum FROM files")
    dst_db.executemany("INSERT OR IGNORE INTO files VALUES (?, ?)", files.fetchall())

    dst_db.commit()
    src_db.close()
    dst_db.close()


def add_dir_to_remote_ark(src_dir: Path, remote_ark: str) -> None:
    """Archive src_dir and merge its contents into a remote ark.

    Archives into a fresh local ark and pushes it, so only objects the remote
    lacks are uploaded and the remote is created if absent. Existing remote
    path mappings take precedence. Supports SSH remotes (user@host:/path).
    """
    with tempfile.TemporaryDirectory() as tmp:
        local_ark = Path(tmp) / "ark"
        archive_dir(src_dir, ark_out=local_ark)
        push_ark(local_ark, remote_ark)


def _missing_objects(
    src_db: sqlite3.Connection, dst_db: sqlite3.Connection
) -> dict[str, str]:
    """checksum → src tar name for every object src indexes and dst does not."""
    dst_known = {cs for (cs,) in dst_db.execute("SELECT checksum FROM objects")}
    rows = src_db.execute("SELECT checksum, tar_name FROM objects")
    return {cs: tar for cs, tar in rows if cs not in dst_known}


def _rsync(*args: str, stdin: str | None = None) -> None:
    subprocess.run(["rsync", "-az", *args], input=stdin, text=True, check=True)
