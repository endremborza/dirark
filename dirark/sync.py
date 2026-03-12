"""Sync operations between local and remote dirark archives."""

import shutil
import subprocess
import tempfile
from pathlib import Path

from .core import archive_dir
from .storage import (
    DB_NAME,
    extract_tar_zst,
    open_db,
    write_objects_to_tar,
)


def push_ark(local_ark: Path, remote: str) -> None:
    """Push a local ark to a remote location via rsync.

    Uses checksum comparison (not just mtime+size) to ensure all changes are
    transferred, even when modifications happen within the same second.
    remote may be a local path string or an SSH target (user@host:/path).
    """
    subprocess.run(
        ["rsync", "-avzc", f"{local_ark}/", remote],
        check=True,
    )


def pull_ark(remote: str, local_ark: Path) -> None:
    """Pull a remote ark to a local path via rsync.

    remote may be a local path string or an SSH target (user@host:/path).
    """
    local_ark.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["rsync", "-avz", f"{remote}/", str(local_ark)],
        check=True,
    )


def merge_arks(src_ark: Path, dst_ark: Path) -> None:
    """Merge all objects and file mappings from src_ark into dst_ark.

    Objects already present in dst by checksum are skipped (deduplication).
    File path mappings are added with INSERT OR IGNORE, so existing paths
    in dst take precedence.
    """
    dst_ark.mkdir(parents=True, exist_ok=True)
    src_db = open_db(src_ark / DB_NAME)
    dst_db = open_db(dst_ark / DB_NAME)
    src_cur, dst_cur = src_db.cursor(), dst_db.cursor()

    src_cur.execute("SELECT checksum, tar_name FROM objects")
    src_objects = dict(src_cur.fetchall())

    dst_cur.execute("SELECT checksum FROM objects")
    dst_known = {row[0] for row in dst_cur.fetchall()}

    missing = {cs: tar for cs, tar in src_objects.items() if cs not in dst_known}

    if missing:
        by_tar: dict[str, list[str]] = {}
        for cs, tar_name in missing.items():
            by_tar.setdefault(tar_name, []).append(cs)

        with tempfile.TemporaryDirectory() as tmp:
            staging_obj = Path(tmp) / "objects"
            staging_obj.mkdir()

            for tar_name, checksums in by_tar.items():
                src_tar = src_ark / tar_name
                if not src_tar.exists():
                    continue
                with tempfile.TemporaryDirectory() as xtmp:
                    extract_tar_zst(src_tar, Path(xtmp))
                    for cs in checksums:
                        obj = Path(xtmp) / "objects" / cs
                        if obj.exists():
                            shutil.copy2(obj, staging_obj / cs)

            staged = {
                cs: staging_obj / cs for cs in missing if (staging_obj / cs).exists()
            }
            if staged:
                tar_name = write_objects_to_tar(dst_ark, staged)
                for cs in staged:
                    dst_cur.execute(
                        "INSERT OR IGNORE INTO objects VALUES (?, ?)",
                        (cs, tar_name),
                    )

    src_cur.execute("SELECT path, checksum FROM files")
    for rel, checksum in src_cur.fetchall():
        dst_cur.execute("INSERT OR IGNORE INTO files VALUES (?, ?)", (rel, checksum))

    dst_db.commit()
    src_db.close()
    dst_db.close()


def add_dir_to_remote_ark(src_dir: Path, remote_ark: str) -> None:
    """Archive src_dir and merge its contents into a remote ark.

    Pulls the remote ark locally, archives src_dir into it, then pushes the
    updated ark back. Supports SSH remotes (user@host:/path) via rsync.

    The remote ark must already exist (at minimum as an empty directory).
    For a first push, use archive_dir followed by push_ark instead.
    """
    with tempfile.TemporaryDirectory() as tmp:
        local_ark = Path(tmp) / "remote_ark"
        pull_ark(remote_ark, local_ark)
        archive_dir(src_dir, ark_out=local_ark)
        push_ark(local_ark, remote_ark)
