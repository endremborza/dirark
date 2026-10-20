"""Sync operations between local and remote dirark archives."""

import logging
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import closing, contextmanager
from itertools import groupby
from pathlib import Path

from .core import archive_dir
from .storage import (
    DB_NAME,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    Adopt,
    Packer,
    attach,
    open_db,
    read_span,
    tar_name,
)

logger = logging.getLogger(__name__)

_MISSING = (
    " FROM s.objects o CROSS JOIN s.frames f WHERE f.id = o.frame AND NOT EXISTS"
    " (SELECT 1 FROM main.objects m WHERE m.checksum = o.checksum)"
)


def push_ark(
    local_ark: Path,
    remote: str,
    *,
    max_tar_mb: int = MAX_TAR_MB,
    max_frame_mb: int = MAX_FRAME_MB,
) -> None:
    """Merge local_ark into the ark at remote via rsync, creating it if absent.

    Index-first: only the remote index is fetched; objects it lacks go into
    fresh tars numbered past every tar it references, and those tars plus the
    merged index are uploaded. A local tar whose objects the remote all lacks is
    uploaded as is. Remote tars are never rewritten, so arks that diverged (e.g.
    one per machine) can all push to one remote. Concurrent pushes are not safe:
    both can claim the same fresh tar name.
    remote may be a local path string or an SSH target (user@host:/path).
    """
    with _beside(local_ark, "push") as staged:
        rsync("--mkpath", f"{staged}/", f"{remote}/")
        rsync("--ignore-missing-args", f"{remote}/{DB_NAME}", f"{staged}/")
        _merge(local_ark, staged, "link", max_tar_mb, max_frame_mb, index_only=True)
        # -c: the merged index can match the remote's size and mtime second
        rsync("-c", f"{staged}/", f"{remote}/")


def pull_ark(
    remote: str,
    local_ark: Path,
    *,
    max_tar_mb: int = MAX_TAR_MB,
    max_frame_mb: int = MAX_FRAME_MB,
) -> None:
    """Merge the ark at remote into local_ark via rsync; local-only objects stay.

    Index-first: fetches the remote index, then only the tars holding objects
    local_ark lacks. A remote without an index is a no-op.
    remote may be a local path string or an SSH target (user@host:/path).
    """
    local_ark.parent.mkdir(parents=True, exist_ok=True)
    with _beside(local_ark, "pull") as fetched:
        rsync("--ignore-missing-args", f"{remote}/{DB_NAME}", f"{fetched}/")
        if not (fetched / DB_NAME).exists():
            return
        local_ark.mkdir(exist_ok=True)
        with closing(open_db(local_ark / DB_NAME)) as db:
            attach(db, fetched / DB_NAME)
            tars = [
                tar_name(t) for (t,) in db.execute(f"SELECT DISTINCT f.tar{_MISSING}")
            ]
        if tars:
            rsync("--files-from=-", f"{remote}/", f"{fetched}/", stdin="\n".join(tars))
        _merge(fetched, local_ark, "move", max_tar_mb, max_frame_mb)


def merge_arks(
    src_ark: Path,
    dst_ark: Path,
    *,
    max_tar_mb: int = MAX_TAR_MB,
    max_frame_mb: int = MAX_FRAME_MB,
) -> None:
    """Merge all objects and file mappings from src_ark into dst_ark.

    Objects already present in dst by checksum are skipped (deduplication).
    File path mappings are added with INSERT OR IGNORE, so existing paths in dst
    take precedence. Frames move without re-encoding: a tar whose objects dst
    all lacks is copied whole, a frame whose objects dst all lacks is copied
    byte for byte, and only the missing objects of other frames are re-packed.
    New tars are numbered past every tar dst's index references, so dst may be
    an index-only copy of a remote ark.
    """
    _merge(src_ark, dst_ark, "copy", max_tar_mb, max_frame_mb)


def add_dir_to_remote_ark(src_dir: Path, remote_ark: str) -> None:
    """Archive src_dir and merge its contents into a remote ark.

    Archives into a fresh ark beside src_dir and pushes it, so only objects the
    remote lacks are uploaded and the remote is created if absent. Existing
    remote path mappings take precedence. Supports SSH remotes (user@host:/path).
    """
    with _beside(src_dir, "add") as tmp:
        local_ark = tmp / "ark"
        archive_dir(src_dir, ark_out=local_ark)
        push_ark(local_ark, remote_ark)


def rsync(*args: str, stdin: str | None = None) -> None:
    subprocess.run(["rsync", "-az", *args], input=stdin, text=True, check=True)


def _merge(
    src: Path,
    dst: Path,
    adopt: Adopt,
    max_tar_mb: int,
    max_frame_mb: int,
    *,
    index_only: bool = False,
) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    with closing(open_db(dst / DB_NAME)) as db:
        attach(db, src / DB_NAME)
        per_tar = dict(
            db.execute(
                "SELECT f.tar, count(*) FROM s.objects o CROSS JOIN s.frames f"
                " WHERE f.id = o.frame GROUP BY f.tar"
            ).fetchall()
        )
        per_frame = dict(
            db.execute(
                "SELECT frame, count(*) FROM s.objects GROUP BY frame"
            ).fetchall()
        )
        missing = db.execute(
            "SELECT f.tar, o.frame, f.offset, f.length, f.raw, o.checksum, o.offset,"
            f" o.size{_MISSING} ORDER BY f.tar, f.offset, o.offset"
        )
        with Packer(
            dst,
            db,
            max_tar_mb=max_tar_mb,
            max_frame_mb=max_frame_mb,
            index_only=index_only,
        ) as pack:
            for tar, group in groupby(missing, key=lambda row: row[0]):
                rows = list(group)
                path = src / tar_name(tar)
                if not path.exists():
                    logger.warning("%s not found; %d objects skipped.", path, len(rows))
                elif len(rows) == per_tar[tar]:
                    query = "SELECT id, offset, length, raw FROM s.frames WHERE tar = ?"
                    frames = db.execute(query, (tar,)).fetchall()
                    pack.adopt(path, frames, [r[1:2] + r[5:] for r in rows], adopt)
                else:
                    _merge_frames(pack, path, rows, per_frame)
            db.execute("INSERT OR IGNORE INTO main.files SELECT * FROM s.files")
            pack.commit()


def _merge_frames(
    pack: Packer, tar: Path, rows: list[tuple], per_frame: dict[int, int]
) -> None:
    """Copy src frames dst lacks entirely; re-pack dst's missing objects of the rest."""
    for (frame, offset, length, raw), group in groupby(rows, key=lambda r: r[1:5]):
        members = [r[5:] for r in group]
        if len(members) == per_frame[frame]:
            pack.copy_frame(tar, offset, length, raw, members)
            continue
        data = read_span(tar, offset, 0, raw)
        for checksum, start, size in members:
            pack.add(checksum, data[start : start + size], size)


@contextmanager
def _beside(path: Path, tag: str) -> Iterator[Path]:
    """A temporary dir next to path: same filesystem, never the OS temp dir."""
    with tempfile.TemporaryDirectory(
        dir=path.parent, prefix=f".{path.name}-{tag}-"
    ) as tmp:
        yield Path(tmp)
