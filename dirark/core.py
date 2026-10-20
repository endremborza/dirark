"""Core archive and restore operations."""

import logging
import os
import shutil
import stat
import subprocess
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import batched, groupby
from pathlib import Path
from sqlite3 import Connection
from subprocess import PIPE
from typing import IO

from .storage import (
    ARK_DIR_EXT,
    DB_NAME,
    HEADER,
    LOOKUP_ROWS,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    MB,
    Packer,
    copy_exact,
    digest_file,
    ensure_clean_outdir,
    open_db,
    query_in,
    read_exact,
    tar_name,
)

logger = logging.getLogger(__name__)

_KNOWN = (
    "SELECT fi.path, fi.mtime_ns, fi.mode, o.size FROM files fi"
    " JOIN objects o ON o.checksum = fi.checksum WHERE fi.path IN ({})"
)


@dataclass(frozen=True, slots=True)
class _Entry:
    rel: str
    path: str
    size: int
    mtime_ns: int
    mode: int


def archive_dir(
    src_dir: Path,
    *,
    ark_out: Path | None = None,
    max_tar_mb: int = MAX_TAR_MB,
    max_frame_mb: int = MAX_FRAME_MB,
) -> None:
    """Archive src_dir into a content-addressed store.

    By default the archive is created at src_dir + ARK_DIR_EXT. Pass ark_out
    to write into an existing ark directory (useful for merging or remote push).

    Re-archiving is incremental: a path whose size, mtime and mode match the
    index is skipped without hashing; any other is hashed and repointed at its
    content, stored once per BLAKE2b checksum. Content a path pointed at before
    stays in the ark.

    Changed files go in batches of max_tar_mb, each committed to the index as it
    is packed, so an interrupted run keeps everything packed so far.
    """
    if ark_out is None:
        ark_out = Path(f"{src_dir}{ARK_DIR_EXT}")
    ensure_clean_outdir(ark_out)
    db = open_db(ark_out / DB_NAME)
    limit = max_tar_mb * MB
    with (
        Packer(ark_out, db, max_tar_mb=max_tar_mb, max_frame_mb=max_frame_mb) as pack,
        ThreadPoolExecutor() as hashers,
    ):
        batch: list[_Entry] = []
        acc = 0
        for entries in batched(_walk(src_dir), LOOKUP_ROWS):
            known = {
                row[0]: row[1:]
                for row in query_in(db, _KNOWN, [e.rel for e in entries])
            }
            for e in entries:
                if known.get(e.rel) != (e.mtime_ns, e.mode, e.size):
                    batch.append(e)
                    acc += e.size
                    if acc >= limit:
                        _pack(db, pack, hashers, batch)
                        batch, acc = [], 0
        _pack(db, pack, hashers, batch)
    db.close()


def restore_ark(ark_dir: Path, dest_dir: Path) -> None:
    """Restore all files from an ark to dest_dir, with their mode and mtime.

    dest_dir is only created if there are files to restore.
    Missing tars or objects produce warnings but do not abort the restore.
    """
    db = open_db(ark_dir / DB_NAME)
    if db.execute("SELECT 1 FROM files LIMIT 1").fetchone() is None:
        print("No files found to restore in the archive.")
        db.close()
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    orphans = db.execute(
        "SELECT path, checksum FROM files fi WHERE NOT EXISTS"
        " (SELECT 1 FROM objects o WHERE o.checksum = fi.checksum)"
    )
    for rel, checksum in orphans:
        logger.warning("checksum %s for %s not in objects.", checksum.hex(), rel)

    rows = db.execute(
        "SELECT f.tar, f.id, o.offset, o.size, fi.checksum, fi.path, fi.mode,"
        " fi.mtime_ns FROM files fi CROSS JOIN objects o CROSS JOIN frames f"
        " WHERE o.checksum = fi.checksum AND f.id = o.frame"
        " ORDER BY f.tar, f.offset, o.offset"
    )
    for tar, group in groupby(rows, key=lambda row: row[0]):
        path = ark_dir / tar_name(tar)
        if not path.exists():
            logger.warning("%s not found.", path.name)
            continue
        bases = _frame_bases(db, tar)
        objects = [
            (bases[fid] + start, size, checksum, [row[5:] for row in same])
            for (fid, start, size, checksum), same in groupby(group, lambda r: r[1:5])
        ]
        with subprocess.Popen(["zstd", "-qdc", str(path)], stdout=PIPE) as proc:
            assert proc.stdout is not None
            try:
                _restore_objects(proc.stdout, objects, dest_dir, path.name)
            except EOFError:
                logger.warning("%s ends early; the rest of it is skipped.", path.name)
            finally:
                proc.kill()
    db.close()


def _walk(root: Path) -> Iterator[_Entry]:
    """Regular files under root, through file symlinks but not directory ones."""
    cut = len(str(root)) + 1
    stack = [str(root)]
    while stack:
        with os.scandir(stack.pop()) as it:
            entries = sorted(it, key=lambda e: e.name)
        subdirs = []
        for e in entries:
            if e.is_dir(follow_symlinks=False):
                subdirs.append(e.path)
            elif e.is_file():
                st = e.stat()
                mode = stat.S_IMODE(st.st_mode)
                yield _Entry(e.path[cut:], e.path, st.st_size, st.st_mtime_ns, mode)
        stack.extend(reversed(subdirs))


def _pack(
    db: Connection, pack: Packer, hashers: ThreadPoolExecutor, batch: list[_Entry]
) -> None:
    if not batch:
        return
    digests = list(hashers.map(digest_file, (e.path for e in batch)))
    sql = "SELECT checksum FROM objects WHERE checksum IN ({})"
    stored = {cs for (cs,) in query_in(db, sql, list(set(digests)))}
    for e, digest in zip(batch, digests):
        if digest not in stored:
            stored.add(digest)
            pack.add(digest, Path(e.path), e.size, e.mtime_ns)
    db.executemany(
        "INSERT OR REPLACE INTO files VALUES (?, ?, ?, ?)",
        ((e.rel, d, e.mtime_ns, e.mode) for e, d in zip(batch, digests)),
    )
    pack.commit()


def _frame_bases(db: Connection, tar: int) -> dict[int, int]:
    """Each frame's position in the tar's decoded stream: frames tile a tar."""
    query = "SELECT id, raw FROM frames WHERE tar = ? ORDER BY offset"
    bases, pos = {}, 0
    for fid, raw in db.execute(query, (tar,)):
        bases[fid] = pos
        pos += raw
    return bases


def _restore_objects(
    stream: IO[bytes],
    objects: list[tuple[int, int, bytes, list[tuple[str, int, int]]]],
    dest_dir: Path,
    tar: str,
) -> None:
    """Copy objects, in stream order, out of a tar's decoded stream to their paths.

    An object whose tar header does not name it is skipped with a warning.
    """
    at = 0
    for pos, size, checksum, targets in objects:
        if pos - HEADER < at:
            logger.warning("object %s missing from %s.", checksum.hex(), tar)
            continue
        copy_exact(stream, pos - HEADER - at, None)
        if checksum.hex().encode() not in read_exact(stream, HEADER):
            logger.warning("object %s missing from %s.", checksum.hex(), tar)
            at = pos
            continue
        paths = [dest_dir / rel for rel, _, _ in targets]
        for p in paths:
            p.parent.mkdir(parents=True, exist_ok=True)
        with open(paths[0], "wb") as out:
            copy_exact(stream, size, out)
        for p in paths[1:]:
            shutil.copyfile(paths[0], p)
        for p, (_, mode, mtime_ns) in zip(paths, targets):
            os.chmod(p, mode)
            os.utime(p, ns=(mtime_ns, mtime_ns))
        at = pos + size
