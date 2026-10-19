"""Core archive and restore operations."""

import logging
import shutil
import tempfile
from pathlib import Path

from .storage import (
    ARK_DIR_EXT,
    DB_NAME,
    MAX_TAR_MB,
    b2sum,
    ensure_clean_outdir,
    extract_objects,
    open_db,
    write_objects_to_tars,
)

logger = logging.getLogger(__name__)


def archive_dir(
    src_dir: Path,
    *,
    ark_out: Path | None = None,
    max_tar_mb: int = MAX_TAR_MB,
) -> None:
    """Archive src_dir into a content-addressed store.

    By default the archive is created at src_dir + ARK_DIR_EXT. Pass ark_out
    to write into an existing ark directory (useful for merging or remote push).

    Archiving is idempotent: re-running on the same directory is a no-op.
    Files with duplicate content are deduplicated by BLAKE2b checksum.

    Work streams in ~max_tar_mb batches, each committed to the index as it
    is packed — an interrupted run keeps everything packed so far and
    resumes where it left off.
    """
    if ark_out is None:
        ark_out = Path(f"{src_dir}{ARK_DIR_EXT}")
    ensure_clean_outdir(ark_out)
    db = open_db(ark_out / DB_NAME)
    cur = db.cursor()

    existing_paths = {r[0] for r in cur.execute("SELECT path FROM files")}
    known_objects = {r[0] for r in cur.execute("SELECT checksum FROM objects")}

    max_bytes = max_tar_mb * 1024 * 1024
    batch_objects: dict[str, Path] = {}
    batch_files: list[tuple[str, str]] = []
    acc = 0

    def flush() -> None:
        nonlocal batch_objects, batch_files, acc
        if batch_objects:
            written = write_objects_to_tars(
                ark_out, batch_objects, max_tar_mb=max_tar_mb
            )
            for checksum, tar_name in written.items():
                cur.execute("INSERT INTO objects VALUES (?, ?)", (checksum, tar_name))
                known_objects.add(checksum)
        for rel, checksum in batch_files:
            cur.execute("INSERT INTO files VALUES (?, ?)", (rel, checksum))
        db.commit()
        batch_objects, batch_files, acc = {}, [], 0

    for path in sorted(src_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(src_dir).as_posix()
        if rel in existing_paths:
            continue
        checksum = b2sum(path)
        if checksum not in known_objects and checksum not in batch_objects:
            batch_objects[checksum] = path
            acc += path.stat().st_size
        batch_files.append((rel, checksum))
        if acc >= max_bytes:
            flush()

    flush()
    db.close()


def restore_ark(ark_dir: Path, dest_dir: Path) -> None:
    """Restore all files from an ark to dest_dir.

    dest_dir is only created if there are files to restore.
    Missing tars or objects produce warnings but do not abort the restore.
    """
    db = open_db(ark_dir / DB_NAME)
    cur = db.cursor()

    cur.execute("SELECT path, checksum FROM files")
    files = cur.fetchall()

    if not files:
        print("No files found to restore in the archive.")
        db.close()
        return

    dest_dir.mkdir(parents=True, exist_ok=True)
    cur.execute("SELECT checksum, tar_name FROM objects")
    checksum_to_tar = dict(cur.fetchall())
    db.close()

    by_tar: dict[Path, list[tuple[str, str]]] = {}
    for rel, checksum in files:
        tar_name = checksum_to_tar.get(checksum)
        if tar_name:
            by_tar.setdefault(ark_dir / tar_name, []).append((rel, checksum))
        else:
            logger.warning("checksum %s for %s not in objects.", checksum, rel)

    with tempfile.TemporaryDirectory() as tmp:
        obj_cache = Path(tmp)

        for tar_path, tar_files in by_tar.items():
            if not tar_path.exists():
                logger.warning("%s not found.", tar_path.name)
                continue

            found = extract_objects(tar_path, {cs for _, cs in tar_files}, obj_cache)

            for rel, checksum in tar_files:
                if checksum not in found:
                    logger.warning(
                        "object %s missing from %s.", checksum, tar_path.name
                    )
                    continue
                dest_file = dest_dir / rel
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(obj_cache / checksum, dest_file)
