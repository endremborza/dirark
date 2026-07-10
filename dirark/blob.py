"""Content-addressed byte storage on top of a dirark archive.

A `BlobStore` exposes a hash-keyed bytes interface over the same on-disk
layout used by `archive_dir`. Writes go to a staging directory keyed by the
blake2b checksum of the payload; `commit` packs staged blobs into the ark's
tar.zst archives via the standard dirark storage primitives.

Layout::

    <root>/staging/         # flat files: <checksum> -> raw bytes
    <root>/ark/             # standard dirark ark (index.sqlite + data-*.tar.zst)

Concurrency: `write` is safe across processes (atomic rename into staging).
`commit` must run with no concurrent writers — i.e. only after workers stop.
"""

import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from .storage import (
    DB_NAME,
    b2sum_bytes,
    open_db,
    read_object_from_tar,
    write_objects_to_tar,
)
from .sync import merge_arks

_STAGING = "staging"
_ARK = "ark"


def blob_checksum(data: bytes) -> str:
    """Return the BLAKE2b content-address key for ``data``.

    Uses the same BLAKE2b-512 (`hashlib`) as archive_dir, so a blob and an
    archived file with identical content share a checksum and deduplicate
    against each other.
    """
    return b2sum_bytes(data)


class BlobStore:
    """Content-addressed byte storage over a dirark ark."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.staging = self.root / _STAGING
        self.ark = self.root / _ARK
        self.staging.mkdir(parents=True, exist_ok=True)
        self.ark.mkdir(parents=True, exist_ok=True)

    def write(self, data: bytes) -> str:
        """Store `data`; return its blake2b checksum. Idempotent."""
        checksum = blob_checksum(data)
        if self._in_staging(checksum) or self._in_ark(checksum):
            return checksum
        self._atomic_stage(checksum, data)
        return checksum

    def read(self, checksum: str) -> bytes:
        """Return bytes for `checksum`; raise KeyError if unknown."""
        staged = self.staging / checksum
        if staged.exists():
            return staged.read_bytes()
        tar_name = self._tar_for(checksum)
        if tar_name is None:
            raise KeyError(checksum)
        return read_object_from_tar(self.ark / tar_name, checksum)

    def has(self, checksum: str) -> bool:
        """Return True if `checksum` is staged or committed."""
        return self._in_staging(checksum) or self._in_ark(checksum)

    def commit(self) -> None:
        """Pack staged blobs into the ark; clear staging."""
        staged = [p for p in self.staging.iterdir() if p.is_file()]
        if not staged:
            return
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        new_objects: dict[str, Path] = {}
        for p in staged:
            cur.execute("SELECT 1 FROM objects WHERE checksum=?", (p.name,))
            if cur.fetchone() is None:
                new_objects[p.name] = p
        if new_objects:
            tar_name = write_objects_to_tar(self.ark, new_objects)
            for checksum in new_objects:
                cur.execute(
                    "INSERT OR IGNORE INTO objects VALUES (?, ?)",
                    (checksum, tar_name),
                )
            db.commit()
        db.close()
        for p in staged:
            p.unlink()

    def merge_from(self, other: "BlobStore") -> None:
        """Pull all staged + committed blobs from `other` into self."""
        for p in other.staging.iterdir():
            if p.is_file() and not self.has(p.name):
                self._atomic_stage(p.name, p.read_bytes())
        if (other.ark / DB_NAME).exists():
            merge_arks(other.ark, self.ark)

    def purge(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)

    def _atomic_stage(self, checksum: str, data: bytes) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.staging)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, self.staging / checksum)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def _in_staging(self, checksum: str) -> bool:
        return (self.staging / checksum).exists()

    def _in_ark(self, checksum: str) -> bool:
        return self._tar_for(checksum) is not None

    def _tar_for(self, checksum: str) -> str | None:
        db_path = self.ark / DB_NAME
        if not db_path.exists():
            return None
        # Raw read-only connect: a lookup must not create open_db's schema.
        db = sqlite3.connect(db_path)
        try:
            cur = db.execute(
                "SELECT tar_name FROM objects WHERE checksum=?", (checksum,)
            )
            row = cur.fetchone()
        finally:
            db.close()
        return row[0] if row else None
