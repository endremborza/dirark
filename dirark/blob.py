"""Content-addressed byte storage on top of a dirark archive.

A `BlobStore` exposes a hash-keyed bytes interface over the same on-disk
layout used by `archive_dir`. Writes go to a staging directory keyed by the
blake2b checksum of the payload; `commit` packs staged blobs into the ark's
tar.zst archives via the standard dirark storage primitives.

Layout::

    <root>/staging/         # flat files: <checksum> -> raw bytes
    <root>/ark/             # standard dirark ark (index.sqlite + data-*.tar.zst)

A committed blob lives in a zstd tar, and pulling one member out decompresses
the whole tar. `read` streams out the one object, unless the previous such
read hit the same tar: then it unpacks that tar into a directory under the
store root, and the `_UNPACKED` most recently used directories serve later
reads. Random reads pay one decompression each; `commit` packs blobs in the
order they were written, so reads in write order unpack each tar once.

Concurrency: `write` is safe across processes (atomic rename into staging).
`commit` must run with no concurrent writers — i.e. only after workers stop.
"""

import os
import shutil
import sqlite3
import tempfile
import weakref
from pathlib import Path

from .storage import (
    DB_NAME,
    b2sum_bytes,
    extract_tar_zst,
    open_db,
    read_object_from_tar,
    write_objects_to_tars,
)
from .sync import merge_arks, pull_ark, push_ark

_STAGING = "staging"
_ARK = "ark"
# Packed size does not grow with smaller tars (zstd's window is smaller still),
# while every read decompresses a whole tar.
_TAR_MB = 16
# Blobs written within one mtime tick pack in any order, so reads in write
# order alternate between neighbouring tars.
_UNPACKED = 2
_UNPACK_PREFIX = ".unpacked-"


def blob_checksum(data: bytes) -> str:
    """Return the BLAKE2b content-address key for ``data``.

    Uses the same BLAKE2b-512 (`hashlib`) as archive_dir, so a blob and an
    archived file with identical content share a checksum and deduplicate
    against each other.
    """
    return b2sum_bytes(data)


class _Unpacked:
    """A tar's objects, unpacked into a directory removed with this object."""

    def __init__(self, tar: Path, parent: Path) -> None:
        self.tar = tar.name
        root = Path(tempfile.mkdtemp(prefix=_UNPACK_PREFIX, dir=parent))
        weakref.finalize(self, shutil.rmtree, root, ignore_errors=True)
        extract_tar_zst(tar, root)
        self.objects = root / "objects"


class BlobStore:
    """Content-addressed byte storage over a dirark ark."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.staging = self.root / _STAGING
        self.ark = self.root / _ARK
        self.staging.mkdir(parents=True, exist_ok=True)
        self.ark.mkdir(parents=True, exist_ok=True)
        self._last_tar = ""
        self._unpacked: list[_Unpacked] = []

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
        unpacked = next((u for u in self._unpacked if u.tar == tar_name), None)
        if unpacked is None or not (unpacked.objects / checksum).exists():
            if self._last_tar != tar_name:
                self._last_tar = tar_name
                return read_object_from_tar(self.ark / tar_name, checksum)
            unpacked = _Unpacked(self.ark / tar_name, self.root)
        others = [u for u in self._unpacked if u.tar != tar_name]
        self._unpacked = [unpacked, *others][:_UNPACKED]
        return (unpacked.objects / checksum).read_bytes()

    def has(self, checksum: str) -> bool:
        """Return True if `checksum` is staged or committed."""
        return self._in_staging(checksum) or self._in_ark(checksum)

    def commit(self) -> None:
        """Pack staged blobs into the ark in write order; clear staging."""
        staged = sorted(
            (p for p in self.staging.iterdir() if p.is_file()),
            key=lambda p: p.stat().st_mtime_ns,
        )
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
            written = write_objects_to_tars(self.ark, new_objects, max_tar_mb=_TAR_MB)
            for checksum, tar_name in written.items():
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
            merge_arks(other.ark, self.ark, max_tar_mb=_TAR_MB)

    def push(self, remote: str) -> None:
        """Merge committed blobs into the store rooted at `remote`; staging stays."""
        push_ark(self.ark, f"{remote}/{_ARK}", max_tar_mb=_TAR_MB)

    def pull(self, remote: str) -> None:
        """Merge the committed blobs of the store rooted at `remote` into this one."""
        pull_ark(f"{remote}/{_ARK}", self.ark, max_tar_mb=_TAR_MB)

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
