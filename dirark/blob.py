"""Content-addressed byte storage on top of a dirark archive.

A `BlobStore` exposes a hash-keyed bytes interface over the same on-disk
layout used by `archive_dir`. Writes go to a staging directory keyed by the
blake2b checksum of the payload; `commit` packs staged blobs into the ark's
frames in the order they were written.

Layout::

    <root>/staging/         # flat files: <checksum> -> raw bytes
    <root>/ark/             # standard dirark ark (index.sqlite + data-*.tar.zst)

A read decodes the frame holding its blob, and decoded frames stay in a
`FrameCache`, so reads in write order decode each frame once. Frames are
FRAME_MB, smaller than archive_dir's: a cold read decodes a whole frame.

Concurrency: `write` is safe across processes (atomic rename into staging).
`commit` must run with no concurrent writers or readers — i.e. only after
workers stop.
"""

import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from .storage import (
    DB_NAME,
    ArkFormatError,
    FrameCache,
    Location,
    Packer,
    digest_bytes,
    locate,
    open_db,
)
from .sync import merge_arks, pull_ark, push_ark

FRAME_MB = 16
_STAGING = "staging"
_ARK = "ark"


def blob_checksum(data: bytes) -> str:
    """Return the BLAKE2b content-address key for ``data``.

    Uses the same BLAKE2b-512 (`hashlib`) as archive_dir, so a blob and an
    archived file with identical content share a checksum and deduplicate
    against each other.
    """
    return digest_bytes(data).hex()


class BlobStore:
    """Content-addressed byte storage over a dirark ark."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.staging = self.root / _STAGING
        self.ark = self.root / _ARK
        self.staging.mkdir(parents=True, exist_ok=True)
        self.ark.mkdir(parents=True, exist_ok=True)
        self._db: sqlite3.Connection | None = None
        self._pid = 0
        self._frames = FrameCache()

    def write(self, data: bytes) -> str:
        """Store `data`; return its blake2b checksum. Idempotent."""
        checksum = blob_checksum(data)
        if not self.has(checksum):
            self._atomic_stage(checksum, data)
        return checksum

    def read(self, checksum: str) -> bytes:
        """Return bytes for `checksum`; raise KeyError if unknown."""
        staged = self.staging / checksum
        if staged.exists():
            return staged.read_bytes()
        loc = self._locate(checksum)
        if loc is None:
            raise KeyError(checksum)
        return self._frames.read(self.ark, loc)

    def has(self, checksum: str) -> bool:
        """Return True if `checksum` is staged or committed."""
        return (self.staging / checksum).exists() or self._locate(checksum) is not None

    def commit(self) -> None:
        """Pack staged blobs into the ark in write order; clear staging."""
        staged = sorted(
            ((p.stat(), p) for p in self.staging.iterdir() if p.is_file()),
            key=lambda sp: sp[0].st_mtime_ns,
        )
        if not staged:
            return
        db = self._index(create=True)
        assert db is not None
        with Packer(self.ark, db, max_frame_mb=FRAME_MB) as pack:
            for st, p in staged:
                checksum = bytes.fromhex(p.name)
                if locate(db, checksum) is None:
                    pack.add(checksum, p, st.st_size)
            pack.commit()
        for _, p in staged:
            p.unlink()

    def merge_from(self, other: "BlobStore") -> None:
        """Pull all staged + committed blobs from `other` into self."""
        for p in other.staging.iterdir():
            if p.is_file() and not self.has(p.name):
                self._atomic_stage(p.name, p.read_bytes())
        if (other.ark / DB_NAME).exists():
            merge_arks(other.ark, self.ark, max_frame_mb=FRAME_MB)

    def push(self, remote: str) -> None:
        """Merge committed blobs into the store rooted at `remote`; staging stays."""
        push_ark(self.ark, f"{remote}/{_ARK}", max_frame_mb=FRAME_MB)

    def pull(self, remote: str) -> None:
        """Merge the committed blobs of the store rooted at `remote` into this one."""
        pull_ark(f"{remote}/{_ARK}", self.ark, max_frame_mb=FRAME_MB)

    def purge(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
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

    def _locate(self, checksum: str) -> Location | None:
        db = self._index()
        return None if db is None else locate(db, bytes.fromhex(checksum))

    def _index(self, create: bool = False) -> sqlite3.Connection | None:
        """This process's connection to the ark index; None while there is none.

        A lookup never creates the index; a connection is not reused across fork.
        """
        if self._db is not None and self._pid == os.getpid():
            return self._db
        path = self.ark / DB_NAME
        if not create and not path.exists():
            return None
        try:
            self._db = open_db(path)
        except ArkFormatError as e:
            migrate = f"dirark migrate {self.ark} --max-frame-mb {FRAME_MB}"
            raise ArkFormatError(e.path, e.version, migrate) from None
        self._pid = os.getpid()
        return self._db
