"""Low-level storage: checksums, the ark index, and framed tar.zst packing and reads.

A tar is a run of independent zstd frames, each decoding to whole tar members
named ``./objects/<checksum hex>``, so the file stays a plain tar.zst while the
index addresses single frames. Tars carry no end-of-archive blocks: GNU tar and
Python's tarfile both stop cleanly after the last member, so appending a frame
is a plain append.
"""

import hashlib
import os
import sqlite3
import subprocess
import tarfile
import threading
from collections import OrderedDict, deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from itertools import batched
from pathlib import Path
from typing import IO, Any, Literal

MB = 1 << 20
MAX_TAR_MB = 256
MAX_FRAME_MB = 64
READ_CACHE_MB = 128
SCHEMA_VERSION = 2
ZSTD_ARGS = ("-9", "-T0", "--long=27")
LOOKUP_ROWS = 1024
DB_NAME = "index.sqlite"
SEP = "-"
TAR_PREFIX = "data" + SEP
TAR_EXT = ".tar.zst"
ARK_DIR_EXT = ".ark.d"
OBJECT_DIR = "./objects/"

# zstd's -T0 barely parallelizes a single frame of a few dozen MB.
_COMPRESS_JOBS = 4
_TAR_MODE = 0o644
_SCHEMA = (
    "CREATE TABLE files(path TEXT PRIMARY KEY, checksum BLOB NOT NULL,"
    " mtime_ns INTEGER NOT NULL, mode INTEGER NOT NULL) WITHOUT ROWID",
    "CREATE TABLE frames(id INTEGER PRIMARY KEY, tar INTEGER NOT NULL,"
    " offset INTEGER NOT NULL, length INTEGER NOT NULL, raw INTEGER NOT NULL)",
    "CREATE INDEX frames_by_tar ON frames(tar, offset)",
    "CREATE TABLE objects(checksum BLOB PRIMARY KEY, frame INTEGER NOT NULL,"
    " offset INTEGER NOT NULL, size INTEGER NOT NULL) WITHOUT ROWID",
)
_LOCATE = (
    "SELECT f.tar, f.offset, f.raw, o.offset, o.size"
    " FROM objects o JOIN frames f ON f.id = o.frame"
)

Adopt = Literal["copy", "link", "move"]


class ArkFormatError(RuntimeError):
    """An ark index in a format this version neither reads nor writes."""

    def __init__(self, path: Path, version: int, migrate: str) -> None:
        super().__init__(
            f"{path} is index format {version}, this dirark reads"
            f" {SCHEMA_VERSION}: run `{migrate}`"
        )
        self.path, self.version = path, version


class SourceChanged(RuntimeError):
    """A file changed between being hashed and being packed."""


@dataclass(frozen=True, slots=True)
class Location:
    """An object's frame (start in a tar, decoded size) and its span in that frame."""

    tar: int
    offset: int
    raw: int
    start: int
    size: int


@dataclass(slots=True)
class _Member:
    checksum: bytes
    source: Path | bytes
    size: int
    mtime_ns: int | None


@dataclass(slots=True)
class _Frame:
    """New members to compress after the decoded bytes of a reopened frame."""

    prefix: bytes = b""
    frame_id: int | None = None
    raw: int = 0
    members: list[_Member] = field(default_factory=list)

    def objects(self) -> list[tuple[bytes, int, int]]:
        rows, pos = [], len(self.prefix)
        for m in self.members:
            rows.append((m.checksum, pos + HEADER, m.size))
            pos += _member_raw(m.size)
        return rows


@dataclass(slots=True)
class _Write:
    """A frame awaiting its place in a tar: compressed bytes or another tar's span."""

    raw: int
    frame_id: int | None
    objects: list[tuple[bytes, int, int]]
    payload: Future[bytes] | tuple[Path, int, int]


class FrameCache:
    """Decoded frames, least recently used evicted first, within READ_CACHE_MB.

    A frame over the budget is never held: a read decodes it up to its object.
    """

    def __init__(self) -> None:
        self._frames: OrderedDict[tuple[Path, int], bytes] = OrderedDict()
        self._bytes = 0

    def read(self, ark_dir: Path, loc: Location) -> bytes:
        key = (ark_dir / tar_name(loc.tar), loc.offset)
        budget = READ_CACHE_MB * MB
        if loc.raw > budget:
            return read_span(key[0], loc.offset, loc.start, loc.size)
        frame = self._frames.get(key)
        if frame is None:
            frame = self._frames[key] = read_span(key[0], loc.offset, 0, loc.raw)
            self._bytes += loc.raw
            while self._bytes > budget:
                self._bytes -= len(self._frames.popitem(last=False)[1])
        else:
            self._frames.move_to_end(key)
        return frame[loc.start : loc.start + loc.size]


class Packer:
    """Appends objects to an ark's tars as independent zstd frames.

    Members fill a frame up to max_frame_mb of tar bytes (a larger object gets a
    frame of its own, streamed straight into its tar), frames fill a tar up to
    max_tar_mb, and up to _COMPRESS_JOBS frames compress side by side. Index
    rows go onto the given connection; `commit` writes what is pending, commits,
    and tidies.

    A frame left under max_frame_mb is reopened by the next `add` that fits:
    re-encoded together with the new members into a fresh tar, since rewriting
    it in place would lose its objects to a crash. `tidy` then cuts the old tar
    back to its indexed end. index_only packs into an index-only copy of an ark,
    whose tars live elsewhere: new tars only, nothing reopened or tidied.
    """

    def __init__(
        self,
        ark_dir: Path,
        db: sqlite3.Connection,
        *,
        max_tar_mb: int = MAX_TAR_MB,
        max_frame_mb: int = MAX_FRAME_MB,
        index_only: bool = False,
        first_tar: int = 1,
    ) -> None:
        self.ark_dir, self.db = ark_dir, db
        self._tar_cap, self._frame_cap = max_tar_mb * MB, max_frame_mb * MB
        self._index_only, self._first_tar = index_only, first_tar
        self._pending = _Frame()
        self._queue: deque[_Write] = deque()
        self._pool = ThreadPoolExecutor(_COMPRESS_JOBS)
        if not index_only:
            self.tidy()
        self._resume()

    def __enter__(self) -> "Packer":
        return self

    def __exit__(self, *_: object) -> None:
        self._pool.shutdown(cancel_futures=True)

    def add(
        self,
        checksum: bytes,
        source: Path | bytes,
        size: int,
        mtime_ns: int | None = None,
    ) -> None:
        """Queue an object for packing.

        A Path source is read when its frame is written and must then still have
        `size` bytes and, if given, `mtime_ns`, or SourceChanged is raised.
        """
        raw = _member_raw(size)
        if self._reopen is not None:
            fid, tar, offset, frame_raw = self._reopen
            self._reopen = None
            if frame_raw + raw <= self._frame_cap:
                prefix = read_span(self._tar_path(tar), offset, 0, frame_raw)
                self._pending = _Frame(prefix, fid, frame_raw)
        if self._pending.raw and self._pending.raw + raw > self._frame_cap:
            self._submit()
        self._pending.members.append(_Member(checksum, source, size, mtime_ns))
        self._pending.raw += raw
        if self._pending.raw >= self._frame_cap:
            self._submit()

    def copy_frame(
        self,
        tar: Path,
        offset: int,
        length: int,
        raw: int,
        objects: list[tuple[bytes, int, int]],
    ) -> None:
        """Append another ark's frame verbatim; objects are (checksum, start, size)."""
        self._enqueue(_Write(raw, None, objects, (tar, offset, length)))

    def adopt(
        self,
        tar: Path,
        frames: Sequence[tuple[int, int, int, int]],
        objects: Iterable[tuple[int, bytes, int, int]],
        how: Adopt,
    ) -> None:
        """Take over another ark's whole tar under a fresh name.

        frames are its (id, offset, length, raw) rows, objects its
        (frame id, checksum, start, size) rows.
        """
        idx = self._next_tar
        self._next_tar += 1
        _place_file(tar, self._tar_path(idx), max(o + n for _, o, n, _ in frames), how)
        insert = "INSERT INTO frames(tar, offset, length, raw) VALUES (?, ?, ?, ?)"
        ids = {
            fid: self.db.execute(insert, (idx, offset, length, raw)).lastrowid
            for fid, offset, length, raw in frames
        }
        self.db.executemany(
            "INSERT INTO objects VALUES (?, ?, ?, ?)",
            ((cs, ids[fid], start, size) for fid, cs, start, size in objects),
        )

    def commit(self) -> None:
        """Write every pending frame, commit the index, tidy."""
        if self._pending.members:
            self._submit()
        while self._queue:
            self._append(self._queue.popleft())
        self.db.commit()
        if not self._index_only:
            self.tidy()
        self._resume()

    def tidy(self) -> None:
        """Cut each tar back to its indexed end; delete tars the index does not use."""
        query = "SELECT tar, max(offset + length) FROM frames GROUP BY tar"
        ends = dict(self.db.execute(query).fetchall())
        for path in list_tars(self.ark_dir):
            end = ends.get(tar_index(path.name))
            if end is None:
                path.unlink()
            elif path.stat().st_size > end:
                os.truncate(path, end)

    def _resume(self) -> None:
        self._tar: int | None = None
        self._tar_raw = self._end = 0
        self._reopen: tuple[int, int, int, int] | None = None
        top = self.db.execute("SELECT max(tar) FROM frames").fetchone()[0]
        self._next_tar = max(self._first_tar, (top or 0) + 1)
        if top is None or self._index_only:
            return
        fid, offset, length, raw = self.db.execute(
            "SELECT id, offset, length, raw FROM frames WHERE tar = ?"
            " ORDER BY offset DESC LIMIT 1",
            (top,),
        ).fetchone()
        if raw < self._frame_cap:
            self._reopen = (fid, top, offset, raw)
            return
        query = "SELECT sum(raw) FROM frames WHERE tar = ?"
        tar_raw = self.db.execute(query, (top,)).fetchone()[0]
        if tar_raw < self._tar_cap:
            self._tar, self._tar_raw, self._end = top, tar_raw, offset + length

    def _submit(self) -> None:
        frame, self._pending = self._pending, _Frame()
        if frame.raw <= self._frame_cap:
            future = self._pool.submit(_compress, frame, None)
            self._enqueue(_Write(frame.raw, frame.frame_id, frame.objects(), future))
            return
        while self._queue:
            self._append(self._queue.popleft())
        tar, offset = self._place(frame.raw, fresh=False)
        end = self._write_at(tar, offset, lambda fd: _compress(frame, fd))
        self._record(tar, offset, end, frame.raw, None, frame.objects())

    def _enqueue(self, write: _Write) -> None:
        self._queue.append(write)
        if len(self._queue) > _COMPRESS_JOBS:
            self._append(self._queue.popleft())

    def _append(self, w: _Write) -> None:
        tar, offset = self._place(w.raw, fresh=w.frame_id is not None)
        if isinstance(w.payload, Future):
            data = w.payload.result()
            end = self._write_at(tar, offset, lambda fd: _write_all(fd, data))
        else:
            src, src_offset, length = w.payload
            end = self._write_at(
                tar, offset, lambda fd: _copy_range(src, src_offset, length, fd)
            )
        self._record(tar, offset, end, w.raw, w.frame_id, w.objects)

    def _place(self, raw: int, *, fresh: bool) -> tuple[int, int]:
        full = self._tar_raw > 0 and self._tar_raw + raw > self._tar_cap
        if self._tar is None or fresh or full:
            self._tar, self._tar_raw, self._end = self._next_tar, 0, 0
            self._next_tar += 1
        return self._tar, self._end

    def _write_at(self, tar: int, offset: int, write: Callable[[int], Any]) -> int:
        fd = os.open(self._tar_path(tar), os.O_WRONLY | os.O_CREAT, _TAR_MODE)
        try:
            os.lseek(fd, offset, os.SEEK_SET)
            write(fd)
            return os.lseek(fd, 0, os.SEEK_CUR)
        finally:
            os.close(fd)

    def _record(
        self,
        tar: int,
        offset: int,
        end: int,
        raw: int,
        frame_id: int | None,
        objects: list[tuple[bytes, int, int]],
    ) -> None:
        self._tar_raw += raw
        self._end = end
        row = (tar, offset, end - offset, raw)
        if frame_id is None:
            insert = "INSERT INTO frames(tar, offset, length, raw) VALUES (?, ?, ?, ?)"
            frame_id = self.db.execute(insert, row).lastrowid
        else:
            update = "UPDATE frames SET tar = ?, offset = ?, length = ?, raw = ?"
            self.db.execute(f"{update} WHERE id = ?", (*row, frame_id))
        self.db.executemany(
            "INSERT INTO objects VALUES (?, ?, ?, ?)",
            ((cs, frame_id, start, size) for cs, start, size in objects),
        )

    def _tar_path(self, idx: int) -> Path:
        return self.ark_dir / tar_name(idx)


def digest_file(path: Path | str) -> bytes:
    """BLAKE2b-512 of a file: the digest the `b2sum` coreutil prints in hex."""
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "blake2b").digest()


def digest_bytes(data: bytes) -> bytes:
    """BLAKE2b-512 of in-memory bytes; equal to `digest_file` of the same content."""
    return hashlib.blake2b(data).digest()


def open_db(path: Path) -> sqlite3.Connection:
    """Open an ark index, creating the schema in a new one.

    Tables:
        files(path PK, checksum, mtime_ns, mode)  -- relative path → content
        frames(id PK, tar, offset, length, raw)    -- a frame's span of a tar
        objects(checksum PK, frame, offset, size)  -- content → span of a frame

    Raises ArkFormatError for an index of another format version.
    """
    db = sqlite3.connect(path)
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if version == 0 and db.execute("SELECT 1 FROM sqlite_master").fetchone() is None:
        for statement in _SCHEMA:
            db.execute(statement)
        db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        version = SCHEMA_VERSION
    if version != SCHEMA_VERSION:
        db.close()
        raise ArkFormatError(path, version or 1, f"dirark migrate {path.parent}")
    return db


def attach(db: sqlite3.Connection, path: Path) -> None:
    """Attach another ark's index as schema `s`, after checking its format."""
    open_db(path).close()
    db.execute("ATTACH DATABASE ? AS s", (str(path),))


def query_in(db: sqlite3.Connection, sql: str, keys: Sequence[Any]) -> Iterator[Any]:
    """Run `sql`, ending in `IN ({})`, over keys in chunks of LOOKUP_ROWS."""
    for chunk in batched(keys, LOOKUP_ROWS):
        yield from db.execute(sql.format(",".join("?" * len(chunk))), chunk)


def locate(db: sqlite3.Connection, checksum: bytes) -> Location | None:
    row = db.execute(f"{_LOCATE} WHERE o.checksum = ?", (checksum,)).fetchone()
    return Location(*row) if row else None


def locate_path(db: sqlite3.Connection, path: str) -> Location | None:
    join = "JOIN files fi ON fi.checksum = o.checksum"
    row = db.execute(f"{_LOCATE} {join} WHERE fi.path = ?", (path,)).fetchone()
    return Location(*row) if row else None


def read_span(tar: Path, offset: int, start: int, size: int) -> bytes:
    """Decode the frame at a tar's offset only as far as [start, start + size)."""
    with open(tar, "rb") as f:
        f.seek(offset)
        with subprocess.Popen(
            ["zstd", "-qdc"], stdin=f, stdout=subprocess.PIPE
        ) as proc:
            assert proc.stdout is not None
            try:
                copy_exact(proc.stdout, start, None)
                return read_exact(proc.stdout, size)
            finally:
                proc.kill()


def read_exact(stream: IO[bytes], n: int) -> bytes:
    data = stream.read(n)
    if len(data) < n:
        raise EOFError(f"stream ended {n - len(data)} bytes early")
    return data


def copy_exact(stream: IO[bytes], n: int, out: IO[bytes] | None) -> None:
    """Move n bytes from stream to out, or past them when out is None."""
    while n:
        chunk = read_exact(stream, min(n, MB))
        if out is not None:
            out.write(chunk)
        n -= len(chunk)


def tar_name(idx: int) -> str:
    return f"{TAR_PREFIX}{idx:05d}{TAR_EXT}"


def tar_index(name: str) -> int:
    """Return the sequence number encoded in a tar archive's file name."""
    return int(name.removesuffix(TAR_EXT).split(SEP)[-1])


def list_tars(ark_dir: Path) -> list[Path]:
    """Return sorted list of tar archives in an ark directory."""
    return sorted(ark_dir.glob(f"{TAR_PREFIX}*{TAR_EXT}"))


def ensure_clean_outdir(ark_dir: Path) -> None:
    """Create ark_dir if needed and raise RuntimeError on unexpected files."""
    ark_dir.mkdir(parents=True, exist_ok=True)
    allowed = {DB_NAME} | {p.name for p in list_tars(ark_dir)}
    for p in ark_dir.iterdir():
        if p.name not in allowed:
            raise RuntimeError(f"Unexpected file in archive dir: {p}")


def member_header(checksum: bytes, size: int) -> bytes:
    info = tarfile.TarInfo(OBJECT_DIR + checksum.hex())
    info.size = size
    return info.tobuf(tarfile.GNU_FORMAT)


HEADER = len(member_header(digest_bytes(b""), 0))


def _member_raw(size: int) -> int:
    return HEADER + size + _padding(size)


def _padding(size: int) -> int:
    return -size % tarfile.BLOCKSIZE


def _compress(frame: _Frame, out: int | None) -> bytes:
    """Compress a frame into fd `out`, or into the returned bytes when out is None."""
    proc = subprocess.Popen(
        ["zstd", "-q", *ZSTD_ARGS, f"--stream-size={frame.raw}", "-c"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE if out is None else out,
    )
    assert proc.stdin is not None
    errors: list[BaseException] = []
    feeder = threading.Thread(target=_feed, args=(proc.stdin, frame, errors))
    feeder.start()
    data = proc.stdout.read() if proc.stdout is not None else b""
    feeder.join()
    if errors:
        proc.wait()
        raise errors[0]
    if proc.wait():
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
    return data


def _feed(pipe: Any, frame: _Frame, errors: list[BaseException]) -> None:
    try:
        with pipe:
            pipe.write(frame.prefix)
            for m in frame.members:
                pipe.write(member_header(m.checksum, m.size))
                if isinstance(m.source, Path):
                    _copy_source(m.source, m, pipe)
                else:
                    pipe.write(m.source)
                pipe.write(bytes(_padding(m.size)))
    except BaseException as e:
        errors.append(e)


def _copy_source(path: Path, m: _Member, pipe: Any) -> None:
    with open(path, "rb") as f:
        left = m.size
        while left and (chunk := f.read(min(left, MB))):
            pipe.write(chunk)
            left -= len(chunk)
        st = os.fstat(f.fileno())
    if left or st.st_size != m.size or m.mtime_ns not in (None, st.st_mtime_ns):
        raise SourceChanged(path)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def _copy_range(src: Path, offset: int, length: int, fd: int) -> None:
    with open(src, "rb") as f:
        while length:
            sent = os.sendfile(fd, f.fileno(), offset, length)
            if not sent:
                raise EOFError(f"{src} ends before byte {offset + length}")
            offset += sent
            length -= sent


def _place_file(src: Path, dst: Path, end: int, how: Adopt) -> None:
    """Put src's first `end` bytes at dst: by rename, hard link, or copy."""
    if how == "move":
        os.replace(src, dst)
        if dst.stat().st_size > end:
            os.truncate(dst, end)
    elif how == "link" and src.stat().st_size == end:
        os.link(src, dst)
    else:
        fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _TAR_MODE)
        try:
            _copy_range(src, 0, end, fd)
        finally:
            os.close(fd)
