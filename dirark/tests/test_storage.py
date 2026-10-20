"""Tests for dirark.storage primitives."""

import hashlib
import os
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from dirark.storage import (
    ARK_DIR_EXT,
    DB_NAME,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    MB,
    TAR_EXT,
    TAR_PREFIX,
    ArkFormatError,
    FrameCache,
    Packer,
    SourceChanged,
    digest_bytes,
    digest_file,
    ensure_clean_outdir,
    list_tars,
    locate,
    open_db,
    read_span,
    tar_name,
)


def _blob(i: int, size: int) -> bytes:
    return bytes([i]) * size


class TestPrimitives(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_digest_file_is_blake2b_512(self) -> None:
        f = self.tmp / "a.txt"
        f.write_text("hello")
        self.assertEqual(digest_file(f), hashlib.blake2b(b"hello").digest())
        self.assertEqual(digest_file(f), digest_bytes(b"hello"))

    def test_open_db_creates_schema(self) -> None:
        db = open_db(self.tmp / DB_NAME)
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master")}
        db.close()
        self.assertLessEqual({"files", "frames", "objects"}, tables)

    def test_open_db_refuses_a_format_1_index(self) -> None:
        db = sqlite3.connect(self.tmp / DB_NAME)
        db.execute("CREATE TABLE objects(checksum TEXT PRIMARY KEY, tar_name TEXT)")
        db.close()
        with self.assertRaisesRegex(ArkFormatError, "dirark migrate"):
            open_db(self.tmp / DB_NAME)

    def test_list_tars_sorted(self) -> None:
        for idx in (2, 1):
            (self.tmp / tar_name(idx)).touch()
        self.assertEqual(
            [t.name for t in list_tars(self.tmp)], [tar_name(1), tar_name(2)]
        )

    def test_ensure_clean_outdir_passes(self) -> None:
        ark = self.tmp / f"src{ARK_DIR_EXT}"
        ark.mkdir()
        (ark / DB_NAME).touch()
        (ark / f"{TAR_PREFIX}00001{TAR_EXT}").touch()
        ensure_clean_outdir(ark)

    def test_ensure_clean_outdir_raises_on_unexpected(self) -> None:
        ark = self.tmp / f"src{ARK_DIR_EXT}"
        ark.mkdir()
        (ark / "unexpected.txt").touch()
        with self.assertRaises(RuntimeError):
            ensure_clean_outdir(ark)


class TestPacker(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.ark = self.tmp / "ark"
        self.ark.mkdir()
        self.db = open_db(self.ark / DB_NAME)

    def tearDown(self) -> None:
        self.db.close()
        self.tmpdir.cleanup()

    def _pack(
        self,
        blobs: list[bytes],
        max_frame_mb: int = MAX_FRAME_MB,
        max_tar_mb: int = MAX_TAR_MB,
    ) -> list[bytes]:
        checksums = [digest_bytes(b) for b in blobs]
        with Packer(
            self.ark, self.db, max_frame_mb=max_frame_mb, max_tar_mb=max_tar_mb
        ) as pack:
            for cs, blob in zip(checksums, blobs):
                pack.add(cs, blob, len(blob))
            pack.commit()
        return checksums

    def _read(self, checksum: bytes) -> bytes:
        loc = locate(self.db, checksum)
        assert loc is not None
        return FrameCache().read(self.ark, loc)

    def _count(self, table: str) -> int:
        return self.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]

    def test_objects_read_back(self) -> None:
        blobs = [b"one", b"", b"three" * 1000]
        for cs, blob in zip(self._pack(blobs), blobs):
            self.assertEqual(self._read(cs), blob)

    def test_frames_hold_at_most_max_frame_mb(self) -> None:
        self._pack([_blob(i, MB * 2 // 5) for i in range(3)], max_frame_mb=1)
        self.assertEqual(self._count("frames"), 2)

    def test_an_oversized_object_gets_its_own_frame(self) -> None:
        big = os.urandom(MB * 3 // 2)
        src = self.tmp / "big.bin"
        src.write_bytes(big)
        with Packer(self.ark, self.db, max_frame_mb=1) as pack:
            pack.add(digest_bytes(b"small"), b"small", 5)
            pack.add(digest_bytes(big), src, len(big))
            pack.commit()
        self.assertEqual(self._count("frames"), 2)
        self.assertEqual(self._read(digest_bytes(big)), big)

    def test_tars_hold_at_most_max_tar_mb(self) -> None:
        self._pack(
            [_blob(i, MB * 3 // 5) for i in range(3)], max_frame_mb=1, max_tar_mb=1
        )
        self.assertEqual(len(list_tars(self.ark)), 3)

    def test_plain_tar_reads_the_objects(self) -> None:
        blobs = [_blob(i, MB * 2 // 5) for i in range(3)]
        checksums = self._pack(blobs, max_frame_mb=1)
        out = self.tmp / "out"
        out.mkdir()
        for tar in list_tars(self.ark):
            subprocess.run(
                ["tar", "-I", "zstd", "-xf", str(tar), "-C", str(out)], check=True
            )
        for cs, blob in zip(checksums, blobs):
            self.assertEqual((out / "objects" / cs.hex()).read_bytes(), blob)

    def test_a_partial_frame_is_reopened_by_the_next_commit(self) -> None:
        first = self._pack([b"first"])
        second = self._pack([b"second"])
        self.assertEqual(self._count("frames"), 1)
        self.assertEqual(len(list_tars(self.ark)), 1)
        self.assertEqual(self._read(first[0]), b"first")
        self.assertEqual(self._read(second[0]), b"second")

    def test_tidy_cuts_crash_leftovers_and_deletes_unindexed_tars(self) -> None:
        (cs,) = self._pack([b"kept"])
        (tar,) = list_tars(self.ark)
        size = tar.stat().st_size
        with open(tar, "ab") as f:
            f.write(b"half-written frame")
        orphan = self.ark / tar_name(99)
        orphan.write_bytes(b"never indexed")
        Packer(self.ark, self.db).__exit__()
        self.assertEqual(tar.stat().st_size, size)
        self.assertFalse(orphan.exists())
        self.assertEqual(self._read(cs), b"kept")

    def test_a_source_changed_after_hashing_fails_loudly(self) -> None:
        src = self.tmp / "f.txt"
        src.write_text("before")
        st = src.stat()
        with Packer(self.ark, self.db) as pack:
            pack.add(digest_file(src), src, st.st_size, st.st_mtime_ns - 1)
            with self.assertRaises(SourceChanged):
                pack.commit()

    def test_frame_cache_decodes_a_frame_once(self) -> None:
        a, b = self._pack([b"a", b"b"])
        cache = FrameCache()
        with mock.patch("dirark.storage.read_span", wraps=read_span) as decode:
            for cs in (a, b, a):
                loc = locate(self.db, cs)
                assert loc is not None
                cache.read(self.ark, loc)
        self.assertEqual(decode.call_count, 1)
