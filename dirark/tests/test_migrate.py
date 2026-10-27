"""Tests for dirark.migrate: format-1 arks re-packed into the current format."""

import hashlib
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import chdir, closing
from pathlib import Path

from dirark.blob import FRAME_MB, BlobStore
from dirark.core import restore_ark
from dirark.migrate import MigrationError, _migrate_remote, migrate_ark
from dirark.reader import ArkReader
from dirark.storage import (
    DB_NAME,
    MAX_FRAME_MB,
    MAX_TAR_MB,
    SCHEMA_VERSION,
    ArkFormatError,
    list_tars,
)


def _format_1_ark(ark: Path, tars: list[dict[str, bytes]]) -> None:
    """Write a format-1 ark: hex index, one single-stream tar per dict of files.

    An empty path stores its content without a file row, as a BlobStore does.
    """
    ark.mkdir(parents=True)
    with closing(sqlite3.connect(ark / DB_NAME)) as db:
        db.execute("CREATE TABLE files(path TEXT PRIMARY KEY, checksum TEXT NOT NULL)")
        db.execute(
            "CREATE TABLE objects(checksum TEXT PRIMARY KEY, tar_name TEXT NOT NULL)"
        )
        for i, files in enumerate(tars, 1):
            name = f"data-{i:05d}.tar.zst"
            stage = ark.parent / f"stage{i}"
            (stage / "objects").mkdir(parents=True)
            for path, content in files.items():
                cs = hashlib.blake2b(content).hexdigest()
                (stage / "objects" / cs).write_bytes(content)
                db.execute("INSERT OR IGNORE INTO objects VALUES (?, ?)", (cs, name))
                if path:
                    db.execute("INSERT INTO files VALUES (?, ?)", (path, cs))
            subprocess.run(
                ["tar", "-C", str(stage), "-I", "zstd", "-cf", str(ark / name), "."],
                check=True,
            )
        db.commit()


_FILES = [{"a.txt": b"alpha", "sub/b.txt": b"beta"}, {"c.txt": b"gamma"}]


class TestMigrate(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.ark = self.tmp / "old.ark.d"
        _format_1_ark(self.ark, _FILES)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _assert_migrated(self, ark: Path) -> None:
        with closing(sqlite3.connect(ark / DB_NAME)) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
        self.assertEqual(version, SCHEMA_VERSION)
        with ArkReader(ark) as r:
            for files in _FILES:
                for path, content in files.items():
                    self.assertEqual(r.read_file(path), content)

    def test_local_ark_is_repacked_in_place(self) -> None:
        old_tars = {t.name for t in list_tars(self.ark)}
        migrate_ark(str(self.ark))
        self._assert_migrated(self.ark)
        self.assertFalse(old_tars & {t.name for t in list_tars(self.ark)})
        self.assertEqual(
            sorted(p.name for p in self.tmp.iterdir() if p.name.startswith(".")), []
        )
        dest = self.tmp / "restored"
        restore_ark(self.ark, dest)
        self.assertEqual((dest / "sub" / "b.txt").read_bytes(), b"beta")

    def test_a_migrated_ark_is_left_alone(self) -> None:
        migrate_ark(str(self.ark))
        before = {t.name: t.read_bytes() for t in list_tars(self.ark)}
        migrate_ark(str(self.ark))
        self.assertEqual({t.name: t.read_bytes() for t in list_tars(self.ark)}, before)

    def test_a_work_dir_holds_the_scratch_and_is_left_empty(self) -> None:
        work = self.tmp / "work"
        work.mkdir()
        migrate_ark(str(self.ark), work=work)
        self._assert_migrated(self.ark)
        self.assertEqual(list(work.iterdir()), [])
        self.assertEqual(
            sorted(p.name for p in self.ark.iterdir() if p.name.startswith(".")), []
        )

    def test_remote_ark_is_repacked_over_rsync(self) -> None:
        with chdir(self.tmp):
            _migrate_remote(str(self.ark), MAX_TAR_MB, MAX_FRAME_MB, self.tmp)
        self._assert_migrated(self.ark)
        self.assertEqual(len(list_tars(self.ark)), 1)

    def _assert_refused(self) -> None:
        before = {p.name: p.read_bytes() for p in self.ark.iterdir()}
        with self.assertRaises(MigrationError):
            migrate_ark(str(self.ark))
        self.assertEqual({p.name: p.read_bytes() for p in self.ark.iterdir()}, before)
        self.assertEqual(
            sorted(p.name for p in self.tmp.iterdir() if p.name.startswith(".")), []
        )

    def test_a_missing_tar_is_refused(self) -> None:
        list_tars(self.ark)[-1].unlink()
        self._assert_refused()

    def test_a_path_without_content_is_refused(self) -> None:
        with closing(sqlite3.connect(self.ark / DB_NAME)) as db:
            db.execute("INSERT INTO files VALUES ('ghost.txt', ?)", ("ab" * 64,))
            db.commit()
        self._assert_refused()

    def test_a_corrupt_object_is_refused(self) -> None:
        tar = list_tars(self.ark)[-1]
        stage = self.tmp / "unpacked"
        stage.mkdir()
        subprocess.run(["tar", "-I", "zstd", "-xf", tar, "-C", stage], check=True)
        (obj,) = (stage / "objects").iterdir()
        obj.write_bytes(b"rot")
        subprocess.run(["tar", "-C", stage, "-I", "zstd", "-cf", tar, "."], check=True)
        self._assert_refused()

    def test_blob_store_reads_after_migration(self) -> None:
        root = self.tmp / "store"
        _format_1_ark(root / "ark", [{"": b"blob bytes"}])
        (root / "staging").mkdir()
        with self.assertRaises(ArkFormatError):
            BlobStore(root).has("00" * 64)
        migrate_ark(str(root / "ark"), max_frame_mb=FRAME_MB)
        store = BlobStore(root)
        self.assertEqual(
            store.read(hashlib.blake2b(b"blob bytes").hexdigest()), b"blob bytes"
        )
