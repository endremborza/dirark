"""Tests for dirark.core: archive_dir and restore_ark."""

import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from dirark.core import archive_dir, restore_ark
from dirark.storage import ARK_DIR_EXT, DB_NAME, TAR_EXT, TAR_PREFIX, open_db


class TestArchiveDir(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.src = self.tmp / "source"
        self.ark = self.tmp / ("source" + ARK_DIR_EXT)
        self.src.mkdir()
        (self.src / "file1.txt").write_text("content of file1")
        (self.src / "subdir").mkdir()
        (self.src / "subdir" / "file2.txt").write_text("content of file2")
        (self.src / "empty_file.txt").touch()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_initial_archive_creates_structure(self) -> None:
        archive_dir(self.src)
        self.assertTrue(self.ark.exists())
        self.assertTrue((self.ark / DB_NAME).exists())
        tars = list(self.ark.glob(f"{TAR_PREFIX}*{TAR_EXT}"))
        self.assertGreater(len(tars), 0)

    def test_initial_archive_records_all_files(self) -> None:
        archive_dir(self.src)
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT path FROM files ORDER BY path")
        paths = [row[0] for row in cur.fetchall()]
        db.close()
        self.assertEqual(paths, ["empty_file.txt", "file1.txt", "subdir/file2.txt"])

    def test_initial_archive_deduplicates_objects(self) -> None:
        archive_dir(self.src)
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM objects")
        n_objects = cur.fetchone()[0]
        db.close()
        self.assertGreaterEqual(n_objects, 2)

    def test_incremental_archive_adds_new_files(self) -> None:
        archive_dir(self.src)
        (self.src / "new.txt").write_text("new content")
        (self.src / "sub2").mkdir()
        (self.src / "sub2" / "more.txt").write_text("more content")
        archive_dir(self.src)

        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        self.assertEqual(cur.fetchone()[0], 5)
        db.close()

    def test_idempotent_archiving(self) -> None:
        archive_dir(self.src)
        archive_dir(self.src)
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        self.assertEqual(cur.fetchone()[0], 3)
        db.close()

    def test_deduplication_across_paths(self) -> None:
        (self.src / "dup.txt").write_text("content of file1")
        archive_dir(self.src)
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        n_files = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM objects")
        n_objects = cur.fetchone()[0]
        db.close()
        self.assertEqual(n_files, 4)
        self.assertLess(n_objects, n_files)

    def test_archive_into_existing_ark(self) -> None:
        self.ark.mkdir()
        archive_dir(self.src, ark_out=self.ark)
        db = open_db(self.ark / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        self.assertEqual(cur.fetchone()[0], 3)
        db.close()


class TestRestoreArk(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.src = self.tmp / "source"
        self.ark = self.tmp / ("source" + ARK_DIR_EXT)
        self.dest = self.tmp / "restored"
        self.src.mkdir()
        (self.src / "file1.txt").write_text("content of file1")
        (self.src / "subdir").mkdir()
        (self.src / "subdir" / "file2.txt").write_text("content of file2")
        (self.src / "empty_file.txt").touch()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_restore_recreates_files(self) -> None:
        archive_dir(self.src)
        restore_ark(self.ark, self.dest)
        self.assertTrue((self.dest / "file1.txt").exists())
        self.assertTrue((self.dest / "subdir" / "file2.txt").exists())
        self.assertTrue((self.dest / "empty_file.txt").exists())

    def test_restore_preserves_content(self) -> None:
        archive_dir(self.src)
        restore_ark(self.ark, self.dest)
        self.assertEqual((self.dest / "file1.txt").read_text(), "content of file1")
        self.assertEqual(
            (self.dest / "subdir" / "file2.txt").read_text(),
            "content of file2",
        )
        self.assertEqual((self.dest / "empty_file.txt").read_text(), "")

    def test_restore_includes_incrementally_added_files(self) -> None:
        archive_dir(self.src)
        (self.src / "new.txt").write_text("new content")
        archive_dir(self.src)
        restore_ark(self.ark, self.dest)
        self.assertTrue((self.dest / "new.txt").exists())
        self.assertEqual((self.dest / "new.txt").read_text(), "new content")

    def test_restore_empty_archive_skips_dest_creation(self) -> None:
        self.ark.mkdir()
        open_db(self.ark / DB_NAME).close()
        with open("/dev/null", "w") as f, redirect_stdout(f):
            restore_ark(self.ark, self.dest)
        self.assertFalse(self.dest.exists())


class TestCLI(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.src = self.tmp / "source"
        self.ark = self.tmp / ("source" + ARK_DIR_EXT)
        self.dest = self.tmp / "restored"
        self.src.mkdir()
        (self.src / "file1.txt").write_text("content of file1")
        (self.src / "subdir").mkdir()
        (self.src / "subdir" / "file2.txt").write_text("content of file2")

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "dirark", *args],
            capture_output=True,
            text=True,
        )

    def test_archive_command(self) -> None:
        result = self._run("archive", str(self.src))
        self.assertEqual(result.returncode, 0)
        self.assertIn("Archived", result.stdout)
        self.assertTrue((self.ark / DB_NAME).exists())

    def test_restore_command(self) -> None:
        self._run("archive", str(self.src))
        result = self._run("restore", str(self.ark), str(self.dest))
        self.assertEqual(result.returncode, 0)
        self.assertIn("Restored", result.stdout)
        self.assertTrue((self.dest / "file1.txt").exists())
        self.assertEqual((self.dest / "file1.txt").read_text(), "content of file1")

    def test_restore_empty_archive(self) -> None:
        self.ark.mkdir()
        open_db(self.ark / DB_NAME).close()
        result = self._run("restore", str(self.ark), str(self.dest))
        self.assertEqual(result.returncode, 0)
        self.assertIn("No files found", result.stdout)
        self.assertFalse(self.dest.exists())

    def test_read_command(self) -> None:
        self._run("archive", str(self.src))
        result = subprocess.run(
            [sys.executable, "-m", "dirark", "read", str(self.ark), "file1.txt"],
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"content of file1")

    def test_read_missing_file_exits_nonzero(self) -> None:
        self._run("archive", str(self.src))
        result = self._run("read", str(self.ark), "nonexistent.txt")
        self.assertNotEqual(result.returncode, 0)
