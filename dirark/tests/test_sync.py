"""Tests for dirark.sync: merge_arks, push_ark, pull_ark, add_dir_to_remote_ark."""

import tempfile
import unittest
from pathlib import Path

from dirark.core import archive_dir, restore_ark
from dirark.storage import ARK_DIR_EXT, DB_NAME, open_db
from dirark.sync import add_dir_to_remote_ark, merge_arks, pull_ark, push_ark


class TestMergeArks(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)

        src_a = self.tmp / "a"
        src_a.mkdir()
        (src_a / "file_a.txt").write_text("content a")
        archive_dir(src_a)
        self.ark_a = self.tmp / ("a" + ARK_DIR_EXT)

        src_b = self.tmp / "b"
        src_b.mkdir()
        (src_b / "file_b.txt").write_text("content b")
        archive_dir(src_b)
        self.ark_b = self.tmp / ("b" + ARK_DIR_EXT)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_merge_adds_missing_files(self) -> None:
        merge_arks(self.ark_b, self.ark_a)
        db = open_db(self.ark_a / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT path FROM files ORDER BY path")
        paths = [row[0] for row in cur.fetchall()]
        db.close()
        self.assertIn("file_a.txt", paths)
        self.assertIn("file_b.txt", paths)

    def test_merged_ark_restores_all_files(self) -> None:
        merge_arks(self.ark_b, self.ark_a)
        dest = self.tmp / "restored"
        restore_ark(self.ark_a, dest)
        self.assertTrue((dest / "file_a.txt").exists())
        self.assertTrue((dest / "file_b.txt").exists())
        self.assertEqual((dest / "file_b.txt").read_text(), "content b")

    def test_merge_deduplicates_objects(self) -> None:
        src_c = self.tmp / "c"
        src_c.mkdir()
        (src_c / "dup.txt").write_text("content a")
        archive_dir(src_c)
        ark_c = self.tmp / ("c" + ARK_DIR_EXT)

        merge_arks(ark_c, self.ark_a)
        db = open_db(self.ark_a / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        n_files = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM objects")
        n_objects = cur.fetchone()[0]
        db.close()
        self.assertEqual(n_files, 2)
        self.assertEqual(n_objects, 1)

    def test_merge_is_idempotent(self) -> None:
        merge_arks(self.ark_b, self.ark_a)
        merge_arks(self.ark_b, self.ark_a)
        db = open_db(self.ark_a / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        self.assertEqual(cur.fetchone()[0], 2)
        db.close()


class TestPushPullArk(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        src = self.tmp / "source"
        src.mkdir()
        (src / "file1.txt").write_text("pushed content")
        archive_dir(src)
        self.local_ark = self.tmp / ("source" + ARK_DIR_EXT)
        self.remote = self.tmp / "remote_ark"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_push_copies_files(self) -> None:
        push_ark(self.local_ark, str(self.remote))
        self.assertTrue((self.remote / DB_NAME).exists())
        tars = list(self.remote.glob("*.tar.zst"))
        self.assertGreater(len(tars), 0)

    def test_pull_copies_files(self) -> None:
        push_ark(self.local_ark, str(self.remote))
        dest = self.tmp / "pulled"
        pull_ark(str(self.remote), dest)
        self.assertTrue((dest / DB_NAME).exists())

    def test_push_then_restore(self) -> None:
        push_ark(self.local_ark, str(self.remote))
        dest = self.tmp / "restored"
        restore_ark(self.remote, dest)
        self.assertTrue((dest / "file1.txt").exists())
        self.assertEqual((dest / "file1.txt").read_text(), "pushed content")


class TestAddDirToRemoteArk(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)

        remote_src = self.tmp / "remote_src"
        remote_src.mkdir()
        (remote_src / "existing.txt").write_text("existing content")
        archive_dir(remote_src)
        self.remote_ark = self.tmp / ("remote_src" + ARK_DIR_EXT)

        self.new_src = self.tmp / "new_content"
        self.new_src.mkdir()
        (self.new_src / "new.txt").write_text("new content")

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_add_dir_merges_into_remote(self) -> None:
        add_dir_to_remote_ark(self.new_src, str(self.remote_ark))
        dest = self.tmp / "restored"
        restore_ark(self.remote_ark, dest)
        self.assertTrue((dest / "existing.txt").exists())
        self.assertTrue((dest / "new.txt").exists())
        self.assertEqual((dest / "new.txt").read_text(), "new content")

    def test_add_dir_preserves_existing_content(self) -> None:
        add_dir_to_remote_ark(self.new_src, str(self.remote_ark))
        dest = self.tmp / "restored"
        restore_ark(self.remote_ark, dest)
        self.assertEqual((dest / "existing.txt").read_text(), "existing content")
