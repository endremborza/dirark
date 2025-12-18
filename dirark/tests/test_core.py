import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from dirark.core import (
    ARK_DIR_EXT,
    DB_NAME,
    TAR_EXT,
    TAR_PREFIX,
    archive_dir,
    b2sum,
    create_tar_zst,
    extract_tar_zst,
    open_db,
    restore_ark,
)


class TestDirarkCore(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.source_dir = self.tmp_path / "source"
        self.archive_repo_dir = self.tmp_path / ("source" + ARK_DIR_EXT)
        self.restore_dir = self.tmp_path / "restored"

        # Create some dummy files for testing
        self.source_dir.mkdir()
        (self.source_dir / "file1.txt").write_text("content of file1")
        (self.source_dir / "subdir").mkdir()
        (self.source_dir / "subdir" / "file2.txt").write_text("content of file2")
        (self.source_dir / "empty_file.txt").touch()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_b2sum(self):
        file_path = self.source_dir / "file1.txt"
        checksum = b2sum(file_path)
        self.assertIsInstance(checksum, str)
        self.assertEqual(len(checksum), 128)  # Blake2b checksum length

    def test_create_and_extract_tar_zst(self):
        test_tar_path = self.tmp_path / f"test{TAR_EXT}"
        create_tar_zst(self.source_dir, test_tar_path)
        self.assertTrue(test_tar_path.exists())

        extract_dest = self.tmp_path / "extracted_tar"
        extract_dest.mkdir()
        extract_tar_zst(test_tar_path, extract_dest)

        self.assertTrue((extract_dest / "file1.txt").exists())
        self.assertTrue((extract_dest / "subdir" / "file2.txt").exists())
        self.assertEqual((extract_dest / "file1.txt").read_text(), "content of file1")

    def test_archive_dir_initial_archive(self):
        archive_dir(self.source_dir)

        self.assertTrue(self.archive_repo_dir.exists())
        self.assertTrue((self.archive_repo_dir / DB_NAME).exists())
        tars = list(self.archive_repo_dir.glob(f"{TAR_PREFIX}*{TAR_EXT}"))
        self.assertGreater(len(tars), 0)

        db = open_db(self.archive_repo_dir / DB_NAME)
        cur = db.cursor()

        # Check files table
        cur.execute("SELECT path, checksum FROM files ORDER BY path")
        files = cur.fetchall()
        self.assertEqual(len(files), 3)
        self.assertEqual(files[0][0], "empty_file.txt")
        self.assertEqual(files[1][0], "file1.txt")
        self.assertEqual(files[2][0], "subdir/file2.txt")

        # Check objects table
        cur.execute("SELECT checksum, tar_name FROM objects")
        objects = cur.fetchall()
        self.assertGreaterEqual(len(objects), 2)  # At least two unique files
        db.close()

    def test_archive_dir_add_new_files(self):
        # Initial archive
        archive_dir(self.source_dir)

        # Add new file to source
        (self.source_dir / "new_file.txt").write_text("new content")
        (self.source_dir / "another_subdir").mkdir()
        (self.source_dir / "another_subdir" / "file3.txt").write_text("content of file3")

        # Merge again
        archive_dir(self.source_dir)

        db = open_db(self.archive_repo_dir / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM files")
        self.assertEqual(cur.fetchone()[0], 5)  # 3 original + 2 new
        db.close()

    def test_restore_ark(self):
        # First, archive some files
        archive_dir(self.source_dir)

        # Now, restore them to a new directory
        restore_ark(self.archive_repo_dir, self.restore_dir)

        self.assertTrue(self.restore_dir.exists())
        self.assertTrue((self.restore_dir / "file1.txt").exists())
        self.assertTrue((self.restore_dir / "subdir" / "file2.txt").exists())
        self.assertTrue((self.restore_dir / "empty_file.txt").exists())

        self.assertEqual((self.restore_dir / "file1.txt").read_text(), "content of file1")
        self.assertEqual(
            (self.restore_dir / "subdir" / "file2.txt").read_text(),
            "content of file2",
        )
        self.assertEqual((self.restore_dir / "empty_file.txt").read_text(), "")

    def test_restore_ark_with_new_files_added(self):
        # Archive initial files
        archive_dir(self.source_dir)

        # Add new files and re-archive
        (self.source_dir / "new_file.txt").write_text("new content")
        archive_dir(self.source_dir)

        # Clear restore directory to ensure fresh restore
        shutil.rmtree(self.restore_dir, ignore_errors=True)
        self.restore_dir.mkdir()

        # Restore
        restore_ark(self.archive_repo_dir, self.restore_dir)

        self.assertTrue((self.restore_dir / "file1.txt").exists())
        self.assertTrue((self.restore_dir / "subdir" / "file2.txt").exists())
        self.assertTrue((self.restore_dir / "empty_file.txt").exists())
        self.assertTrue((self.restore_dir / "new_file.txt").exists())

        self.assertEqual((self.restore_dir / "new_file.txt").read_text(), "new content")

    def test_restore_ark_empty_archive(self):
        # Create an empty archive repo
        self.archive_repo_dir.mkdir()
        open_db(self.archive_repo_dir / DB_NAME).close()

        # Try to restore
        captured_output = sys.stdout
        sys.stdout = self._io_object = open("/dev/null", "w")
        restore_ark(self.archive_repo_dir, self.restore_dir)
        sys.stdout = captured_output

        self.assertFalse(self.restore_dir.exists())  # Should not create if nothing to restore

    def test_cli_archive_command(self):
        # Using subprocess to test the CLI
        cmd = [
            sys.executable,
            "-m",
            "dirark",
            "archive",
            str(self.source_dir),
            str(self.archive_repo_dir),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        self.assertIn("Successfully archived", result.stdout)
        self.assertTrue((self.archive_repo_dir / DB_NAME).exists())

    def test_cli_restore_command(self):
        # First, archive using CLI
        archive_cmd = [
            sys.executable,
            "-m",
            "dirark",
            "archive",
            str(self.source_dir),
            str(self.archive_repo_dir),
        ]
        subprocess.run(archive_cmd, check=True)

        # Then, restore using CLI
        restore_cmd = [
            sys.executable,
            "-m",
            "dirark",
            "restore",
            str(self.archive_repo_dir),
            str(self.restore_dir),
        ]
        result = subprocess.run(restore_cmd, capture_output=True, text=True, check=True)
        self.assertIn("Successfully restored", result.stdout)
        self.assertTrue((self.restore_dir / "file1.txt").exists())
        self.assertEqual((self.restore_dir / "file1.txt").read_text(), "content of file1")

    def test_cli_restore_command_empty_archive(self):
        self.archive_repo_dir.mkdir()
        open_db(self.archive_repo_dir / DB_NAME).close()

        restore_cmd = [
            sys.executable,
            "-m",
            "dirark",
            "restore",
            str(self.archive_repo_dir),
            str(self.restore_dir),
        ]
        result = subprocess.run(restore_cmd, capture_output=True, text=True, check=True)
        self.assertIn("No files found to restore in the archive.", result.stdout)
        self.assertFalse(self.restore_dir.exists())
