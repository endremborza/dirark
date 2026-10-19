"""Tests for dirark.storage primitives."""

import tempfile
import unittest
from pathlib import Path

from dirark.storage import (
    ARK_DIR_EXT,
    DB_NAME,
    TAR_EXT,
    TAR_PREFIX,
    b2sum,
    create_tar_zst,
    ensure_clean_outdir,
    extract_tar_zst,
    list_tars,
    next_tar_path,
    open_db,
    write_objects_to_tars,
)


class TestStoragePrimitives(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.src = self.tmp / "src"
        self.src.mkdir()
        (self.src / "a.txt").write_text("hello")
        (self.src / "sub").mkdir()
        (self.src / "sub" / "b.txt").write_text("world")

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_b2sum_returns_128_hex_chars(self) -> None:
        cs = b2sum(self.src / "a.txt")
        self.assertIsInstance(cs, str)
        self.assertEqual(len(cs), 128)

    def test_b2sum_is_deterministic(self) -> None:
        self.assertEqual(b2sum(self.src / "a.txt"), b2sum(self.src / "a.txt"))

    def test_b2sum_differs_for_different_content(self) -> None:
        self.assertNotEqual(
            b2sum(self.src / "a.txt"), b2sum(self.src / "sub" / "b.txt")
        )

    def test_open_db_creates_schema(self) -> None:
        db = open_db(self.tmp / DB_NAME)
        cur = db.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        self.assertIn("files", tables)
        self.assertIn("objects", tables)
        db.close()

    def test_create_and_extract_tar_zst(self) -> None:
        tar = self.tmp / f"test{TAR_EXT}"
        create_tar_zst(self.src, tar)
        self.assertTrue(tar.exists())

        out = self.tmp / "extracted"
        out.mkdir()
        extract_tar_zst(tar, out)
        self.assertTrue((out / "a.txt").exists())
        self.assertEqual((out / "a.txt").read_text(), "hello")

    def test_list_tars_sorted(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        (ark / f"{TAR_PREFIX}00002{TAR_EXT}").touch()
        (ark / f"{TAR_PREFIX}00001{TAR_EXT}").touch()
        tars = list_tars(ark)
        names = [t.name for t in tars]
        self.assertEqual(names, sorted(names))

    def test_next_tar_path_first(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        path = next_tar_path(ark)
        self.assertEqual(path.name, f"{TAR_PREFIX}00001{TAR_EXT}")

    def test_next_tar_path_increments(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        (ark / f"{TAR_PREFIX}00003{TAR_EXT}").touch()
        path = next_tar_path(ark)
        self.assertEqual(path.name, f"{TAR_PREFIX}00004{TAR_EXT}")

    def test_next_tar_path_min_idx(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        path = next_tar_path(ark, min_idx=5)
        self.assertEqual(path.name, f"{TAR_PREFIX}00005{TAR_EXT}")

    def test_ensure_clean_outdir_passes(self) -> None:
        ark = self.tmp / f"src{ARK_DIR_EXT}"
        ark.mkdir()
        (ark / DB_NAME).touch()
        (ark / f"{TAR_PREFIX}00001{TAR_EXT}").touch()
        ensure_clean_outdir(ark)  # should not raise

    def test_ensure_clean_outdir_raises_on_unexpected(self) -> None:
        ark = self.tmp / f"src{ARK_DIR_EXT}"
        ark.mkdir()
        (ark / "unexpected.txt").touch()
        with self.assertRaises(RuntimeError):
            ensure_clean_outdir(ark)

    def test_write_objects_to_tars(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        open_db(ark / DB_NAME).close()
        cs = b2sum(self.src / "a.txt")
        written = write_objects_to_tars(ark, {cs: self.src / "a.txt"})
        self.assertEqual(set(written), {cs})
        self.assertTrue((ark / written[cs]).exists())
        self.assertEqual(written[cs], f"{TAR_PREFIX}00001{TAR_EXT}")

    def test_write_objects_to_tars_rolls_over_at_cap(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        open_db(ark / DB_NAME).close()
        objects = {}
        for i in range(3):
            f = self.src / f"big{i}.bin"
            f.write_bytes(bytes([i]) * (1024 * 1024 + 1))
            objects[b2sum(f)] = f
        written = write_objects_to_tars(ark, objects, max_tar_mb=1)
        self.assertEqual(len(list_tars(ark)), 3)
        self.assertEqual(len(set(written.values())), 3)
        for tar_name in written.values():
            self.assertTrue((ark / tar_name).exists())
        self.assertFalse(list(ark.parent.glob(".ark.ark.d-staging-*")))

    def test_coalescing_counts_source_bytes_not_compressed_size(self) -> None:
        ark = self.tmp / "ark.ark.d"
        ark.mkdir()
        tars = []
        for i in range(3):
            f = self.src / f"compressible{i}.bin"
            f.write_bytes(bytes([i]) * (1024 * 1024 * 2 // 5))
            tars += write_objects_to_tars(ark, {b2sum(f): f}, max_tar_mb=1).values()
        self.assertEqual(tars[0], tars[1])
        self.assertNotEqual(tars[1], tars[2])
