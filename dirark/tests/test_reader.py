"""Tests for dirark.reader.ArkReader."""

import tempfile
import unittest
from pathlib import Path

from dirark.core import archive_dir
from dirark.reader import ArkReader
from dirark.storage import ARK_DIR_EXT


class TestArkReader(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.src = self.tmp / "source"
        self.ark = self.tmp / ("source" + ARK_DIR_EXT)
        self.src.mkdir()
        (self.src / "hello.txt").write_text("hello world")
        (self.src / "sub").mkdir()
        (self.src / "sub" / "data.bin").write_bytes(b"\x00\x01\x02\x03")
        (self.src / "empty.txt").touch()
        archive_dir(self.src)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_list_files(self) -> None:
        with ArkReader(self.ark) as r:
            files = r.list_files()
        self.assertEqual(files, ["empty.txt", "hello.txt", "sub/data.bin"])

    def test_get_checksum_returns_string(self) -> None:
        with ArkReader(self.ark) as r:
            cs = r.get_checksum("hello.txt")
        self.assertIsInstance(cs, str)
        self.assertEqual(len(cs), 128)

    def test_read_file_text(self) -> None:
        with ArkReader(self.ark) as r:
            data = r.read_file("hello.txt")
        self.assertEqual(data, b"hello world")

    def test_read_file_binary(self) -> None:
        with ArkReader(self.ark) as r:
            data = r.read_file("sub/data.bin")
        self.assertEqual(data, b"\x00\x01\x02\x03")

    def test_read_empty_file(self) -> None:
        with ArkReader(self.ark) as r:
            data = r.read_file("empty.txt")
        self.assertEqual(data, b"")

    def test_missing_file_raises_key_error(self) -> None:
        with ArkReader(self.ark) as r:
            with self.assertRaises(KeyError):
                r.read_file("does_not_exist.txt")

    def test_get_checksum_missing_raises_key_error(self) -> None:
        with ArkReader(self.ark) as r:
            with self.assertRaises(KeyError):
                r.get_checksum("does_not_exist.txt")

    def test_context_manager_closes(self) -> None:
        r = ArkReader(self.ark)
        with r:
            _ = r.list_files()
        with self.assertRaises(Exception):
            r.list_files()
