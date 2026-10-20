"""Tests for dirark.BlobStore: content-addressed bytes storage."""

import hashlib
import multiprocessing as mp
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from dirark.blob import BlobStore, blob_checksum
from dirark.storage import (
    DB_NAME,
    MB,
    ArkFormatError,
    list_tars,
    locate,
    open_db,
    read_span,
)

_TEST_FRAME_MB = 1
_OWN_FRAME = _TEST_FRAME_MB * MB // 2 + 1
_THIRD_FRAME = _TEST_FRAME_MB * MB // 3 - MB // 64


def _blobs(n: int, size: int) -> list[bytes]:
    return [bytes([i]) * size for i in range(n)]


class TestBlobStore(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name) / "bs"
        self.bs = BlobStore(self.root)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _commit_in_write_order(self, blobs: list[bytes]) -> list[str]:
        checksums = []
        for i, blob in enumerate(blobs):
            checksums.append(self.bs.write(blob))
            os.utime(self.bs.staging / checksums[-1], ns=(i, i))
        with mock.patch("dirark.blob.FRAME_MB", _TEST_FRAME_MB):
            self.bs.commit()
        return checksums

    def _frames(self, ark: Path) -> list[tuple[int, int]]:
        with closing(open_db(ark / DB_NAME)) as db:
            return db.execute(
                "SELECT tar, offset FROM frames ORDER BY tar, offset"
            ).fetchall()

    def test_write_returns_blake2b_hex(self) -> None:
        data = b"hello world"
        cs = self.bs.write(data)
        self.assertEqual(cs, hashlib.blake2b(data).hexdigest())

    def test_blob_checksum_matches_hashlib(self) -> None:
        data = b"abc"
        self.assertEqual(blob_checksum(data), hashlib.blake2b(data).hexdigest())

    def test_write_then_read_staged(self) -> None:
        data = b"some bytes"
        cs = self.bs.write(data)
        self.assertEqual(self.bs.read(cs), data)
        self.assertTrue((self.root / "staging" / cs).exists())

    def test_write_is_idempotent(self) -> None:
        data = b"same"
        cs1 = self.bs.write(data)
        cs2 = self.bs.write(data)
        self.assertEqual(cs1, cs2)
        staged = list((self.root / "staging").iterdir())
        self.assertEqual(len(staged), 1)

    def test_has_reports_staging_and_ark(self) -> None:
        cs = self.bs.write(b"x")
        self.assertTrue(self.bs.has(cs))
        self.bs.commit()
        self.assertTrue(self.bs.has(cs))

    def test_commit_moves_blobs_to_ark(self) -> None:
        cs_a = self.bs.write(b"a-content")
        cs_b = self.bs.write(b"b-content")
        self.bs.commit()
        self.assertEqual(list((self.root / "staging").iterdir()), [])
        self.assertEqual(self.bs.read(cs_a), b"a-content")
        self.assertEqual(self.bs.read(cs_b), b"b-content")

    def test_commit_with_empty_staging_is_noop(self) -> None:
        self.bs.commit()
        # No tar produced
        self.assertEqual(list_tars(self.bs.ark), [])

    def test_read_unknown_raises_keyerror(self) -> None:
        with self.assertRaises(KeyError):
            self.bs.read("deadbeef" * 16)

    def test_commit_then_write_then_commit(self) -> None:
        cs1 = self.bs.write(b"one")
        self.bs.commit()
        cs2 = self.bs.write(b"two")
        self.bs.commit()
        self.assertEqual(self.bs.read(cs1), b"one")
        self.assertEqual(self.bs.read(cs2), b"two")

    def test_read_sees_blobs_committed_into_a_cached_frame(self) -> None:
        cs1 = self.bs.write(b"one")
        self.bs.commit()
        self.assertEqual(self.bs.read(cs1), b"one")
        cs2 = self.bs.write(b"two")
        self.bs.commit()
        self.assertEqual(len(self._frames(self.bs.ark)), 1)
        self.assertEqual(self.bs.read(cs2), b"two")
        self.assertEqual(self.bs.read(cs1), b"one")

    def test_small_commits_share_one_frame_and_tar(self) -> None:
        for i in range(3):
            self.bs.write(f"commit {i}".encode())
            self.bs.commit()
        self.assertEqual(len(self._frames(self.bs.ark)), 1)
        self.assertEqual(len(list_tars(self.bs.ark)), 1)

    def test_reads_in_write_order_decode_each_frame_once(self) -> None:
        blobs = _blobs(6, _THIRD_FRAME)
        checksums = self._commit_in_write_order(blobs)
        self.assertEqual(len(self._frames(self.bs.ark)), 2)
        with mock.patch("dirark.storage.read_span", wraps=read_span) as decode:
            self.assertEqual([self.bs.read(c) for c in checksums], blobs)
        self.assertEqual(decode.call_count, 2)

    def test_reads_alternating_between_frames(self) -> None:
        blobs = _blobs(3, _OWN_FRAME)
        checksums = self._commit_in_write_order(blobs)
        order = [0, 1, 1, 2, 0, 0, 2, 1]
        self.assertEqual(
            [self.bs.read(checksums[i]) for i in order], [blobs[i] for i in order]
        )

    def test_decoded_frames_stay_within_the_cache_budget(self) -> None:
        checksums = self._commit_in_write_order(_blobs(2, _OWN_FRAME))
        with (
            mock.patch("dirark.storage.READ_CACHE_MB", _TEST_FRAME_MB),
            mock.patch("dirark.storage.read_span", wraps=read_span) as decode,
        ):
            for i in (0, 1, 0):
                self.bs.read(checksums[i])
        self.assertEqual(decode.call_count, 3)

    def test_commit_packs_in_write_order(self) -> None:
        checksums = self._commit_in_write_order(_blobs(8, _OWN_FRAME))
        db = open_db(self.bs.ark / DB_NAME)
        locs = [locate(db, bytes.fromhex(c)) for c in checksums]
        db.close()
        spans = [(loc.tar, loc.offset) for loc in locs if loc is not None]
        self.assertEqual(spans, self._frames(self.bs.ark))

    def test_merge_push_pull_keep_the_blob_frames(self) -> None:
        tmp = Path(self.tmpdir.name)
        self._commit_in_write_order(_blobs(3, _OWN_FRAME))
        merged, pulled = BlobStore(tmp / "merged"), BlobStore(tmp / "pulled")
        merged.merge_from(self.bs)
        self.bs.push(str(tmp / "remote"))
        pulled.pull(str(tmp / "remote"))
        for ark in (merged.ark, tmp / "remote" / "ark", pulled.ark):
            self.assertEqual(len(self._frames(ark)), 3)

    def test_a_held_connection_sees_commits_of_another_instance(self) -> None:
        self.bs.write(b"first")
        self.bs.commit()
        self.assertTrue(self.bs.has(blob_checksum(b"first")))
        other = BlobStore(self.root)
        cs = other.write(b"elsewhere")
        other.commit()
        self.assertEqual(self.bs.read(cs), b"elsewhere")

    def test_a_format_1_store_names_the_migration(self) -> None:
        db = sqlite3.connect(self.bs.ark / DB_NAME)
        db.execute("CREATE TABLE objects(checksum TEXT PRIMARY KEY, tar_name TEXT)")
        db.close()
        with self.assertRaisesRegex(ArkFormatError, "--max-frame-mb"):
            self.bs.write(b"x")

    def test_commit_deduplicates_existing_objects(self) -> None:
        self.bs.write(b"dup")
        self.bs.commit()
        self.bs.write(b"dup")  # already in ark; staging file added then commit dedups
        self.bs.commit()
        # Still readable, no error
        self.assertEqual(self.bs.read(blob_checksum(b"dup")), b"dup")

    def test_merge_from_staged_and_committed(self) -> None:
        other_root = Path(self.tmpdir.name) / "other"
        other = BlobStore(other_root)
        cs_staged = other.write(b"staged-only")
        cs_committed = other.write(b"committed")
        other.commit()
        cs_staged_after = other.write(b"more-staged")

        self.bs.merge_from(other)
        self.assertEqual(self.bs.read(cs_staged), b"staged-only")
        self.assertEqual(self.bs.read(cs_committed), b"committed")
        self.assertEqual(self.bs.read(cs_staged_after), b"more-staged")

    def test_push_pull_carries_every_commit(self) -> None:
        remote = str(Path(self.tmpdir.name) / "remote")
        first = self.bs.write(b"first commit")
        self.bs.commit()
        self.bs.push(remote)
        second = self.bs.write(b"second commit")
        self.bs.commit()
        self.bs.push(remote)

        pulled = BlobStore(Path(self.tmpdir.name) / "pulled")
        pulled.pull(remote)
        self.assertEqual(pulled.read(first), b"first commit")
        self.assertEqual(pulled.read(second), b"second commit")

    def test_purge_removes_root(self) -> None:
        self.bs.write(b"gone")
        self.bs.commit()
        self.bs.purge()
        self.assertFalse(self.root.exists())

    def test_failed_stage_leaves_no_orphan_temp(self) -> None:
        with mock.patch("dirark.blob.os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.bs.write(b"never lands")
        self.assertEqual(list(self.bs.staging.iterdir()), [])


def _writer(root_str: str, payload: bytes) -> str:
    bs = BlobStore(Path(root_str))
    return bs.write(payload)


class TestBlobStoreConcurrency(unittest.TestCase):
    def test_parallel_writes_same_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "shared"
            # Pre-create so workers don't race on mkdir parents
            BlobStore(root)
            with mp.get_context("spawn").Pool(4) as pool:
                checksums = pool.starmap(
                    _writer, [(str(root), b"identical") for _ in range(8)]
                )
            self.assertEqual(len(set(checksums)), 1)
            staged = list((root / "staging").iterdir())
            self.assertEqual(len(staged), 1)
            self.assertEqual(staged[0].read_bytes(), b"identical")

    def test_parallel_writes_distinct_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "shared"
            BlobStore(root)
            payloads = [f"item-{i}".encode() for i in range(16)]
            with mp.get_context("spawn").Pool(4) as pool:
                checksums = pool.starmap(_writer, [(str(root), p) for p in payloads])
            self.assertEqual(set(checksums), {blob_checksum(p) for p in payloads})
            staged = list((root / "staging").iterdir())
            self.assertEqual(len(staged), len(payloads))


if __name__ == "__main__":
    unittest.main()
