"""ArkReader: retrieve individual files from a dirark archive."""

from pathlib import Path

from .storage import DB_NAME, open_db, read_object_from_tar


class ArkReader:
    """Read-only access to individual files within a dirark archive.

    Supports context manager usage::

        with ArkReader(ark_dir) as reader:
            data = reader.read_file("path/to/file.txt")
    """

    def __init__(self, ark_dir: Path) -> None:
        """Open an ark for reading."""
        self._ark_dir = ark_dir
        self._db = open_db(ark_dir / DB_NAME)

    def __enter__(self) -> "ArkReader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def list_files(self) -> list[str]:
        """Return sorted list of all archived file paths."""
        cur = self._db.cursor()
        cur.execute("SELECT path FROM files ORDER BY path")
        return [row[0] for row in cur.fetchall()]

    def get_checksum(self, path: str) -> str:
        """Return the BLAKE2b checksum of an archived file by relative path."""
        cur = self._db.cursor()
        cur.execute("SELECT checksum FROM files WHERE path=?", (path,))
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"File not found in ark: {path}")
        return row[0]

    def read_file(self, path: str) -> bytes:
        """Read and return the raw bytes of an archived file.

        Raises KeyError if path is not in the ark.
        """
        cur = self._db.cursor()
        cur.execute(
            "SELECT f.checksum, o.tar_name "
            "FROM files f JOIN objects o ON f.checksum = o.checksum "
            "WHERE f.path=?",
            (path,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"File not found in ark: {path}")
        checksum, tar_name = row
        return read_object_from_tar(self._ark_dir / tar_name, checksum)

    def close(self) -> None:
        """Close the underlying database connection."""
        self._db.close()
