"""ArkReader: retrieve individual files from a dirark archive."""

from pathlib import Path

from .storage import DB_NAME, FrameCache, locate_path, open_db


class ArkReader:
    """Read-only access to individual files within a dirark archive.

    A read decodes the one frame holding the file; decoded frames are cached,
    so files archived together read back from one decode.

    Supports context manager usage::

        with ArkReader(ark_dir) as reader:
            data = reader.read_file("path/to/file.txt")
    """

    def __init__(self, ark_dir: Path) -> None:
        """Open an ark for reading."""
        self._ark_dir = ark_dir
        self._db = open_db(ark_dir / DB_NAME)
        self._frames = FrameCache()

    def __enter__(self) -> "ArkReader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def list_files(self) -> list[str]:
        """Return sorted list of all archived file paths."""
        rows = self._db.execute("SELECT path FROM files ORDER BY path")
        return [path for (path,) in rows]

    def get_checksum(self, path: str) -> str:
        """Return the BLAKE2b checksum of an archived file by relative path."""
        query = "SELECT checksum FROM files WHERE path=?"
        row = self._db.execute(query, (path,)).fetchone()
        if row is None:
            raise KeyError(f"File not found in ark: {path}")
        return row[0].hex()

    def read_file(self, path: str) -> bytes:
        """Read and return the raw bytes of an archived file.

        Raises KeyError if path is not in the ark.
        """
        loc = locate_path(self._db, path)
        if loc is None:
            raise KeyError(f"File not found in ark: {path}")
        return self._frames.read(self._ark_dir, loc)

    def close(self) -> None:
        """Close the underlying database connection."""
        self._db.close()
