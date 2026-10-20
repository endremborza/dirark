"""The ``dirark`` command: the library's functions, dispatched by protocli."""

import sys
from pathlib import Path

from protocli import Dispatcher

from .core import archive_dir, restore_ark
from .migrate import migrate_ark
from .reader import ArkReader
from .sync import add_dir_to_remote_ark, merge_arks, pull_ark, push_ark


def _read(ark_dir: Path, file_path: str) -> None:
    """Print a file from an ark to stdout."""
    with ArkReader(ark_dir) as reader:
        sys.stdout.buffer.write(reader.read_file(file_path))


main = Dispatcher(
    "dirark",
    {
        "archive": archive_dir,
        "restore": restore_ark,
        "read": _read,
        "push": push_ark,
        "pull": pull_ark,
        "merge": merge_arks,
        "add": add_dir_to_remote_ark,
        "migrate": migrate_ark,
    },
).run

if __name__ == "__main__":
    raise SystemExit(main())
