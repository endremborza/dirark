"""dirark – directory archival and retrieval tool."""

from .core import archive_dir, restore_ark
from .reader import ArkReader
from .sync import add_dir_to_remote_ark, merge_arks, pull_ark, push_ark

__version__ = "0.1.0"

__all__ = [
    "archive_dir",
    "restore_ark",
    "ArkReader",
    "push_ark",
    "pull_ark",
    "merge_arks",
    "add_dir_to_remote_ark",
]
