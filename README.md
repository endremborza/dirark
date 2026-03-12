# dirark

[![pypi](https://img.shields.io/pypi/v/dirark.svg)](https://pypi.org/project/dirark/)

Content-addressed directory archival with deduplication and remote sync.

Archives directories into compressed, deduplicated stores indexed by BLAKE2b checksums. Supports incremental archiving (idempotent re-runs), merging arks, and pushing/pulling to remote storage via rsync.

## Requirements

- Python >= 3.12
- System utilities: `b2sum`, `rsync`, `tar`, `zstd`

## Install

```bash
uv add dirark
# or
pip install dirark
```

## CLI

```bash
# Archive a directory → creates <src>.ark.d
dirark archive <src_dir>

# Restore all files from an ark
dirark restore <ark_dir> <dest_dir>

# Print a single file's contents to stdout
dirark read <ark_dir> <file_path>

# Push/pull ark to/from remote (local path or user@host:/path)
dirark push <local_ark> <remote>
dirark pull <remote> <local_ark>

# Merge src_ark into dst_ark
dirark merge <src_ark> <dst_ark>

# Archive a directory and add it to an existing remote ark
dirark add <src_dir> <remote_ark>
```

## Python API

```python
from dirark import archive_dir, restore_ark, ArkReader
from dirark import push_ark, pull_ark, merge_arks, add_dir_to_remote_ark
from pathlib import Path

# Archive
archive_dir(Path("my_dir"))             # creates my_dir.ark.d
archive_dir(Path("my_dir"), ark_out=Path("existing.ark.d"))  # merge into existing

# Restore
restore_ark(Path("my_dir.ark.d"), Path("restored/"))

# Read individual files without full restore
with ArkReader(Path("my_dir.ark.d")) as ark:
    files = ark.list_files()
    checksum = ark.get_checksum("path/to/file.txt")
    data = ark.read_file("path/to/file.txt")

# Remote sync
push_ark(Path("my_dir.ark.d"), "user@host:/backups/my_dir.ark.d")
pull_ark("user@host:/backups/my_dir.ark.d", Path("local.ark.d"))

# Merge two local arks
merge_arks(Path("src.ark.d"), Path("dst.ark.d"))

# Archive locally and push to remote in one step
add_dir_to_remote_ark(Path("my_dir"), "user@host:/backups/archive.ark.d")
```

## Storage Format

An ark is a directory containing:

- `index.sqlite` — maps file paths to BLAKE2b checksums, and checksums to tar archives
- `data-NNNNN.tar.zst` — zstd-compressed tars holding the actual file data (max 256 MB each)

Files are deduplicated by checksum. Re-archiving a directory that hasn't changed is a no-op.
