# dirark

[![pypi](https://img.shields.io/pypi/v/dirark.svg)](https://pypi.org/project/dirark/)

Content-addressed directory archival with deduplication and remote sync.

Archives directories into compressed, deduplicated stores indexed by BLAKE2b checksums. Re-archiving is incremental (unchanged files are skipped without hashing, changed ones repointed), arks merge by checksum, and push/pull sync them with remote storage over rsync, moving only what the other side lacks.

## Requirements

- Python >= 3.12
- System utilities: `rsync`, `tar`, `zstd`

## Install

```bash
uv add dirark
# or
pip install dirark
```

## CLI

```bash
# Archive a directory → creates <src>.ark.d; re-run to add what changed
dirark archive <src_dir>

# Restore all files from an ark, with their mode and mtime
dirark restore <ark_dir> <dest_dir>

# Print a single file's contents to stdout
dirark read <ark_dir> <file_path>

# Merge an ark into a remote one and back (local path or user@host:/path);
# only objects the other side lacks are transferred, nothing is overwritten
dirark push <local_ark> <remote>
dirark pull <remote> <local_ark>

# Merge src_ark into dst_ark
dirark merge <src_ark> <dst_ark>

# Archive a directory and add it to a remote ark (created if absent)
dirark add <src_dir> <remote_ark>

# Re-pack an ark in index format 1 into the current format, in place
dirark migrate <ark>
```

`dirark --help-all` prints every command's arguments and flags.

## Python API

```python
from dirark import archive_dir, restore_ark, ArkReader, migrate_ark
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

# Remote sync: index-first merges, so arks that diverged (one per machine) can share a remote
push_ark(Path("my_dir.ark.d"), "user@host:/backups/my_dir.ark.d")
pull_ark("user@host:/backups/my_dir.ark.d", Path("local.ark.d"))

# Merge two local arks
merge_arks(Path("src.ark.d"), Path("dst.ark.d"))

# Archive locally and push to remote in one step
add_dir_to_remote_ark(Path("my_dir"), "user@host:/backups/archive.ark.d")

# Re-pack a format-1 ark, local or remote
migrate_ark("user@host:/backups/my_dir.ark.d")
```

## Blob Store

`BlobStore` exposes the same compressed, deduplicated storage as a content-addressed bytes interface — useful for caching computed artifacts keyed by their content.

```python
from dirark import BlobStore
from pathlib import Path

store = BlobStore(Path("cache"))

# write() stages bytes and returns their BLAKE2b checksum; it is idempotent
checksum = store.write(b"...payload...")
assert store.has(checksum)
assert store.read(checksum) == b"...payload..."

# write() is safe from many processes at once (atomic staging). Once writers
# have stopped, commit() packs staged blobs into the ark in write order.
store.commit()

# Pull blobs from another store (e.g. a per-worker shard) into this one
store.merge_from(BlobStore(Path("worker-0")))

# Sync committed blobs with a store rooted at a remote path (staging stays local)
store.push("user@host:/backups/cache")
store.pull("user@host:/backups/cache")
```

Checksums are computed with the same BLAKE2b-512 hashing used by `archive_dir`, so a blob and an archived file with identical content share a key and deduplicate against each other — including across `merge_from`.

A store packs frames of `dirark.blob.FRAME_MB`, smaller than `archive_dir`'s, because blobs are read one at a time (see *Trade-offs*). Migrating a format-1 store's ark takes the same frame size: `dirark migrate <root>/ark --max-frame-mb <FRAME_MB>`; the store's own error message prints the exact command.

## Storage Format

An ark is a directory containing:

- `index.sqlite` — the index, its format number in `PRAGMA user_version` (`dirark.storage.SCHEMA_VERSION`). `files` maps each relative path to its checksum, mtime and mode; `objects` maps each checksum to a span of a frame; `frames` maps each frame to a byte span of a tar. Checksums are 64-byte BLOBs.
- `data-NNNNN.tar.zst` — tars of up to `MAX_TAR_MB` of members, each tar a run of independent zstd frames of up to `MAX_FRAME_MB` of members (a larger object gets a frame of its own). A member is named `./objects/<checksum hex>`.

Each tar is still a plain tar.zst — `tar -I zstd -xf data-00001.tar.zst` extracts its objects — while a read decodes only the frame holding its object. An index in another format raises `ArkFormatError` naming the `dirark migrate` command that converts it.

## Development

Performance is settled by measurement, kept outside this repo so that the benchmarks are versioned apart from the code: the `dirark` suite of ubemmer measures archiving, reading, restoring and syncing JSON and media corpora at several sizes, against any commit, compared with a tagged baseline. A commit that changes the API the suite relies on declares the next contract number as `[tool.ubemmer] contract` in `pyproject.toml`.

## Trade-offs

dirark optimizes packed size first, then wall time, then memory; these are the places where they pull against each other. Figures were measured on a 20-core NVMe machine over two corpora: a Python site-packages tree (12k files, 531 MiB of code and shared libraries) and an aswan object store (42 blobs of JSON and text, 169.5 MiB, mostly weekly snapshots of the same endpoints).

### Frame size: packed size against cold reads

Frames are independent so that one object can be read, and one frame merged, without decoding its neighbours; the cost is that zstd's long window only finds repeats inside a frame. Bigger frames pack smaller, and a read the cache cannot serve decodes its whole frame:

| frame size | site-packages packed | cold read | aswan JSON packed | cold read |
| --- | --: | --: | --: | --: |
| `FRAME_MB` (`BlobStore`) | 124.5 MiB | 19 ms | 9.5 MiB | 13 ms |
| `MAX_FRAME_MB` (`archive_dir`) | 123.6 MiB | 72 ms | 8.4 MiB | 44 ms |
| `MAX_TAR_MB` (one frame per tar) | 120.2 MiB | 85 ms | 5.6 MiB | 54 ms |

Code barely cares; repetitive data like versioned snapshots does, since a snapshot's repeats sit one snapshot back. `archive_dir` takes the middle row because its reads are occasional and restores stream whole tars anyway; `BlobStore` takes the first because aswan reads blobs one by one. `ArkReader` and `BlobStore` keep decoded frames within `READ_CACHE_MB`, so reads in write order decode each frame once. A frame over that budget is never held: each read decodes it from its start up to the object, which is why the last row's cold read looks cheap, and why every further read of it costs the same again. Format 1, which wrote each tar as a single stream, packed these corpora to 154.5 and 16.9 MiB and decoded a whole tar on every cold read.

### Tar size

Each frame is compressed on its own and a read decodes one frame wherever it sits in its tar, so packed size and read cost follow the frame size above, not `MAX_TAR_MB`; format 1, one stream per tar, traded both on tar size. `MAX_TAR_MB` sets the file count of an ark and the granularity of whole-file moves: a pull fetches, in full, every remote tar holding any missing object (a tar whose objects the destination wholly lacks is then adopted as is, any other is merged frame by frame), and every commit appends to or cuts back the ark's last tar, so a file-level backup of the ark re-copies up to one tar of old content per commit.

### Compression settings and memory

`ZSTD_ARGS` picks a high level and zstd's long window. zstd's own threads barely split a frame, so `_COMPRESS_JOBS` frames compress side by side, which keeps `archive_dir` slightly faster than format 1's single-threaded level 3 while packing smaller. The price is memory: each compressing zstd holds about 47 MiB on a `FRAME_MB` frame and 174 MiB on a `MAX_FRAME_MB` frame, with up to `_COMPRESS_JOBS` of them at once next to the Python process (about 190 MiB for `archive_dir` on site-packages). Reading holds one decoded frame (an object past `READ_CACHE_MB` costs about its own size) plus the cache.

Site-packages through the whole pipeline, format 1 against the current format:

| step | format 1 | current |
| --- | --: | --: |
| `archive_dir` | 3.83 s | 3.36 s |
| re-run with nothing changed | 0.50 s | 0.06 s |
| `restore_ark` | 2.16 s | 1.02 s |
| `push_ark` to an empty remote | 4.30 s | 0.51 s |
| `pull_ark` into an empty ark | 4.27 s | 0.46 s |
| packed | 154.5 MiB | 123.6 MiB |
| of which `index.sqlite` | 6.1 MiB | 2.6 MiB |

### Reopened frames

A commit that leaves its last frame under the frame size has that frame reopened by the next commit: decoded, joined with the new objects, re-encoded into a fresh tar, and cut from the old one. Small, frequent commits (a `BlobStore` committing after every aswan run) therefore still fill whole frames, at the price of re-encoding up to one frame per commit and rewriting the tail of the local ark's last tar. Remote tars are never rewritten: a push always writes fresh ones.

### Incremental re-archiving

A path is skipped when its size, mtime and mode all match the index, so an edit that preserves all three is missed. A changed path is repointed at its new content and the old content stays in the tars: the index keeps one version per path, the tars every version ever archived, and nothing is garbage-collected.

A file that changes between being hashed and being packed aborts its batch with `SourceChanged` (batches already committed stay), so archive live directories from a snapshot.

### Merging

No object is stored twice: of a frame the destination partly holds, only the missing objects are decoded and re-packed, while frames and tars it wholly lacks move byte for byte. Paths already in the destination keep their mapping.

### Compatibility and concurrency

- Tars carry no end-of-archive blocks, so an append is a plain append. GNU tar and Python's `tarfile` read them without complaint; other tar implementations are untested.
- The current format is unreadable by dirark releases before it, and `dirark migrate` is one-way.
- Checksums in `index.sqlite` are BLOBs; query them with `hex(checksum)`.
- `push_ark` is not atomic: two pushers to one remote can claim the same fresh tar name, and the later upload overwrites the earlier one's objects.
- `BlobStore.write` is safe from many processes, but `commit` must run with no concurrent writers or readers, since it re-encodes the last frame and cuts tars back.
