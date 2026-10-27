# dirark

Content-addressed directory archival: BLAKE2b-keyed storage in framed zstd tars with a SQLite index, incremental re-archival, ark merging, and rsync sync. Two consumers: directory trees archived through the `dirark` CLI / `archive_dir`, and aswan's object store, which wraps `dirark.BlobStore` (datazimmer reaches dirark only transitively through aswan). aswan's AGENTS.md treats dirark as co-developed: primitives aswan needs belong here, not patched around there.

## Goals, in priority order

1. Storage size, compute time and memory per archived byte. Performance claims are settled by measurement (ubemmer's `dirark` suite, outside this repo: `ubemmer compare dirark` judges a change against its baseline), never asserted; the README's *Trade-offs* section records the measured knobs.
2. A near-empty dependency tree: the heavy lifting runs in system binaries (`zstd`, `rsync`, `tar` for format-1 migration) and C-backed stdlib (`hashlib`, `sqlite3`); Python only orchestrates.
3. A stable on-disk contract: an ark is `index.sqlite` + `data-NNNNN.tar.zst`, each tar readable by plain `tar -I zstd -xf`. Changing it bumps `SCHEMA_VERSION` with a minor release and ships a `dirark migrate` path from the format before; consumers pin a minor, so an ark changes format only when its owner raises the pin and migrates it.

## Where to look

- `dirark/storage.py` — everything the rest composes. `digest_file`/`digest_bytes` (BLAKE2b-512, the `b2sum` digest, as bytes), `open_db` (the schema: `files` path → checksum/mtime/mode, `objects` checksum → span of a frame, `frames` → byte span of a tar; refuses other formats with `ArkFormatError`), `attach` (another index as schema `s`), `query_in` (chunked `IN` lookups), `locate`/`locate_path` → `Location`, `read_span` (streams a frame's decode only as far as needed), `FrameCache` (decoded frames within `READ_CACHE_MB`, shared by `ArkReader` and `BlobStore`), and `Packer`, the only writer of tars: it fills frames up to a frame cap and tars up to `MAX_TAR_MB`, compresses `_COMPRESS_JOBS` frames side by side with `ZSTD_ARGS`, copies foreign frames byte for byte (`copy_frame`), takes over whole tars (`adopt`), reopens a partial last frame into a fresh tar, and `tidy`s tars back to their indexed ends.
- `dirark/core.py` — `archive_dir` (scandir walk, chunked index lookups skip paths whose size/mtime/mode match, threaded hashing, batches of `max_tar_mb` committed one by one) and `restore_ark` (one zstd stream per tar, objects copied out by their index position, each checked against its tar header).
- `dirark/reader.py` — `ArkReader`, a context manager for reading individual files by path through a `FrameCache`.
- `dirark/blob.py` — `BlobStore` + `blob_checksum`: the same storage as a content-addressed *bytes* cache. `write` atomically stages and returns the checksum (multi-process safe); `commit` packs staging in write (mtime) order at `FRAME_MB`; `read` goes through a `FrameCache`; lookups use one connection per process; `merge_from`, `push`, `pull` delegate to sync. This is what `aswan.ObjectStore` wraps.
- `dirark/sync.py` — `push_ark`/`pull_ark` (index-first rsync merges: fetch the other side's index, move only what it lacks, never rewrite a remote tar), `merge_arks`, `add_dir_to_remote_ark`, and `_merge` beneath them: a source tar the destination wholly lacks is adopted (linked for a push, moved for a pull, copied for a merge), a wholly-missing frame is copied verbatim, and only the missing objects of mixed frames are decoded and re-packed. Staging dirs sit beside the ark (`_beside`), never in the OS temp dir.
- `dirark/migrate.py` — `migrate_ark`: format-1 arks (hex index, single-stream tars) re-packed in place, locally or over rsync, every object re-hashed against its checksum; missing or corrupt content raises `MigrationError` before the ark changes. The scratch (unpacked tars, the new index) goes under `work`, so an ark on a network mount migrates with only its tars crossing the wire. A format's path stays while any ark of it remains.
- `dirark/__main__.py` — the `dirark` console script: protocli dispatches straight to the library functions (`archive`/`restore`/`push`/`pull`/`merge`/`add`/`migrate`), so their signatures are the CLI and keyword-only options surface as flags. `read` is the one wrapper (bytes to stdout). Success is silent; failures raise.
- `dirark/__init__.py` — the public exports and `__version__`.
- `dirark/tests/` — one suite per module.

## Architecture in one sentence

Every file/blob is hashed with BLAKE2b and stored once as a tar member inside an independent zstd frame; `index.sqlite` maps paths → checksums → (frame, offset) → (tar, byte span), so a read decodes one frame, re-archiving touches only changed paths, and merges and syncs move frames and whole tars as bytes.

## Invariants

- A tar is its indexed frames tiled from offset 0 in offset order; bytes past the last indexed frame are crash leftovers or a reopened frame's old copy, and `Packer.tidy` cuts them. `restore_ark` relies on the tiling (it verifies each member's header).
- Frames are never rewritten in place: a reopened frame is re-encoded into a fresh tar and its row repointed, so a crash before the index commit loses nothing.
- Tars carry no end-of-archive blocks; appending a frame is a plain append.
- Remote tars are never rewritten: a push writes fresh tars numbered past every tar the remote index references.
- Objects are unique per checksum across an ark; `Packer` inserts fail loudly on a duplicate, so callers dedupe first.
- ubemmer's `dirark` suite drives dirark only through the API its contract's README names; a commit that changes that API declares the next contract number as `[tool.ubemmer] contract` in `pyproject.toml`, and the suite gains a driver for it.

## Where the cost goes

- Archiving: zstd at `ZSTD_ARGS` dominates, run `_COMPRESS_JOBS` frames at a time since zstd's own threads barely split one frame; hashing is threaded; a no-op re-run is one scandir walk plus chunked index lookups.
- Reading: a cold read decodes one frame, or a frame over `READ_CACHE_MB` only up to its object (see the README table); `FrameCache` makes reads in write order decode each frame once.
- Sync and merge: tars and frames move as bytes; only mixed frames are decoded and re-packed. The full index still crosses the wire on every push and pull.
- Memory: each compressing zstd holds about its frame's worth and up to `_COMPRESS_JOBS` run at once; a read holds one decoded frame, or for a frame over `READ_CACHE_MB` just its object (`read_span` stops decoding there). Index lookups and merge diffs stay in SQLite, so outside `migrate_ark` (which loads the format-1 index) Python memory does not grow with ark size.

## Setup

- Python ≥ 3.12, `uv` for the env (`.venv`), `ruff` with its default line length, `select = ["E", "F", "I"]`. `protocli` comes from PyPI; `branb link ../protocli` imports a local checkout ahead of it while the two are co-developed, and `branb link` with no paths undoes that.
- `zstd`, `rsync` and `tar` must be on PATH; a missing one fails loudly at the first call.
- CI (`.github/workflows/test.yml`) runs `branb test` (branthebuilder: ruff, then pytest with coverage into `coverage/`). Pushing a `v*` tag runs `release.yml`: the tests again, then `uv build && uv publish` and the GitHub release.
- `docs/api.md` is generated from docstrings by `branb build-docs`; regenerate it instead of editing by hand. `branb tag` writes `docs/release_notes.md`, bumps `__version__`, commits, tags and pushes; committing and tagging are the maintainer's.

## Known sharp edges

- **`push_ark` is not atomic** — two pushers racing on one remote can claim the same fresh tar name; the later upload overwrites the earlier one's objects.
- **`BlobStore.commit()` excludes concurrent writers and readers** — `write` is atomic-staged and safe from many processes, but `commit` reopens the last frame and tidies tars, so it runs once workers have stopped (exactly how aswan calls it at `save_current`).
- **`BlobStore.read` and `ArkReader` are not thread-safe** — each keeps its own `FrameCache`; give each reading thread its own instance.
- **Incremental archiving trusts size, mtime and mode** — an edit preserving all three is missed. Superseded content stays in the tars; nothing garbage-collects it.
- **A source changing mid-archive raises `SourceChanged`** — its batch is rolled back, earlier batches stay committed.
- **Format-1 arks are refused** with `ArkFormatError` until `dirark migrate` converts them; migration is one-way.
- Requires a posix toolchain; not portable to environments lacking the CLIs.

## Running

```bash
uv run pytest
uv run --project ../../tools/ubemmer ubemmer compare dirark --profile quick   # with UBEMMER_DATA set
```
