## v1.0.0

- **Breaking:** the ark storage format changed (index schema version 2): a binary SQLite index and tars of independent zstd frames replace the old hex index and single-stream tars; existing arks must be converted with the new `dirark migrate` command before they can be read or written.
- **Breaking:** `archive_dir`, `merge_arks`, `push_ark`, and `pull_ark` now take their options as keyword-only arguments, and gained a `max_frame_mb` option.
- Added `dirark migrate` (and `migrate_ark`) to re-pack a format-1 ark in place, locally or over rsync, with a `--work` option to put its scratch space on another disk; it raises `MigrationError` and leaves the ark untouched if an indexed tar is missing or an object is missing, corrupt, or orphaned.
- Re-archiving is now incremental: a file whose size, mtime, and mode are unchanged is skipped without rehashing, instead of only skipping paths already indexed.
- `restore_ark` now restores each file's original mode and mtime.
- `ArkFormatError` is now exported from `dirark`.

---

## v0.4.1

fix relative package

---

## v0.4.0

heavy optimization and dispatch

---

## v0.3.0

sync and tests

---

## v0.1.0

syncing
