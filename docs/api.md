# dirark

dirark – directory archival and retrieval tool.

## `ArkReader`
Read-only access to individual files within a dirark archive.

    Supports context manager usage::

        with ArkReader(ark_dir) as reader:
            data = reader.read_file("path/to/file.txt")

### `close(self) -> None`
Close the underlying database connection.

### `get_checksum(self, path: str) -> str`
Return the BLAKE2b checksum of an archived file by relative path.

### `list_files(self) -> list[str]`
Return sorted list of all archived file paths.

### `read_file(self, path: str) -> bytes`
Read and return the raw bytes of an archived file.

        Raises KeyError if path is not in the ark.

## `add_dir_to_remote_ark(src_dir: pathlib.Path, remote_ark: str) -> None`
Archive src_dir and merge its contents into a remote ark.

    Pulls the remote ark locally, archives src_dir into it, then pushes the
    updated ark back. Supports SSH remotes (user@host:/path) via rsync.

    The remote ark must already exist (at minimum as an empty directory).
    For a first push, use archive_dir followed by push_ark instead.

## `archive_dir(src_dir: pathlib.Path, ark_out: pathlib.Path | None = None) -> None`
Archive src_dir into a content-addressed store.

    By default the archive is created at src_dir + ARK_DIR_EXT. Pass ark_out
    to write into an existing ark directory (useful for merging or remote push).

    Archiving is idempotent: re-running on the same directory is a no-op.
    Files with duplicate content are deduplicated by BLAKE2b checksum.

## `merge_arks(src_ark: pathlib.Path, dst_ark: pathlib.Path) -> None`
Merge all objects and file mappings from src_ark into dst_ark.

    Objects already present in dst by checksum are skipped (deduplication).
    File path mappings are added with INSERT OR IGNORE, so existing paths
    in dst take precedence.

## `pull_ark(remote: str, local_ark: pathlib.Path) -> None`
Pull a remote ark to a local path via rsync.

    remote may be a local path string or an SSH target (user@host:/path).

## `push_ark(local_ark: pathlib.Path, remote: str) -> None`
Push a local ark to a remote location via rsync.

    Uses checksum comparison (not just mtime+size) to ensure all changes are
    transferred, even when modifications happen within the same second.
    remote may be a local path string or an SSH target (user@host:/path).

## `restore_ark(ark_dir: pathlib.Path, dest_dir: pathlib.Path) -> None`
Restore all files from an ark to dest_dir.

    dest_dir is only created if there are files to restore.
    Missing tars or objects produce warnings but do not abort the restore.
