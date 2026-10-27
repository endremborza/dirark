# dirark

dirark – directory archival and retrieval tool.

## `ArkFormatError`
An ark index in a format this version neither reads nor writes.

## `ArkReader`
Read-only access to individual files within a dirark archive.

    A read decodes the one frame holding the file; decoded frames are cached,
    so files archived together read back from one decode.

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

## `BlobStore`
Content-addressed byte storage over a dirark ark.

### `commit(self) -> None`
Pack staged blobs into the ark in write order; clear staging.

### `has(self, checksum: str) -> bool`
Return True if `checksum` is staged or committed.

### `merge_from(self, other: 'BlobStore') -> None`
Pull all staged + committed blobs from `other` into self.

### `pull(self, remote: str) -> None`
Merge the committed blobs of the store rooted at `remote` into this one.

### `purge(self) -> None`

### `push(self, remote: str) -> None`
Merge committed blobs into the store rooted at `remote`; staging stays.

### `read(self, checksum: str) -> bytes`
Return bytes for `checksum`; raise KeyError if unknown.

### `write(self, data: bytes) -> str`
Store `data`; return its blake2b checksum. Idempotent.

## `add_dir_to_remote_ark(src_dir: pathlib.Path, remote_ark: str) -> None`
Archive src_dir and merge its contents into a remote ark.

    Archives into a fresh ark beside src_dir and pushes it, so only objects the
    remote lacks are uploaded and the remote is created if absent. Existing
    remote path mappings take precedence. Supports SSH remotes (user@host:/path).

## `archive_dir(src_dir: pathlib.Path, *, ark_out: pathlib.Path | None = None, max_tar_mb: int = 256, max_frame_mb: int = 64) -> None`
Archive src_dir into a content-addressed store.

    By default the archive is created at src_dir + ARK_DIR_EXT. Pass ark_out
    to write into an existing ark directory (useful for merging or remote push).

    Re-archiving is incremental: a path whose size, mtime and mode match the
    index is skipped without hashing; any other is hashed and repointed at its
    content, stored once per BLAKE2b checksum. Content a path pointed at before
    stays in the ark.

    Changed files go in batches of max_tar_mb, each committed to the index as it
    is packed, so an interrupted run keeps everything packed so far.

## `blob_checksum(data: bytes) -> str`
Return the BLAKE2b content-address key for ``data``.

    Uses the same BLAKE2b-512 (`hashlib`) as archive_dir, so a blob and an
    archived file with identical content share a checksum and deduplicate
    against each other.

## `merge_arks(src_ark: pathlib.Path, dst_ark: pathlib.Path, *, max_tar_mb: int = 256, max_frame_mb: int = 64) -> None`
Merge all objects and file mappings from src_ark into dst_ark.

    Objects already present in dst by checksum are skipped (deduplication).
    File path mappings are added with INSERT OR IGNORE, so existing paths in dst
    take precedence. Frames move without re-encoding: a tar whose objects dst
    all lacks is copied whole, a frame whose objects dst all lacks is copied
    byte for byte, and only the missing objects of other frames are re-packed.
    New tars are numbered past every tar dst's index references, so dst may be
    an index-only copy of a remote ark.

## `migrate_ark(ark: str, *, max_tar_mb: int = 256, max_frame_mb: int = 64, work: pathlib.Path | None = None) -> None`
Re-pack a format-1 ark in place; a current-format ark is left alone.

    ark is a local path or an rsync remote (user@host:/path). Each tar is
    unpacked, and the new index built, in a temporary dir under work (beside the
    ark by default: point it at a local disk for an ark on a network mount); the
    new tars are written beside the ark. A remote is fetched into a temporary
    dir under work (the working directory by default), migrated there, and
    uploaded back. Every object is re-hashed against its checksum: a missing
    tar, a missing or corrupt object, or a path without content raises
    MigrationError and leaves the ark as it was. A BlobStore's ark takes
    max_frame_mb=dirark.blob.FRAME_MB.

## `pull_ark(remote: str, local_ark: pathlib.Path, *, max_tar_mb: int = 256, max_frame_mb: int = 64) -> None`
Merge the ark at remote into local_ark via rsync; local-only objects stay.

    Index-first: fetches the remote index, then only the tars holding objects
    local_ark lacks. A remote without an index is a no-op.
    remote may be a local path string or an SSH target (user@host:/path).

## `push_ark(local_ark: pathlib.Path, remote: str, *, max_tar_mb: int = 256, max_frame_mb: int = 64) -> None`
Merge local_ark into the ark at remote via rsync, creating it if absent.

    Index-first: only the remote index is fetched; objects it lacks go into
    fresh tars numbered past every tar it references, and those tars plus the
    merged index are uploaded. A local tar whose objects the remote all lacks is
    uploaded as is. Remote tars are never rewritten, so arks that diverged (e.g.
    one per machine) can all push to one remote. Concurrent pushes are not safe:
    both can claim the same fresh tar name.
    remote may be a local path string or an SSH target (user@host:/path).

## `restore_ark(ark_dir: pathlib.Path, dest_dir: pathlib.Path) -> None`
Restore all files from an ark to dest_dir, with their mode and mtime.

    dest_dir is only created if there are files to restore.
    Missing tars or objects produce warnings but do not abort the restore.
