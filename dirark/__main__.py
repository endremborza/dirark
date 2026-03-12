"""CLI entry point for dirark."""

import argparse
import sys
from pathlib import Path

from .core import archive_dir, restore_ark
from .reader import ArkReader
from .sync import add_dir_to_remote_ark, merge_arks, pull_ark, push_ark


def main() -> None:
    """Parse arguments and dispatch to the appropriate subcommand."""
    parser = argparse.ArgumentParser(
        description="Cold-storage directory archival tool."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("archive", help="Archive a directory.")
    p.add_argument("source_dir", type=Path)
    p.set_defaults(func=_archive)

    p = sub.add_parser("restore", help="Restore files from an ark.")
    p.add_argument("ark_dir", type=Path)
    p.add_argument("dest_dir", type=Path)
    p.set_defaults(func=_restore)

    p = sub.add_parser("push", help="Push local ark to remote via rsync.")
    p.add_argument("local_ark", type=Path)
    p.add_argument("remote")
    p.set_defaults(func=_push)

    p = sub.add_parser("pull", help="Pull remote ark to local path via rsync.")
    p.add_argument("remote")
    p.add_argument("local_ark", type=Path)
    p.set_defaults(func=_pull)

    p = sub.add_parser("merge", help="Merge src_ark into dst_ark (local).")
    p.add_argument("src_ark", type=Path)
    p.add_argument("dst_ark", type=Path)
    p.set_defaults(func=_merge)

    p = sub.add_parser("add", help="Archive a directory and add it to a remote ark.")
    p.add_argument("source_dir", type=Path)
    p.add_argument("remote_ark")
    p.set_defaults(func=_add)

    p = sub.add_parser("read", help="Print a file from an ark to stdout.")
    p.add_argument("ark_dir", type=Path)
    p.add_argument("file_path")
    p.set_defaults(func=_read)

    args = parser.parse_args()
    args.func(args)


def _archive(args: argparse.Namespace) -> None:
    try:
        archive_dir(args.source_dir)
        print(f"Archived '{args.source_dir}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _restore(args: argparse.Namespace) -> None:
    try:
        restore_ark(args.ark_dir, args.dest_dir)
        print(f"Restored to '{args.dest_dir}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _push(args: argparse.Namespace) -> None:
    try:
        push_ark(args.local_ark, args.remote)
        print(f"Pushed '{args.local_ark}' to '{args.remote}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _pull(args: argparse.Namespace) -> None:
    try:
        pull_ark(args.remote, args.local_ark)
        print(f"Pulled '{args.remote}' to '{args.local_ark}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _merge(args: argparse.Namespace) -> None:
    try:
        merge_arks(args.src_ark, args.dst_ark)
        print(f"Merged '{args.src_ark}' into '{args.dst_ark}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _add(args: argparse.Namespace) -> None:
    try:
        add_dir_to_remote_ark(args.source_dir, args.remote_ark)
        print(f"Added '{args.source_dir}' to '{args.remote_ark}'.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _read(args: argparse.Namespace) -> None:
    try:
        with ArkReader(args.ark_dir) as reader:
            sys.stdout.buffer.write(reader.read_file(args.file_path))
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
