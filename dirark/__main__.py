import argparse
import sys
from pathlib import Path

from .core import archive_dir, restore_ark


def main():
    parser = argparse.ArgumentParser(description="A simple cold storage archiver for Linux machines.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Archive command
    archive_parser = subparsers.add_parser("archive", help="Archive files into a cold storage repository.")
    archive_parser.add_argument(
        "source_dir",
        type=Path,
        help="The directory containing files to archive.",
    )
    archive_parser.set_defaults(func=run_archive)

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore files from a cold storage repository.")
    restore_parser.add_argument(
        "archive_repo_dir",
        type=Path,
        help="The directory of the cold storage repository (containing index and tar files).",
    )
    restore_parser.add_argument(
        "destination_dir",
        type=Path,
        help="The directory where the archived files will be restored.",
    )
    restore_parser.set_defaults(func=run_restore)

    args = parser.parse_args()
    args.func(args)


def run_archive(args):
    try:
        archive_dir(args.source_dir)
        print(f"Successfully archived '{args.source_dir}'")
    except Exception as e:
        print(f"Error during archiving: {e}", file=sys.stderr)
        sys.exit(1)


def run_restore(args):
    try:
        restore_ark(args.archive_repo_dir, args.destination_dir)
        print(f"Successfully restored from '{args.archive_repo_dir}' to '{args.destination_dir}'.")
    except Exception as e:
        print(f"Error during restoring: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
