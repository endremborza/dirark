"""CLI entry point for dirark."""

import argparse
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from .core import archive_dir, restore_ark
from .reader import ArkReader
from .sync import add_dir_to_remote_ark, merge_arks, pull_ark, push_ark


def _read(ark_dir: Path, file_path: str) -> None:
    with ArkReader(ark_dir) as reader:
        sys.stdout.buffer.write(reader.read_file(file_path))


@dataclass(frozen=True)
class Command:
    """A subcommand: its parser spec, the function it calls, and its message.

    params maps positional arg names to their argparse type, in the order the
    target function expects them. done is a format string over the parsed args;
    an empty string prints nothing (used by commands that write their own output).
    """

    name: str
    help: str
    params: tuple[tuple[str, type], ...]
    func: Callable[..., object]
    done: str = ""


COMMANDS = (
    Command(
        "archive",
        "Archive a directory.",
        (("source_dir", Path),),
        archive_dir,
        "Archived '{source_dir}'.",
    ),
    Command(
        "restore",
        "Restore files from an ark.",
        (("ark_dir", Path), ("dest_dir", Path)),
        restore_ark,
        "Restored to '{dest_dir}'.",
    ),
    Command(
        "push",
        "Push local ark to remote via rsync.",
        (("local_ark", Path), ("remote", str)),
        push_ark,
        "Pushed '{local_ark}' to '{remote}'.",
    ),
    Command(
        "pull",
        "Pull remote ark to local path via rsync.",
        (("remote", str), ("local_ark", Path)),
        pull_ark,
        "Pulled '{remote}' to '{local_ark}'.",
    ),
    Command(
        "merge",
        "Merge src_ark into dst_ark (local).",
        (("src_ark", Path), ("dst_ark", Path)),
        merge_arks,
        "Merged '{src_ark}' into '{dst_ark}'.",
    ),
    Command(
        "add",
        "Archive a directory and add it to a remote ark.",
        (("source_dir", Path), ("remote_ark", str)),
        add_dir_to_remote_ark,
        "Added '{source_dir}' to '{remote_ark}'.",
    ),
    Command(
        "read",
        "Print a file from an ark to stdout.",
        (("ark_dir", Path), ("file_path", str)),
        _read,
    ),
)


def main() -> None:
    """Parse arguments and dispatch to the appropriate subcommand."""
    parser = argparse.ArgumentParser(
        description="Cold-storage directory archival tool."
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for cmd in COMMANDS:
        p = sub.add_parser(cmd.name, help=cmd.help)
        for arg_name, arg_type in cmd.params:
            p.add_argument(arg_name, type=arg_type)
        p.set_defaults(cmd=cmd)

    args = parser.parse_args()
    cmd: Command = args.cmd
    try:
        cmd.func(*(getattr(args, name) for name, _ in cmd.params))
        if cmd.done:
            print(cmd.done.format(**vars(args)))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
