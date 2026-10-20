"""Render every recorded result into one self-contained HTML page."""

import json
import os
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path

from corpora import DATA_ENV, data_dir
from runner import METRICS, RESULTS, git

TEMPLATE = Path(__file__).with_name("report.template.html")
REPORT = Path(__file__).with_name("report.html")
PLACEHOLDER = "/*BENCH*/null"
NOISE_FLOOR = 0.03


def report() -> None:
    """Write benchmarks/report.html from every recorded result."""
    paths = sorted(RESULTS.glob("*/*.json"))
    if DATA_ENV in os.environ:
        paths += sorted((data_dir() / "results").glob("*/worktree.json"))
    runs = [json.loads(p.read_text()) for p in paths]
    tags = _tags()
    for r in runs:
        r["tags"] = tags.get(r["commit"], [])
    runs.sort(key=lambda r: (r["date"], r["dirty"]))
    payload = {
        "metrics": {k: asdict(m) for k, m in METRICS.items()},
        "runs": runs,
        "noise": NOISE_FLOOR,
    }
    data = json.dumps(payload).replace("</", "<\\/")
    REPORT.write_text(TEMPLATE.read_text().replace(PLACEHOLDER, data))
    print(REPORT)


def _tags() -> dict[str, list[str]]:
    fmt = "%(refname:short) %(objectname) %(*objectname)"
    out = defaultdict(list)
    for line in git("for-each-ref", f"--format={fmt}", "refs/tags").splitlines():
        name, obj, *peeled = line.split()
        out[peeled[0] if peeled else obj].append(name)
    return out
