# Benchmarks

`benchmarks/` measures dirark on a fixed matrix of corpora × size tiers × operations, against any commit, and keeps the results per commit so that each change can be compared with a tagged baseline in a browsable report.

## Running

Everything runs through `uv run python benchmarks/bench.py <command>` with `DIRARK_BENCH_DATA` naming a data directory on a local disk, which holds the corpora, their tiers and the scratch space of a run.

| command | does |
| --- | --- |
| `status` | lists every corpus, its tiers and whether it is fetched |
| `fetch [NAME...]` | downloads public corpora (every missing one by default) and checks them against their pinned digests |
| `run [REF] [--corpora a,b] [--tiers 100M,1G] [--reps N] [--force]` | measures a commit in a worktree of its own, or the working tree when `REF` is omitted |
| `report` | writes `benchmarks/report.html` from every recorded result |
| `sweep CORPUS TIER` | archives one tier at each frame size dirark uses and prints packed size, archive time and median cold read, on the working tree |

A case needs `SPACE_FACTOR` times its tier in free space plus `FREE_MARGIN`, the peak of format 1's merges, which stage every object unpacked; `--tiers 100M,1G` is the quick profile. The runner waits for the machine to idle below `MAX_BUSY_CORES` before each case, since any concurrent load skews wall times; `--force` measures anyway and records the load it saw.

## Corpora

A corpus is a directory read in sorted path order. Its manifest (path, size and BLAKE2b of every file) is hashed into a digest, pinned in `benchmarks/corpora.py` for public corpora, so a result is only compared with results measured on the same bytes.

| corpus | source | shape |
| --- | --- | --- |
| `silesia` | the Silesia corpus | the standard compression corpus, run whole, so its ratios compare with published ones |
| `json-docs` | GH Archive hourly event dumps from `GHARCHIVE_START`, cut into pages of `PAGE_EVENTS` events | many independent API-like documents of one schema |
| `json-snapshots` | npm releases before `SNAPSHOT_CUTOFF` of `@mdn/browser-compat-data` and `caniuse-db` (their `data.json`) and `@octokit/openapi` (`api.github.com.json`), named by publish time so the three series interleave | a few endpoints re-captured release by release, as aswan stores them |
| `image` | the start of the Open Images validation tarball | Flickr JPEGs |
| `video` | Blender open films (`VIDEOS`), ordered so that tiers land near their nominal sizes | a few files, most larger than a frame |
| `audio` | MTG-Jamendo full tracks, verified against the dataset's published sha256s | full-length 320 kbps MP3s |
| `media` | hard links to the files of `MEDIA_SOURCES`, merged by the byte offset at which each ends within its own corpus | photos, tracks and films interleaved in step by bytes, as a personal media folder mixes them |

Each public corpus is fetched to `FETCH_MARGIN` past the largest tier. A corpus named `private-*` has no fetcher: its files are copied into `$DIRARK_BENCH_DATA/corpora/private-<name>/` by hand, named so that sorted order is write order, and its results are recorded like any other but cannot be reproduced elsewhere.

A tier is the prefix of the corpus whose bytes come closest to the nominal size in `TIERS`, kept when within a factor of `TIER_SLACK` of it, so a tier's actual size is reported beside every result. The file after the prefix is the tier's probe. Tiers are directories of hard links into the corpus, rebuilt when their digest changes.

## What a case measures

A case is one tier of one corpus. Each of its repetitions (`REPS`, by tier) runs, in order:

- `archive_dir` into a fresh ark from a cold page cache: wall time, CPU time, peak memory, and the ratio of source bytes to ark bytes with the index's share separately;
- `archive_dir` again with nothing changed: the walk and index lookups alone;
- cold reads of a fixed sample of `READS` files, each through a fresh `ArkReader` with the ark evicted from the page cache: median and 95th percentile;
- `restore_ark` of the whole ark from a cold cache, its output checked for file count and bytes;
- `pull_ark` of the ark into an empty one, then `push_ark` of it to an empty remote, both through rsync between local paths, so they time the merge and the copy without a network; each destination's index is checked for every path;
- `archive_dir` again with the probe added: the cost of adding one file to an ark of that size, walk and reopened frame included;
- `push_ark` of that to the remote: the cost of syncing one new file.

Corpora whose names start with one of `BLOB_PREFIXES` also go through `BlobStore`, aswan's path: every file written and committed (ratio, wall, CPU, memory), the same sample read cold through fresh stores, and the probe written and committed.

Each operation runs in its own `python -I -S` that imports dirark from the measured checkout ahead of anything installed, so any commit with today's public API can be measured, format 1 included; its temporary files go under the data dir, since format 1 stages restores and merges in the OS temp dir. Wall and CPU time cover the operation alone, CPU including its zstd children; page caches are dropped with `posix_fadvise`, which needs no privileges; peak memory is the anonymous RSS of the whole process tree, sampled every `SAMPLE_S`.

## Results and the report

A clean commit's results go to `benchmarks/results/<host>/<sha>.json` and are committed with the code; re-running a commit replaces only the cases it ran. A dirty working tree's results go to `$DIRARK_BENCH_DATA/results/<host>/worktree.json`, replaced on every run. Each case records its tier's digest, size and file count, the harness digest (`HARNESS`), the load seen before it, and every repetition of every metric.

`report` writes one self-contained `benchmarks/report.html` with three linked views for one machine:

- **Commits**: one row per measured commit, each cell the geometric mean over the visible cases of that commit's median against the baseline's, so one number per metric says whether a change helped. Sortable by any column.
- **Cases**: the selected commit's medians per case, with their change against the baseline; a hover shows every repetition.
- **History**: one case and metric across commits, with the repetitions' spread and the baseline marked.

The baseline defaults to the newest tagged commit with results. A change counts as noise when the two commits' repetition ranges overlap, or for single repetitions when it is under `NOISE_FLOOR`. Cases measured on different corpus bytes are shown but not compared, and those measured by a different harness are marked.
