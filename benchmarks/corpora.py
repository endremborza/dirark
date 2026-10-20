"""Benchmark corpora: public ones fetched and pinned, private ones frozen by hand.

A corpus is a directory under `corpora/` of the data dir, read in sorted path
order. Its manifest lists every file with its size and BLAKE2b digest; the
digest over the manifest pins what a result was measured on. A tier is the
prefix of that order whose bytes come closest to the tier's nominal size, kept
when within TIER_SLACK of it, and the file after the prefix is the probe that
"add one file" adds. A corpus whose name starts with PRIVATE_PREFIX has no
fetcher: it is copied in by hand and never published.
"""

import gzip
import hashlib
import heapq
import io
import json
import os
import shutil
import subprocess
import tarfile
import zipfile
from base64 import b64decode
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import accumulate, batched
from operator import itemgetter
from pathlib import Path
from typing import IO

DATA_ENV = "DIRARK_BENCH_DATA"
TIERS = {"100M": 10**8, "1G": 10**9, "10G": 10**10}
WHOLE = "all"
TIER_SLACK = 2
FETCH_MARGIN = 1.02
PRIVATE_PREFIX = "private-"
_FETCH_TARGET = int(max(TIERS.values()) * FETCH_MARGIN)
_CURL = ("curl", "-fsSL", "--retry", "5")
_NPM_JOBS = 8

SILESIA_URL = "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"
GHARCHIVE_URL = "https://data.gharchive.org/{:%Y-%m-%d}-{}.json.gz"
GHARCHIVE_START = datetime(2024, 1, 15)
PAGE_EVENTS = 100
NPM_URL = "https://registry.npmjs.org/"
SNAPSHOT_SERIES = (
    ("@mdn/browser-compat-data", ("package/data.json",)),
    ("caniuse-db", ("package/data.json",)),
    (
        "@octokit/openapi",
        ("package/generated/api.github.com.json", "package/cache/api.github.com.json"),
    ),
)
SNAPSHOT_CUTOFF = "2026-10-01"
OPEN_IMAGES_URL = "https://open-images-dataset.s3.amazonaws.com/tar/validation.tar.gz"
BLENDER_URL = "https://download.blender.org/"
VIDEOS = (
    "demo/movies/elephantsdream_teaser.mp4.zip",
    "demo/movies/caminandes_gran_dillama.mp4.zip",
    "demo/movies/BBB/bbb_sunflower_1080p_30fps_normal.mp4.zip",
    "demo/movies/ToS/tears_of_steel_720p.mov",
    "durian/movies/Sintel.2010.720p.mkv.zip",
    "demo/movies/BBB/bbb_sunflower_2160p_60fps_normal.mp4.zip",
    "durian/movies/sintel_4k.mov.zip",
    "demo/movies/ToS/ToS-4k-1920.mov.zip",
    "durian/movies/Sintel.2010.1080p.mkv",
    "demo/movies/BBB/bbb_sunflower_1080p_60fps_normal.mp4.zip",
    "demo/movies/Cycles_Demoreel_2015.mov",
)
JAMENDO_URL = (
    "https://cdn.freesound.org/mtg-jamendo/autotagging_moodtheme/audio/"
    "autotagging_moodtheme_audio-{:02d}.tar"
)
JAMENDO_SHA256 = (
    "74bff54fcfc4d008d4e6d57314bcced398f234373ce595627165b8bd738a8923",
    "24ff6f9b5b342e598dfd0ba18e830d6cb8dae5bcf5f7d1870a2dd39f62c34354",
    "51f0e6683f1f50417da25cef8604b7115da747e2d69399d877e4b4c7c2a0e7e1",
    "7b344becad4192a23a20b57bb5333558b8849ee9479d262b07b5eb106113432e",
    "4a69ed58357f870412afee8c9b6e3dee5575b06ba7c69d22615b28b47f09b3df",
    "fb121fea57ac6486e9e66dafd537ae047d8d54a0a0c8241593dacd765d1d05e3",
    "32c792881fc69272062cadf12bed2ab7d85710d37a652b3085371d9ba1d748a0",
    "5c8546adf1c353db903596cd99acf94701233cee7599851473b20a9765546883",
)
MEDIA_SOURCES = ("image", "audio", "video")


@dataclass(frozen=True)
class Entry:
    path: str
    size: int
    digest: str


@dataclass(frozen=True)
class Tier:
    corpus: str
    name: str
    files: tuple[Entry, ...]
    probe: Entry | None

    @property
    def key(self) -> str:
        return f"{self.corpus}/{self.name}"

    @property
    def bytes(self) -> int:
        return sum(e.size for e in self.files)

    @property
    def digest(self) -> str:
        return manifest_digest(self.files)


@dataclass(frozen=True)
class Public:
    build: Callable[[Path, Path], None]
    digest: str | None = None
    whole: bool = False


def data_dir() -> Path:
    if DATA_ENV not in os.environ:
        raise RuntimeError(f"set {DATA_ENV} to the benchmark data directory")
    return Path(os.environ[DATA_ENV])


def corpus_names() -> list[str]:
    private = sorted(
        p.name
        for p in (data_dir() / "corpora").glob(f"{PRIVATE_PREFIX}*")
        if p.is_dir()
    )
    return [*PUBLIC, *private]


def fetch(name: str) -> None:
    """Build a public corpus from its sources and check it against its pin."""
    dest = data_dir() / "corpora" / name
    if dest.exists():
        raise FileExistsError(f"{dest} exists; delete it to fetch again")
    partial, downloads = dest.with_name(f"{name}.partial"), data_dir() / "downloads"
    shutil.rmtree(partial, ignore_errors=True)
    (data_dir() / "manifests" / f"{name}.json").unlink(missing_ok=True)
    partial.mkdir(parents=True)
    downloads.mkdir(exist_ok=True)
    PUBLIC[name].build(partial, downloads)
    partial.rename(dest)
    got, pinned = manifest_digest(manifest(name)), PUBLIC[name].digest
    if pinned is not None and got != pinned:
        raise ValueError(f"{name}: fetched digest {got}, pinned {pinned}")


def manifest(name: str) -> tuple[Entry, ...]:
    """The corpus's files in order, hashed once and cached beside the corpora."""
    cache = data_dir() / "manifests" / f"{name}.json"
    if cache.exists():
        return tuple(Entry(*row) for row in json.loads(cache.read_text()))
    root = data_dir() / "corpora" / name
    paths = sorted(p for p in root.rglob("*") if p.is_file())
    with ThreadPoolExecutor() as pool:
        digests = list(pool.map(_hash, paths))
    entries = tuple(
        Entry(p.relative_to(root).as_posix(), p.stat().st_size, d)
        for p, d in zip(paths, digests)
    )
    cache.parent.mkdir(exist_ok=True)
    cache.write_text(json.dumps([[e.path, e.size, e.digest] for e in entries]))
    return entries


def manifest_digest(entries: tuple[Entry, ...]) -> str:
    h = hashlib.blake2b(digest_size=16)
    for e in entries:
        h.update(f"{e.path}\0{e.size}\0{e.digest}\n".encode())
    return h.hexdigest()


def tiers(name: str) -> list[Tier]:
    entries = manifest(name)
    public = PUBLIC.get(name)
    if public is not None and public.whole:
        return [Tier(name, WHOLE, entries, None)]
    sums = list(accumulate(e.size for e in entries[:-1]))
    out = []
    for tier, size in TIERS.items():
        k = min(range(len(sums)), key=lambda i: abs(sums[i] - size))
        if size / TIER_SLACK <= sums[k] <= size * TIER_SLACK:
            out.append(Tier(name, tier, entries[: k + 1], entries[k + 1]))
    return out


def tier_dir(tier: Tier) -> Path:
    """The tier as a directory of hard links into its corpus, rebuilt when stale."""
    root = data_dir() / "tiers" / tier.corpus
    path, stamp = root / tier.name, root / f"{tier.name}.digest"
    if path.is_dir() and stamp.exists() and stamp.read_text() == tier.digest:
        return path
    shutil.rmtree(path, ignore_errors=True)
    corpus = data_dir() / "corpora" / tier.corpus
    for e in tier.files:
        (path / e.path).parent.mkdir(parents=True, exist_ok=True)
        os.link(corpus / e.path, path / e.path)
    stamp.write_text(tier.digest)
    return path


def _hash(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "blake2b").hexdigest()


def _curl(url: str, out: Path) -> None:
    subprocess.run([*_CURL, "-o", str(out), url], check=True)


def _get(url: str) -> bytes:
    return subprocess.run([*_CURL, url], check=True, capture_output=True).stdout


@contextmanager
def _stream(url: str) -> Iterator[IO[bytes]]:
    """The body of `url` as a pipe; leaving early stops the download."""
    proc = subprocess.Popen([*_CURL, url], stdout=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        yield proc.stdout
    finally:
        done = proc.poll() is not None
        proc.kill()
        if proc.wait() and done:
            raise subprocess.CalledProcessError(proc.returncode, url)


def _silesia(dest: Path, downloads: Path) -> None:
    archive = downloads / "silesia.zip"
    _curl(SILESIA_URL, archive)
    with zipfile.ZipFile(archive) as z:
        z.extractall(dest)
    archive.unlink()


def _json_docs(dest: Path, _: Path) -> None:
    total, hour = 0, GHARCHIVE_START
    while total < _FETCH_TARGET:
        stem = f"{hour:%Y-%m-%d}-{hour.hour:02d}"
        (dest / stem).mkdir()
        with _stream(GHARCHIVE_URL.format(hour, hour.hour)) as body:
            lines = gzip.open(body)
            for i, page in enumerate(batched(lines, PAGE_EVENTS)):
                doc = b"[" + b",".join(line.rstrip(b"\n") for line in page) + b"]"
                total += (dest / stem / f"{i:05d}.json").write_bytes(doc)
        hour += timedelta(hours=1)


def _json_snapshots(dest: Path, _: Path) -> None:
    jobs = []
    for package, members in SNAPSHOT_SERIES:
        meta = json.loads(_get(NPM_URL + package.replace("/", "%2f")))
        for version, info in meta["versions"].items():
            published = meta["time"][version]
            if "-" not in version and published < SNAPSHOT_CUTOFF:
                jobs.append((published, package, version, info["dist"], members))
    with ThreadPoolExecutor(_NPM_JOBS) as pool:
        list(pool.map(lambda job: _npm_snapshot(dest, *job), jobs))


def _npm_snapshot(
    dest: Path,
    published: str,
    package: str,
    version: str,
    dist: dict,
    members: tuple[str, ...],
) -> None:
    """The first of `members` the release ships, or nothing when it ships none."""
    data = _get(dist["tarball"])
    if "integrity" in dist:
        algo, b64 = dist["integrity"].split("-", 1)
        ok = hashlib.new(algo, data).digest() == b64decode(b64)
    else:
        ok = hashlib.sha1(data).hexdigest() == dist["shasum"]
    if not ok:
        raise ValueError(f"{package}@{version}: tarball fails its registry checksum")
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        names = set(tar.getnames())
        member = next((m for m in members if m in names), None)
        if member is None:
            return
        body = tar.extractfile(member)
        assert body is not None
        stamp = published[:19].replace(":", "")
        name = f"{stamp}-{package.rsplit('/', 1)[-1]}-{version}.json"
        (dest / name).write_bytes(body.read())


def _image(dest: Path, _: Path) -> None:
    total = 0
    with (
        _stream(OPEN_IMAGES_URL) as body,
        tarfile.open(fileobj=body, mode="r|gz") as tar,
    ):
        for m in tar:
            if not m.isfile():
                continue
            out = dest / m.name
            out.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(m) as src, out.open("wb") as f:  # type: ignore[union-attr]
                shutil.copyfileobj(src, f)
            total += m.size
            if total >= _FETCH_TARGET:
                break


def _video(dest: Path, downloads: Path) -> None:
    for i, rel in enumerate(VIDEOS):
        name = rel.rsplit("/", 1)[-1]
        if not name.endswith(".zip"):
            _curl(BLENDER_URL + rel, dest / f"{i:02d}-{name}")
            continue
        archive = downloads / name
        _curl(BLENDER_URL + rel, archive)
        with zipfile.ZipFile(archive) as z:
            (info,) = [m for m in z.infolist() if not m.is_dir()]
            with (
                z.open(info) as src,
                (dest / f"{i:02d}-{Path(info.filename).name}").open("wb") as f,
            ):
                shutil.copyfileobj(src, f)
        archive.unlink()


def _audio(dest: Path, downloads: Path) -> None:
    total = 0
    for i, sha in enumerate(JAMENDO_SHA256):
        if total >= _FETCH_TARGET:
            return
        archive = downloads / f"jamendo-{i:02d}.tar"
        _curl(JAMENDO_URL.format(i), archive)
        if _sha256(archive) != sha:
            raise ValueError(f"{archive.name} fails its published sha256")
        with tarfile.open(archive) as tar:
            tar.extractall(dest, filter="data")
            total += sum(m.size for m in tar.getmembers() if m.isfile())
        archive.unlink()
    if total < _FETCH_TARGET:
        raise ValueError("the pinned Jamendo chunks fall short of the largest tier")


def _media(dest: Path, _: Path) -> None:
    """Hard links to the MEDIA_SOURCES, merged by where each file ends in its corpus.

    Every prefix then holds each kind in step by bytes, and a large file comes
    after the smaller files of the other kinds that end before it.
    """
    streams = []
    for name in MEDIA_SOURCES:
        if not (data_dir() / "corpora" / name).is_dir():
            fetch(name)
        root, entries = data_dir() / "corpora" / name, manifest(name)
        ends = accumulate(e.size for e in entries)
        streams.append([(end, root / e.path, e.size) for end, e in zip(ends, entries)])
    total = 0
    for i, (_end, src, size) in enumerate(heapq.merge(*streams, key=itemgetter(0))):
        if total >= _FETCH_TARGET:
            return
        os.link(src, dest / f"{i:06d}-{src.name}")
        total += size


def _sha256(path: Path) -> str:
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


PUBLIC: dict[str, Public] = {
    "silesia": Public(_silesia, "f9f28ef91de67f4cebe87dba341b8987", whole=True),
    "json-docs": Public(_json_docs, "44e5af902dd1d25dbc05710299549987"),
    "json-snapshots": Public(_json_snapshots, "e0c577c2eb022cb71cac37034ac5f3a1"),
    "image": Public(_image, "ba35dcfa7dd0043072c1337c1085c075"),
    "video": Public(_video, "9c651929aae2771f6562ae4ae3554b85"),
    "audio": Public(_audio, "b9029f853b4c3e8793904241f53e92a7"),
    "media": Public(_media, "5f2143bf027e08818689450ae6e7a3c3"),
}
