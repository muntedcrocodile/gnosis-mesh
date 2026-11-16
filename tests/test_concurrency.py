"""Two processes (think daemon + CLI) writing the same store at once: no lost updates, no 'database is locked'."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from gnosis_mesh import Store

ROOT = Path(__file__).parent.parent
N = 400

# Each worker waits for a go-file so they really overlap, then interleaves the daemon's write pattern (peer upsert +
# message append) with the CLI's (block/nick) and re-reads to exercise readers alongside the other writer.
WORKER = """
import sys, time
from pathlib import Path
from gnosis_mesh import Store
home, tag, n = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
s = Store(home)
while not (home / "go").exists():
    time.sleep(0.001)
for i in range(n):
    s.remember_peer(f"shared-{i % 20}", [f"/ip4/10.0.0.{i % 250}/tcp/1"], tag)  # both hit the same rows
    s.remember_peer(f"{tag}-{i}", [], tag)
    s.log_message(f"{tag}-{i}", "in", f"{tag} {i}")
    if i % 10 == 0:
        s.block(f"{tag}-blocked-{i}")
        s.set_nick(f"{tag}-{i}", f"nick-{tag}-{i}")
    assert len(s.messages(peer=f"{tag}-{i}")) == 1
s.close()
"""


def run_workers(home: Path, tags: list[str]) -> None:
    Store(home).close()  # create the schema first so the workers race on data, not setup
    procs = [subprocess.Popen([sys.executable, "-c", WORKER, str(home), t, str(N)], cwd=ROOT,
                              stderr=subprocess.PIPE, text=True) for t in tags]
    (home / "go").touch()
    for p in procs:
        _, err = p.communicate(timeout=120)
        assert p.returncode == 0, err


def test_two_processes_no_lost_updates(home: Path) -> None:
    run_workers(home, ["daemon", "cli"])
    s = Store(home)
    peers = s.peers()
    for tag in ("daemon", "cli"):
        assert all(f"{tag}-{i}" in peers for i in range(N)), f"lost peer upserts from {tag}"
        assert sum(m["text"].startswith(f"{tag} ") for m in s.messages()) == N
        assert sum(b.startswith(f"{tag}-blocked-") for b in s.config()["blocked"]) == N // 10
        assert sum(v.startswith(f"nick-{tag}-") for v in s.config()["nicks"].values()) == N // 10
    assert len(peers) == 2 * N + 20
    assert len(s.messages()) == 2 * N
    ids = [m["id"] for m in s.messages()]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    s.close()


def test_follow_cursor_sees_every_message_once(home: Path) -> None:
    """`read -f` polls messages(after=last_id) while another process appends: nothing skipped or repeated."""
    s = Store(home)
    proc = subprocess.Popen([sys.executable, "-c", WORKER, str(home), "w", str(N)], cwd=ROOT)
    (home / "go").touch()
    seen: list[str] = []
    cursor = 0
    while True:
        done = proc.poll() is not None  # check before reading, so the final read sees everything
        for m in s.messages(after=cursor):
            cursor = m["id"]
            seen.append(m["text"])
        if done:
            break
    assert proc.returncode == 0
    assert seen == [f"w {i}" for i in range(N)]
    s.close()
