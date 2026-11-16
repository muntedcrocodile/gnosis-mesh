"""One-time import of the legacy config.json / peers.json / messages.jsonl files."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path

import click
import pytest

from gnosis_mesh import Store, socket_path

CONFIG = {"username": "me", "nicks": {"P1": "bobby"}, "blocked": ["P3"], "extra": [1, 2]}
PEERS = {
    "P1": {"addrs": ["/ip4/1.2.3.4/tcp/4777"], "seen": 1700000000.5, "name": "bob"},
    "P2": {"addrs": [], "seen": 1700000001.0},
    "P3": {"addrs": ["/ip4/9.9.9.9/tcp/1"], "seen": 1700000002.0, "name": "mallory"},
}
MESSAGES = [
    {"ts": 1700000000.1, "peer": "P1", "dir": "in", "text": "hello"},
    {"ts": 1700000000.2, "peer": "P1", "dir": "out", "text": "hi bob ✓"},
    {"ts": 1700000000.3, "peer": "P3", "dir": "in", "text": "spam"},
]


def write_legacy(home: Path, truncated_tail: bool = False) -> None:
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.json").write_text(json.dumps(CONFIG, indent=2))
    (home / "peers.json").write_text(json.dumps(PEERS, indent=2))
    lines = "".join(json.dumps(m) + "\n" for m in MESSAGES)
    (home / "messages.jsonl").write_text(lines + ('{"ts": 17000' if truncated_tail else ""))
    (home / "key").write_bytes(b"k" * 32)


def assert_migrated(s: Store, home: Path) -> None:
    assert s.config() == CONFIG
    assert s.peers() == PEERS
    assert [{k: m[k] for k in ("ts", "peer", "dir", "text")} for m in s.messages()] == MESSAGES
    for name in ("config.json", "peers.json", "messages.jsonl"):
        assert not (home / name).exists() and (home / f"{name}.migrated").exists()
    assert s.key_seed() == b"k" * 32  # the identity file is left alone


def test_migrates_all_files(home: Path) -> None:
    write_legacy(home)
    s = Store(home)
    assert_migrated(s, home)
    assert s.label("P1") == "bobby" and s.label("P2").startswith("P2")
    assert [m["text"] for m in s.messages(include_blocked=False)] == ["hello", "hi bob ✓"]
    s.close()


def test_migration_runs_once(home: Path) -> None:
    write_legacy(home)
    Store(home).close()
    write_legacy(home)  # e.g. an old daemon still running recreated the files: they are NOT re-imported
    s = Store(home)
    assert len(s.messages()) == len(MESSAGES)
    assert (home / "messages.jsonl").exists()
    s.close()


def test_partial_and_empty_legacy_files(home: Path) -> None:
    home.mkdir()
    (home / "peers.json").write_text("")
    (home / "messages.jsonl").write_text("\n")
    s = Store(home)
    assert s.peers() == {} and s.messages() == [] and s.config()["username"] == ""
    assert (home / "peers.json.migrated").exists() and not (home / "config.json.migrated").exists()
    s.close()


def test_truncated_last_line_is_skipped(home: Path) -> None:
    write_legacy(home, truncated_tail=True)
    s = Store(home)
    assert len(s.messages()) == len(MESSAGES)
    s.close()


def test_corrupt_json_aborts_without_losing_files(home: Path) -> None:
    write_legacy(home)
    (home / "peers.json").write_text("{not json")
    try:
        Store(home)
    except json.JSONDecodeError:
        pass
    else:
        raise AssertionError("expected the migration to fail loudly")
    # nothing half-imported, originals untouched: fixing the file and retrying works
    assert (home / "config.json").exists() and (home / "peers.json").exists()
    (home / "peers.json").write_text(json.dumps(PEERS))
    s = Store(home)
    assert_migrated(s, home)
    s.close()


def test_concurrent_first_runs_import_once(home: Path) -> None:
    """The CLI and the daemon starting together must not import the history twice."""
    write_legacy(home)
    code = "import sys; from pathlib import Path; from gnosis_mesh import Store; Store(Path(sys.argv[1])).close()"
    procs = [subprocess.Popen([sys.executable, "-c", code, str(home)], cwd=Path(__file__).parent.parent)
             for _ in range(4)]
    assert all(p.wait(timeout=60) == 0 for p in procs)
    s = Store(home)
    assert_migrated(s, home)
    s.close()


def test_refuses_while_a_legacy_daemon_is_running(home: Path) -> None:
    """An old daemon would keep writing the JSON files after the import, so refuse until it is stopped."""
    write_legacy(home)
    sock = socket_path(home)
    try:
        with socket.socket(socket.AF_UNIX) as listener:  # stands in for the old daemon's control socket
            listener.bind(sock)
            listener.listen()
            with pytest.raises(click.ClickException, match="stop it"):
                Store(home)
            assert (home / "messages.jsonl").exists() and not (home / "messages.jsonl.migrated").exists()
        assert os.path.exists(sock)  # the "daemon" died without cleaning up: a stale socket must not block us
        s = Store(home)
        assert_migrated(s, home)
        s.close()
    finally:
        os.unlink(sock)
